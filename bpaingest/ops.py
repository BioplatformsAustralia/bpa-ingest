import subprocess
import tempfile
import requests
import ckanapi
import os
from urlparse import urlparse
from collections import defaultdict
from .util import make_logger, authenticated_ckan_session

logger = make_logger(__name__)
UPLOAD_RETRY = 3


# https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


method_stats = defaultdict(int)


def ckan_method(ckan, object_type, method):
    """
    returns a CKAN method from the upstream API, with an
    intermediate function which does some global accounting
    """
    fn = getattr(ckan.action, object_type + '_' + method)

    def _proxy_fn(*args, **kwargs):
        method_stats[(object_type, method)] += 1
        return fn(*args, **kwargs)
    return _proxy_fn


def print_accounts():
    print("API call accounting:")
    for object_type, method in sorted(method_stats, key=lambda x: method_stats[x]):
        print("  %14s  %6s  %d" % (object_type, method, method_stats[(object_type, method)]))


def diff_objects(obj1, obj2, desc, skip_differences=None):
    def sort_if_list(v):
        if type(v) is list:
            return list(sorted(v))
        return v
    differences = []
    for k in obj1.keys():
        if skip_differences and k in skip_differences:
            continue
        v1 = sort_if_list(obj1.get(k))
        v2 = sort_if_list(obj2.get(k))
        # co-erce to string to cope with numeric types in the JSON data
        if v1 != v2 and unicode(v1) != unicode(v2):
            differences.append((k, v1, v2))
    if differences:
        logger.debug("%s/%s: differs" % (desc, obj2.get('id', '<no id?>')))
    differences.sort()
    for k, v1, v2 in differences:
        logger.debug("   -  %s=%s (%s)" % (k, v2, type(v2).__name__))
    for k, v1, v2 in differences:
        logger.debug("   +  %s=%s (%s)" % (k, v1, type(v1).__name__))
    return len(differences) > 0


def patch_if_required(ckan, object_type, ckan_object, patch_object, skip_differences=None):
    """
    patch ckan_object if applying patch_object would change it. ckan_object is unchanged
    for any keys which are not mentioned in patch_object
    """
    patch_needed = diff_objects(patch_object, ckan_object, object_type)
    if patch_needed:
        ckan_object = ckan_method(ckan, object_type, "patch")(**patch_object)
    return patch_needed, ckan_object


def make_obj(ckan, obj_type, obj):
    try:
        ckan_obj = ckan_method(ckan, obj_type, 'show')(id=obj['name'])
    except ckanapi.errors.NotFound:
        ckan_obj = ckan_method(ckan, obj_type, 'create')(name=obj['name'])
        logger.info("created %s `%s'" % (obj_type, obj['name']))
    # copy over auto-allocated ID
    obj['id'] = ckan_obj['id']
    was_patched, ckan_obj = patch_if_required(ckan, obj_type, ckan_obj, obj)
    if was_patched:
        logger.info("updated %s `%s'" % (obj_type, obj['name']))
    return ckan_obj


def make_group(ckan, group_obj):
    return make_obj(ckan, 'group', group_obj)


def make_organization(ckan, organization_obj):
    return make_obj(ckan, 'organization', organization_obj)


class ArchiveInfo(object):
    def __init__(self, ckan):
        self.ckan_address = self.ckan_address = None
        if ckan is not None:
            self.ckan_session = authenticated_ckan_session(ckan)
            self.ckan_address = ckan.address
        self.other_session = requests.Session()
        self._size_cache = {}

    def pick_session(self, url):
        "pick session based on URL, if on CKAN use our authenticated session"
        if self.ckan_address is not None and url.startswith(self.ckan_address):
            return self.ckan_session
        return self.other_session

    def resolve_url(self, url, auth):
        session = self.pick_session(url)
        new_url = url
        for i in range(4):
            response = session.head(new_url, auth=auth)
            if response.status_code == 301 or response.status_code == 302:
                new_url = response.headers.get('location')
            elif response.status_code in (403, 401):
                # if we're getting 403s and we re-upload a bunch of data because of it, that is unhelpful
                logger.error("authentication error accessing archive: aborting to avoid destructive side-effects")
                logger.error((url, auth))
                raise Exception()
            elif response.status_code == 200:
                return new_url
            else:
                return None

    def get_size(self, url, auth):
        def _size(response):
            if response.status_code in (403, 401):
                # if we're getting 403s and we re-upload a bunch of data because of it, that is unhelpful
                logger.error("authentication error accessing archive: aborting to avoid destructive side-effects.")
                logger.error((url, auth))
                raise Exception()
            if response.status_code != 200:
                return None
            if 'content-length' in response.headers:
                return int(response.headers['content-length'])
            if 'content-range' in response.headers:
                return int(response.headers.get('content-range').rsplit('/', 1)[-1])

        if not url:
            return None
        session = self.pick_session(url)
        if url not in self._size_cache:
            resolved = self.resolve_url(url, auth)
            if resolved is None:
                return None
            self._size_cache[url] = self._size_cache[resolved] = _size(session.head(resolved, auth=auth))
        return self._size_cache[url]

    def get_etag(self, url, auth):
        session = self.pick_session(url)

        def _etag(response):
            if response.status_code != 200:
                return None
            return response.headers.get('etag')
        if not url:
            return None
        resolved = self.resolve_url(url, auth)
        if resolved is None:
            return None
        return _etag(session.head(resolved, auth=auth))


def same_netloc(u1, u2):
    n1 = urlparse(u1).netloc
    n2 = urlparse(u2).netloc
    return n1 == n2


def check_resource(ckan_address, archive_info, current_url, legacy_url, metadata_etag, auth=None):
    """
    returns None if the ckan_obj looks good (is on the CKAN server, size matches legacy url size)
    otherwise returns a short string describing the problem
    """

    if current_url is None:
        logger.error('resource missing (no current URL)')
        return 'missing'

    if not same_netloc(current_url, ckan_address):
        logger.error('resource is not hosted on CKAN server: %s' % (current_url))
        return 'not-on-ckan'

    # determine the size of the original file in the legacy archive
    legacy_size = archive_info.get_size(legacy_url, auth)
    if legacy_size is None:
        logger.error("error getting size of: %s" % (legacy_url))
        return 'error-getting-size'

    # determine the URL of the proxied s3 resource, and then its size
    current_size = archive_info.get_size(current_url, None)
    if current_size is None:
        logger.error("error getting size of: %s" % (current_url))
        return 'error-getting-size'

    if current_size != legacy_size:
        logger.error("CKAN resource %s has incorrect size: %d (should be %d)" % (current_url, current_size, legacy_size))
        return 'wrong-size'

    if metadata_etag is None:
        logger.warning("CKAN resource %s has no metadata etag: run genhash for this project." % (legacy_url))

    # if we have a pre-calculated s3etag in metadata, check it matches
    current_etag = archive_info.get_etag(current_url, None)
    if metadata_etag is not None and current_etag.strip('"') != metadata_etag:
        logger.error("CKAN resource %s has incorrect etag: %s (should be %s)" % (current_url, current_etag, metadata_etag))
        return 'wrong-etag'

    return None


def download_legacy_file(legacy_url, auth):
    basename = legacy_url.rsplit('/', 1)[-1]
    tempdir = tempfile.mkdtemp(prefix='bpaingest-data-')
    path = os.path.join(tempdir, basename)
    archive_info = ArchiveInfo(None)
    resolved_url = archive_info.resolve_url(legacy_url, auth)
    logger.info("Resolved `%s' to `%s'" % (legacy_url, resolved_url))
    if not resolved_url:
        logger.error("unable to resolve `%s' - file missing?" % (legacy_url))
        os.rmdir(tempdir)
        return None, None
    # wget will resume downloads, which is a huge win when dealing with
    # mirrors that sometimes close connections. ugly, but pragmatic.
    wget_args = ['wget', '-q', '-c', '-t', '0', '-O', path]
    if auth:
        wget_args += ['--user', auth[0]]
        wget_args += ['--password', auth[1]]
    wget_args.append(resolved_url)
    status = subprocess.call(wget_args)
    if status != 0:
        try:
            os.unlink(path)
        except OSError:
            pass
        try:
            os.rmdir(tempdir)
        except OSError:
            pass
        return None, None
    return tempdir, path


def reupload_resource(ckan, ckan_obj, legacy_url, auth=None):
    "reupload data from legacy_url to ckan_obj"

    tempdir, path = download_legacy_file(legacy_url, auth)
    if path is None:
        logger.debug("download from legacy archive failed")
        return
    try:
        logger.debug("re-uploading from tempfile: %s" % (path))
        upload_obj = ckan_obj.copy()
        upload_obj['url'] = 'dummy-value'  # required by CKAN < 2.5
        for i in range(UPLOAD_RETRY):
            try:
                with open(path, "rb") as fd:
                    updated_obj = ckan_method(ckan, 'resource', 'update')(upload=fd, id=upload_obj['id'])
                logger.debug("upload successful: %s" % (updated_obj['url']))
                break
            except Exception, e:
                logger.error("attempt %d/%d - upload failed: %s" % (i + 1, UPLOAD_RETRY, str(e)))
        return True
    finally:
        os.unlink(path)
        os.rmdir(tempdir)


def create_resource(ckan, ckan_obj):
    "create resource, uploading data from legacy_url"
    return ckan_method(ckan, 'resource', 'create')(**ckan_obj)


def get_organization(ckan, id):
    return ckan_method(ckan, 'organization', 'show')(id=id)
