import subprocess
import tempfile
import requests
import ckanapi
import os
from urlparse import urlparse
from collections import defaultdict
from .util import make_logger

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


def patch_if_required(ckan, object_type, ckan_object, patch_object, skip_differences=None):
    """
    patch ckan_object if applying patch_object would change it. ckan_object is unchanged
    for any keys which are not mentioned in patch_object
    """
    differences = []
    for k in patch_object.keys():
        if skip_differences and k in skip_differences:
            continue
        v1 = patch_object.get(k)
        v2 = ckan_object.get(k)
        # co-erce to string to cope with numeric types in the JSON data
        if v1 != v2 and str(v1) != str(v2):
            differences.append((k, v1, v2))
    for k, v, v2 in differences:
        logger.debug("%s/%s: difference on k `%s', we have `%s' vs ckan `%s'" % (object_type, ckan_object.get('id', '<no id?>'), k, v, v2))
    patch_needed = len(differences) > 0
    if patch_needed:
        logger.debug("%s" % (patch_object))
        ckan_object = ckan_method(ckan, object_type, "patch")(**patch_object)
    return patch_needed, ckan_object


def make_group(ckan, group_obj):
    try:
        ckan_obj = ckan_method(ckan, 'group', 'show')(id=group_obj['name'])
    except ckanapi.errors.NotFound:
        ckan_obj = ckan_method(ckan, 'group', 'create')(name=group_obj['name'])
        logger.info("created group `%s'" % (group_obj['name']))
    # copy over auto-allocated ID
    group_obj['id'] = ckan_obj['id']
    was_patched, ckan_obj = patch_if_required(ckan, 'group', ckan_obj, group_obj)
    if was_patched:
        logger.info("updated group `%s'" % (group_obj['name']))
    return ckan_obj


def make_organization(ckan, org_obj):
    try:
        ckan_obj = ckan_method(ckan, 'organization', 'show')(id=org_obj['name'])
        logger.info("created organization `%s'" % (org_obj['name']))
    except ckanapi.errors.NotFound:
        ckan_obj = ckan_method(ckan, 'organization', 'create')(name=org_obj['name'])
    # copy over auto-allocated ID
    org_obj['id'] = ckan_obj['id']
    was_patched, ckan_obj = patch_if_required(ckan, 'organization', ckan_obj, org_obj)
    if was_patched:
        logger.info("patched organization `%s'" % (org_obj['name']))
    return ckan_obj


def resolve_url(url, auth):
    new_url = url
    for i in range(4):
        response = requests.head(new_url, auth=auth)
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

_size_cache = {}


def get_size(url, auth):
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
    if url not in _size_cache:
        resolved = resolve_url(url, auth)
        if resolved is None:
            return None
        _size_cache[url] = _size_cache[resolved] = _size(requests.head(resolved, auth=auth))
    return _size_cache[url]


def get_etag(url, auth):
    def _etag(response):
        if response.status_code != 200:
            return None
        return response.headers.get('etag')
    if not url:
        return None
    resolved = resolve_url(url, auth)
    if resolved is None:
        return None
    return _etag(requests.head(resolved, auth=auth))


def same_netloc(u1, u2):
    n1 = urlparse(u1).netloc
    n2 = urlparse(u2).netloc
    return n1 == n2


def check_resource(ckan, current_url, legacy_url, metadata_etag, auth=None):
    """returns True if the ckan_obj looks good (is on the CKAN server, size matches legacy url size)"""

    if current_url is None:
        logger.error('resource missing (no current URL)')
        return False

    if not same_netloc(current_url, ckan.address):
        logger.error('resource is not hosted on CKAN server: %s' % (current_url))
        return False

    # determine the size of the original file in the legacy archive
    legacy_size = get_size(legacy_url, auth)
    if legacy_size is None:
        logger.error("error getting size of: %s" % (legacy_url))
        return False

    # determine the URL of the proxied s3 resource, and then its size
    current_size = get_size(current_url, None)
    if current_size is None:
        logger.error("error getting size of: %s" % (legacy_url))
        return False

    if current_size != legacy_size:
        logger.error("CKAN resource %s has incorrect size: %d (should be %d)" % (current_url, current_size, legacy_size))
        return False

    if metadata_etag is None:
        logger.warning("CKAN resource %s has no metadata etag: run genhash for this project." % (obj_id))

    # if we have a pre-calculated s3etag in metadata, check it matches
    current_etag = get_etag(current_url, None)
    if metadata_etag is not None and current_etag.strip('"') != metadata_etag:
        logger.error("CKAN resource %s has incorrect etag: %s (should be %s)" % (current_url, current_etag, metadata_etag))
        return False

    return True


def download_legacy_file(legacy_url, auth):
    basename = legacy_url.rsplit('/', 1)[-1]
    tempdir = tempfile.mkdtemp()
    path = os.path.join(tempdir, basename)
    resolved_url = resolve_url(legacy_url, auth)
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
