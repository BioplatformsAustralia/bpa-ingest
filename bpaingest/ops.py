import progressbar
import tempfile
import requests
import ckanapi
import os
from urlparse import urlparse
from contextlib import closing
from .util import make_logger

logger = make_logger(__name__)


# https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def ckan_method(ckan, object_type, method):
    """
    returns a CKAN method from the upstream API, with an
    intermediate function which retries on 500 errors
    """
    return getattr(ckan.action, object_type + '_' + method)


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
        ckan_object = ckan_method(ckan, object_type, "patch")(**patch_object)
    return patch_needed, ckan_object


def make_group(ckan, group_obj):
    try:
        ckan_obj = ckan_method(ckan, 'group', 'show')(id=group_obj['name'])
        logger.info("created group `%s'" % (group_obj['name']))
    except ckanapi.errors.NotFound:
        ckan_obj = ckan_method(ckan, 'group', 'create')(name=group_obj['name'])
    # copy over auto-allocated ID
    group_obj['id'] = ckan_obj['id']
    was_patched, ckan_obj = patch_if_required(ckan, 'group', ckan_obj, group_obj)
    if was_patched:
        logger.info("created group `%s'" % (group_obj['name']))
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
        elif response.status_code == 200:
            return new_url
        else:
            return None

_size_cache = {}


def get_size(url, auth):
    def _size(response):
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

    # if we have a pre-calculated s3etag in metadata, check it matches
    current_etag = get_etag(current_url, None)
    if metadata_etag is not None and current_etag.strip('"') != metadata_etag:
        logger.error("CKAN resource %s has incorrect etag: %s (should be %s)" % (current_url, current_etag, metadata_etag))
        return False

    return True


def download_legacy_file(legacy_url, auth):

    def download_to_fileobj(url, fd):
        logger.debug("downloading `%s'" % (url))
        response = requests.get(url, stream=True, auth=auth)
        total_size = int(response.headers['content-length'])
        logger.info('Downloading %s' % (sizeof_fmt(total_size)))
        bar = progressbar.ProgressBar(max_value=total_size)
        retrieved = 0
        with closing(response):
            if not response.ok:
                logger.error("unable to download `%s': status %d" % (url, response.status_code))
                return None
            for block in response.iter_content(65532):
                retrieved += len(block)
                bar.update(retrieved)
                fd.write(block)
        return retrieved

    basename = legacy_url.rsplit('/', 1)[-1]
    tempdir = tempfile.mkdtemp()
    path = os.path.join(tempdir, basename)
    resolved_url = resolve_url(legacy_url, auth)
    logger.info("Resolved `%s' to `%s'" % (legacy_url, resolved_url))
    if not resolved_url:
        logger.error("unable to resolve `%s' - file missing?" % (legacy_url))
        os.rmdir(tempdir)
        return None, None
    with open(path, 'w') as fd:
        size = download_to_fileobj(resolved_url, fd)
        if size is None:
            os.rmdir(tempdir)
            return None, None
    return tempdir, path


def reupload_resource(ckan, ckan_obj, legacy_url, auth=None):
    "reupload data from legacy_url to ckan_obj"

    tempdir, path = download_legacy_file(legacy_url, auth)
    if path is None:
        return
    try:
        logger.debug("re-uploading from tempfile: %s" % (path))
        upload_obj = ckan_obj.copy()
        upload_obj['url'] = 'dummy-value'  # required by CKAN < 2.5
        with open(path, "rb") as fd:
            updated_obj = ckan.action.resource_update(upload=fd, id=upload_obj['id'])
            logger.debug("upload successful: %s" % (updated_obj['url']))
        return True
    finally:
        os.unlink(path)
        os.rmdir(tempdir)


def create_resource(ckan, ckan_obj, legacy_url, auth=None):
    "create resource, uploading data from legacy_url"

    tempdir, path = download_legacy_file(legacy_url, auth)
    if path is None:
        return
    try:
        logger.debug("uploading from tempfile: %s" % (path))
        upload_obj = ckan_obj.copy()
        upload_obj['url'] = 'dummy-value'  # required by CKAN < 2.5
        with open(path, "rb") as fd:
            updated_obj = ckan.action.resource_create(upload=fd, **upload_obj)
            logger.debug("upload successful: %s" % (updated_obj['url']))
        return True
    finally:
        os.unlink(path)
        os.rmdir(tempdir)
