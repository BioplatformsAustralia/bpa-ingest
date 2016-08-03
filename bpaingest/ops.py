import progressbar
import tempfile
import requests
import ckanapi
import os
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
    "patch ckan_object if applying patch_object would change it"
    differences = []
    for (k, v) in patch_object.items():
        if skip_differences and k in skip_differences:
            continue
        v2 = ckan_object.get(k)
        # co-erce to string to cope with numeric types in the JSON data
        if v != v2 and str(v) != str(v2):
            differences.append((k, v, v2))
    for k, v, v2 in differences:
        logger.debug("%s/%s: difference on k `%s', v `%s' v2 `%s'" % (object_type, ckan_object.get('id', '<no id?>'), k,
                                                                      v, v2))
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


def create_resource(ckan, ckan_obj, legacy_url):
    "create resource, uploading data from legacy_url"

    def resolve_url(url):
        new_url = url
        for i in range(4):
            response = requests.head(new_url)
            if response.status_code == 301 or response.status_code == 302:
                new_url = response.headers.get('location')
            elif response.status_code == 200:
                return new_url
            else:
                return None

    def download_to_fileobj(url, fd):
        logger.debug("downloading `%s'" % (url))
        response = requests.get(url, stream=True)
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
    resolved_url = resolve_url(legacy_url)
    if not resolved_url:
        logger.error("unable to resolve `%s' - file missing?" % (legacy_url))
        return
    logger.info("Resolved `%s' to `%s'" % (legacy_url, resolved_url))
    try:
        with open(path, 'w') as fd:
            size = download_to_fileobj(resolved_url, fd)
            if not size:
                return
            logger.debug("uploading from tempfile: %s" % (path))
            upload_obj = ckan_obj.copy()
            upload_obj['url'] = 'dummy-value'  # required by CKAN < 2.5
        with open(path, "rb") as fd:
            ckan.action.resource_create(upload=fd, **upload_obj)
        return True
    finally:
        os.unlink(path)
        os.rmdir(tempdir)
