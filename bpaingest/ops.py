import tempfile
import requests
import ckanapi
import sys
from contextlib import closing
from .util import make_logger


logger = make_logger(__name__)


def ckan_method(ckan, object_type, method):
    """
    returns a CKAN method from the upstream API, with an
    intermediate function which retries on 500 errors
    """
    return getattr(ckan.action, object_type + '_' + method)


def patch_if_required(ckan, object_type, ckan_object, patch_object):
    "patch ckan_object if applying patch_object would change it"
    differences = []
    for (k, v) in patch_object.items():
        v2 = ckan_object.get(k)
        # co-erce to string to cope with numeric types in the JSON data
        if v != v2 and str(v) != str(v2):
            differences.append((k, v, v2))
    for k, v, v2 in differences:
        logger.debug("%s/%s: difference on k `%s', v `%s' v2 `%s'" % (
            object_type,
            ckan_object.get('id', '<no id?>'),
            k,
            v,
            v2))
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


def create_resource(ckan, ckan_obj, do_upload):
    url = ckan_obj['url']  # URL in the legacy archive
    if do_upload:
        response = requests.get(url, stream=True)
        with closing(response):
            if not response.ok:
                raise Exception("unable to download `%s': status %d" % (url, response.status))
            with tempfile.NamedTemporaryFile() as tempf:
                print("downloading `%s' to temporary file" % (url))
                for block in response.iter_content(1048576):
                    tempf.write(block)
                    sys.stderr.write('.')
                    sys.stderr.flush()
                tempf.flush()
                print("uploading from tempfile: %s" % (tempf.name))
                with open(tempf.name, "rb") as read_back:
                    ckan.action.resource_create(upload=read_back, **ckan_obj)
    else:
        ckan.action.resource_create(**ckan_obj)
