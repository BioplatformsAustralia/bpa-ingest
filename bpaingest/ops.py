import ckanapi
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
        if v != v2:
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
