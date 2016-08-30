from .ops import ckan_method, patch_if_required, make_group, check_resource, create_resource, reupload_resource, get_size
import ckanapi
from .util import make_logger
from .bpa import get_bpa
from .util import prune_dict

logger = make_logger(__name__)


def sync_package(ckan, obj):
    try:
        ckan_obj = ckan_method(ckan, 'package', 'show')(id=obj['name'])
    except ckanapi.errors.NotFound:
        create_obj = {
            'type': obj['type'],
            'id': obj['id'],
            'name': obj['name'],
            'owner_org': obj['owner_org']
        }
        print(create_obj)
        ckan_obj = ckan_method(ckan, 'package', 'create')(**create_obj)
        logger.info('created package object: %s' % (obj['id']))
    patch_obj = obj.copy()
    patch_obj['id'] = ckan_obj['id']
    was_patched, ckan_obj = patch_if_required(ckan, 'package', ckan_obj, patch_obj)
    if was_patched:
        logger.info('patched package object: %s' % (obj['id']))
    return ckan_obj


def sync_packages(ckan, packages, org, group):
    # FIXME: we don't check if there are any packages we should remove (unpublish)
    logger.info('syncing %d packages' % (len(packages)))
    # we have to post the group back in package objects, send a minimal version of it
    api_group_obj = prune_dict(group, ('display_name', 'description', 'title', 'image_display_url', 'id', 'name'))
    ckan_packages = []
    for package in packages:
        obj = package.copy()
        obj['owner_org'] = org['id']
        obj['groups'] = [api_group_obj]
        ckan_packages.append(sync_package(ckan, obj))
    return ckan_packages


def sync_package_resources(ckan, package_obj, md5_legacy_url, resources, auth):
    to_reupload = []
    current_resources = package_obj['resources']
    existing_resources = dict((t['id'], t) for t in current_resources)
    needed_resources = dict((t['md5'], t) for t in resources)
    to_create = set(needed_resources) - set(existing_resources)
    to_delete = set(existing_resources) - set(needed_resources)

    for current_ckan_obj in current_resources:
        obj_id = current_ckan_obj['id']
        legacy_url = md5_legacy_url[obj_id]
        current_url = current_ckan_obj.get('url')
        if not current_url or not check_resource(ckan, current_url, legacy_url, auth):
            logger.error('resource check failed, queued for re-upload: %s' % (obj_id))
            to_reupload.append((current_ckan_obj, legacy_url))
        else:
            logger.info('resource check OK')

    for obj_id in to_create:
        resource_obj = needed_resources[obj_id]
        legacy_url = md5_legacy_url[obj_id]
        if create_resource(ckan, resource_obj, legacy_url, auth):
            logger.info('created resource: %s' % (obj_id))

    for obj_id in to_delete:
        ckan_method(ckan, 'resource', 'delete')(id=obj_id)
        logger.info('deleted resource: %s' % (obj_id))

    # patch all the resources, to ensure everything is synced on
    # existing resources
    package_obj = ckan_method(ckan, 'package', 'show')(id=package_obj['id'])
    current_resources = package_obj['resources']
    for current_ckan_obj in current_resources:
        obj_id = current_ckan_obj['id']
        resource_obj = needed_resources[obj_id]
        legacy_url = md5_legacy_url[obj_id]
        was_patched, ckan_obj = patch_if_required(ckan, 'resource', current_ckan_obj, resource_obj)
        if was_patched:
            logger.info('patched resource: %s' % (obj_id))

    return to_reupload


def reupload_resources(ckan, to_reupload, md5_legacy_url, auth):
    for reupload_obj, legacy_url in sorted(to_reupload, key=lambda x: get_size(x[1], None)):
        reupload_resource(ckan, reupload_obj, legacy_url)
        obj_id = reupload_obj['id']
        legacy_url = md5_legacy_url[obj_id]
        reupload_resource(ckan, reupload_obj, legacy_url, auth)


def sync_resources(ckan, resources, ckan_packages, auth):
    logger.info('syncing %d resources' % (len(resources)))

    bpa_id_package_id = {}
    for package_obj in ckan_packages:
        bpa_id_package_id[package_obj['bpa_id']] = package_obj['id']

    resource_idx = {}
    md5_legacy_url = {}
    for bpa_id, legacy_url, resource_obj in resources:
        if bpa_id not in resource_idx:
            resource_idx[bpa_id] = []
        obj = resource_obj.copy()
        obj['package_id'] = bpa_id_package_id[bpa_id]
        resource_idx[bpa_id].append(obj)
        md5_legacy_url[obj['md5']] = legacy_url

    to_reupload = []
    for package_obj in ckan_packages:
        to_reupload += sync_package_resources(ckan, package_obj, md5_legacy_url, resource_idx.get(package_obj['bpa_id']), auth)
    reupload_resources(ckan, to_reupload, md5_legacy_url, auth)


def sync_metadata(ckan, meta, auth):
    ckan_org = get_bpa(ckan)
    ckan_group = make_group(ckan, meta.get_group())
    ckan_packages = sync_packages(ckan, meta.get_packages(), ckan_org, ckan_group)
    sync_resources(ckan, meta.get_resources(), ckan_packages, auth)
