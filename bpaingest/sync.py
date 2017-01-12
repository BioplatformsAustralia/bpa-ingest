from __future__ import print_function

from .ops import ckan_method, patch_if_required, check_resource, create_resource, reupload_resource, get_organization, ArchiveInfo
import ckanapi
from Queue import Queue
from threading import Thread
from .util import make_logger
from .util import prune_dict
from genhash import S3_HASH_FIELD

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
        ckan_obj = ckan_method(ckan, 'package', 'create')(**create_obj)
        logger.info('created package object: %s' % (obj['id']))
    patch_obj = obj.copy()
    patch_obj['id'] = ckan_obj['id']
    # tags are handed back with a bunch of info that's irrelevant
    compare_ckan_obj = ckan_obj.copy()
    compare_ckan_obj['tags'] = [{'name': t['name']} for t in ckan_obj['tags']]
    was_patched, ckan_obj = patch_if_required(ckan, 'package', compare_ckan_obj, patch_obj)
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
        if api_group_obj is not None:
            obj['groups'] = [api_group_obj]
        ckan_packages.append(sync_package(ckan, obj))
    return ckan_packages


def sync_package_resources(ckan, archive_info, package_obj, md5_legacy_url, resources, auth):
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
        resource_issue = check_resource(ckan, archive_info, current_url, legacy_url, current_ckan_obj.get(S3_HASH_FIELD), auth)
        if resource_issue:
            logger.error('resource check failed (%s) queued for re-upload: %s' % (resource_issue, obj_id))
            to_reupload.append((current_ckan_obj, legacy_url))
        else:
            logger.info('resource check OK: %s' % (obj_id))

    for obj_id in to_create:
        resource_obj = needed_resources[obj_id]
        legacy_url = md5_legacy_url[obj_id]
        # we don't upload at the time we create the resource: it's more useful to immediately
        # get all the metadata into the CKAN instance, with links to legacy mirrors. we can
        # them come back and upload into CKAN using the reupload functionality of this script
        create_obj = resource_obj.copy()
        create_obj['url'] = legacy_url
        current_ckan_obj = create_resource(ckan, create_obj)
        if current_ckan_obj:
            logger.info('created resource: %s' % (obj_id))
            to_reupload.append((current_ckan_obj, legacy_url))

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


def reupload_resources(ckan, archive_info, to_reupload, md5_legacy_url, auth, num_threads):
    # this is not a lot of code, but it's about 99% of the time we spend in
    # this script. hence, the uploads run in parallel.
    def upload_worker():
        while True:
            reupload_obj, legacy_url = q.get()
            reupload_resource(ckan, reupload_obj, legacy_url, auth)
            q.task_done()

    q = Queue()
    for i in range(num_threads):
        t = Thread(target=upload_worker)
        t.daemon = True
        t.start()

    logger.info("%d objects to be re-uploaded" % (len(to_reupload)))
    for item in to_reupload:
        q.put(item)
    q.join()


def sync_resources(ckan, resources, resource_linkage_attr, ckan_packages, auth, num_threads, do_uploads):
    logger.info('syncing %d resources' % (len(resources)))

    archive_info = ArchiveInfo(ckan)

    resource_linkage_package_id = {}
    from pprint import pprint
    for package_obj in ckan_packages:
        pprint(package_obj)
        resource_linkage_package_id[package_obj[resource_linkage_attr]] = package_obj['id']

    # wire the resources to their CKAN package
    resource_idx = {}
    md5_legacy_url = {}
    for resource_linkage, legacy_url, resource_obj in resources:
        package_id = resource_linkage_package_id[resource_linkage]
        obj = resource_obj.copy()
        obj['package_id'] = package_id
        if package_id not in resource_idx:
            resource_idx[package_id] = []
        resource_idx[package_id].append(obj)
        md5_legacy_url[obj['md5']] = legacy_url

    to_reupload = []
    for package_obj in ckan_packages:
        package_id = package_obj['id']
        package_resources = resource_idx.get(package_id)
        if package_resources is None:
            logger.warning("No resources for package `%s`" % (package_id))
            continue
        to_reupload += sync_package_resources(ckan, archive_info, package_obj, md5_legacy_url, package_resources, auth)
    if do_uploads:
        reupload_resources(ckan, archive_info, to_reupload, md5_legacy_url, auth, num_threads)


def sync_metadata(ckan, meta, auth, num_threads, do_uploads):
    organization = get_organization(ckan, meta.organization)
    packages = meta.get_packages()
    # check that the IDs in packages are unique
    assert(len(list(set([t['id'] for t in packages]))) == len(packages))
    ckan_packages = sync_packages(ckan, packages, organization, None)
    sync_resources(ckan, meta.get_resources(), meta.resource_linkage, ckan_packages, auth, num_threads, do_uploads)
