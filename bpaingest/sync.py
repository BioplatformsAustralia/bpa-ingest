from __future__ import print_function

from .ops import ckan_method, patch_if_required, check_resource, create_resource, reupload_resource, get_organization, ArchiveInfo
import ckanapi
from Queue import Queue
from threading import Thread
from .util import make_logger
from .util import prune_dict
from genhash import S3_HASH_FIELD
from collections import Counter
from urlparse import urlparse

logger = make_logger(__name__)


def sync_package(ckan, obj):
    try:
        ckan_obj = ckan_method(ckan, 'package', 'show')(id=obj['name'])
    except ckanapi.errors.NotFound:
        create_obj = {
            'type': obj['type'],
            'id': obj['id'],
            'name': obj['name'],
            'owner_org': obj['owner_org'],
            'private': obj['private'],
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
    for package in sorted(packages, key=lambda p: p['name']):
        obj = package.copy()
        obj['owner_org'] = org['id']
        if api_group_obj is not None:
            obj['groups'] = [api_group_obj]
        ckan_packages.append(sync_package(ckan, obj))
    return ckan_packages


def check_resources(ckan, current_resources, resource_id_legacy_url, auth, num_threads):
    # another time-consuming activity: runs in parallel

    ckan_address = ckan.address
    archive_info = ArchiveInfo(ckan)
    # appending to a list is thread-safe in Python
    to_reupload = []

    def check_worker():
        while True:
            task = q.get()
            if task is None:
                break
            current_ckan_obj, legacy_url, current_url = task
            obj_id = current_ckan_obj['id']
            resource_issue = check_resource(ckan_address, archive_info, current_url, legacy_url, current_ckan_obj.get(S3_HASH_FIELD), auth)
            if resource_issue:
                logger.error('resource check failed (%s) queued for re-upload: %s' % (resource_issue, obj_id))
                to_reupload.append((current_ckan_obj, legacy_url))
            else:
                logger.info('resource check OK: %s' % (obj_id))
            q.task_done()

    q = Queue()
    threads = []
    for i in range(num_threads):
        t = Thread(target=check_worker)
        threads.append(t)
        t.start()

    logger.info("%d resources to be checked" % (len(current_resources)))
    for current_ckan_obj in current_resources:
        obj_id = current_ckan_obj['id']
        legacy_url = resource_id_legacy_url.get(obj_id)
        current_url = current_ckan_obj.get('url')
        q.put((current_ckan_obj, legacy_url, current_url))
    q.join()

    for thread in threads:
        q.put(None)

    for thread in threads:
        thread.join()

    return to_reupload


def check_package_resources(ckan, ckan_packages, resource_id_legacy_url, auth):
    all_resources = []
    for package_obj in sorted(ckan_packages, key=lambda p: p['name']):
        current_resources = package_obj['resources']
        all_resources += current_resources

    return check_resources(ckan, all_resources, resource_id_legacy_url, auth, 8)


def sync_package_resources(ckan, package_obj, resource_id_legacy_url, resources, auth):
    current_resources = package_obj['resources']
    existing_resources = dict((t['id'], t) for t in current_resources)
    needed_resources = dict((t['id'], t) for t in resources)
    to_create = set(needed_resources) - set(existing_resources)
    to_delete = set(existing_resources) - set(needed_resources)

    to_reupload = []

    for obj_id in to_create:
        resource_obj = needed_resources[obj_id]
        legacy_url = resource_id_legacy_url[obj_id]
        # we don't upload at the time we create the resource: it's more useful to immediately
        # get all the metadata into the CKAN instance, with links to legacy mirrors. we can
        # them come back and upload into CKAN using the reupload functionality of this script
        create_obj = resource_obj.copy()
        create_obj['url'] = legacy_url
        current_ckan_obj = create_resource(ckan, create_obj)
        if current_ckan_obj:
            logger.info('created resource: %s/%s' % (create_obj['package_id'], obj_id))
            to_reupload.append((current_ckan_obj, legacy_url))

    for obj_id in to_delete:
        delete_obj = existing_resources[obj_id]
        ckan_method(ckan, 'resource', 'delete')(id=obj_id)
        logger.info('deleted resource: %s/%s' % (delete_obj['package_id'], obj_id))

    # patch all the resources, to ensure everything is synced on
    # existing resources
    package_obj = ckan_method(ckan, 'package', 'show')(id=package_obj['id'])
    current_resources = package_obj['resources']
    for current_ckan_obj in current_resources:
        obj_id = current_ckan_obj['id']
        resource_obj = needed_resources[obj_id]
        legacy_url = resource_id_legacy_url[obj_id]
        was_patched, ckan_obj = patch_if_required(ckan, 'resource', current_ckan_obj, resource_obj)
        if was_patched:
            logger.info('patched resource: %s' % (obj_id))

    return to_reupload


def reupload_resources(ckan, to_reupload, resource_id_legacy_url, auth, num_threads):
    # this is not a lot of code, but it's a lot of the time we spend in
    # this script. hence, the uploads run in parallel.
    def upload_worker():
        while True:
            task = q.get()
            if task is None:
                break
            reupload_obj, legacy_url = task
            reupload_resource(ckan, reupload_obj, legacy_url, auth)
            q.task_done()

    q = Queue()
    threads = []
    for i in range(num_threads):
        t = Thread(target=upload_worker)
        threads.append(t)
        t.start()

    logger.info("%d objects to be re-uploaded" % (len(to_reupload)))
    for item in to_reupload:
        q.put(item)
    q.join()

    for thread in threads:
        q.put(None)
    for thread in threads:
        thread.join()


def sync_resources(ckan, resources, resource_linkage_attrs, ckan_packages, auth, num_threads, do_uploads):
    logger.info('syncing %d resources' % (len(resources)))

    resource_linkage_package_id = {}
    for package_obj in ckan_packages:
        linkage_tpl = tuple(package_obj[t] for t in resource_linkage_attrs)
        resource_linkage_package_id[linkage_tpl] = package_obj['id']

    # wire the resources to their CKAN package
    resource_idx = {}
    resource_id_legacy_url = {}
    for resource_linkage, legacy_url, resource_obj in resources:
        package_id = resource_linkage_package_id.get(resource_linkage)
        if package_id is None:
            logger.error("Unable to find package for `%s', skipping resource (%s)" % (repr(resource_linkage), legacy_url))
        obj = resource_obj.copy()
        obj['package_id'] = package_id
        if package_id not in resource_idx:
            resource_idx[package_id] = []
        resource_idx[package_id].append(obj)
        resource_id_legacy_url[obj['id']] = legacy_url

    # check all existing resources on all existing packages, in parallel
    to_reupload = check_package_resources(ckan, ckan_packages, resource_id_legacy_url, auth)

    for package_obj in sorted(ckan_packages, key=lambda p: p['name']):
        package_id = package_obj['id']
        package_resources = resource_idx.get(package_id)
        if package_resources is None:
            logger.warning("No resources for package `%s`" % (package_id))
            continue
        to_reupload += sync_package_resources(ckan, package_obj, resource_id_legacy_url, package_resources, auth)
    if do_uploads:
        reupload_resources(ckan, to_reupload, resource_id_legacy_url, auth, num_threads)


def resources_add_format(resources):
    """
    centrally assign formats to resources, based on file extension: no point
    duplicating this function in all the get_resources() implementations.
    if a get_resources() implementation needs to override this, it can just set
    the format key in the resource, and this function will leave the resource
    alone
    """
    extension_map = {
        'JPG': 'JPEG',
        'TGZ': 'TAR',
    }
    for resource_linkage, legacy_url, resource_obj in resources:
        if 'format' in resource_obj:
            continue
        filename = urlparse(legacy_url).path.split('/')[-1]
        if '.' not in filename:
            continue
        extension = filename.rsplit('.', 1)[-1].upper()
        extension = extension_map.get(extension, extension)
        if filename.lower().endswith('.fastq.gz'):
            resource_obj['format'] = 'FASTQ'
        elif filename.lower().endswith('.fasta.gz'):
            resource_obj['format'] = 'FASTA'
        elif extension in ('PNG', 'XLSX', 'XLS', 'PPTX', 'ZIP', 'TAR', 'GZ', 'DOC', 'DOCX', 'PDF', 'CSV', 'JPEG', 'XML', 'BZ2', 'EXE', 'EXF', 'FASTA', 'FASTQ', 'SCAN', 'WIFF'):
            resource_obj['format'] = extension


def sync_metadata(ckan, meta, auth, num_threads, do_uploads):
    def unique_packages():
        by_id = dict((t['id'], t) for t in packages)
        id_count = Counter(t['id'] for t in packages)
        for k, cnt in id_count.items():
            if cnt > 1:
                logger.error("package id `%s' appears more than once: excluded from sync" % (k))
                continue
            yield by_id[k]

    organization = get_organization(ckan, meta.organization)
    packages = meta.get_packages()
    packages = list(unique_packages())
    ckan_packages = sync_packages(ckan, packages, organization, None)
    resources = meta.get_resources()
    resources_add_format(resources)
    sync_resources(ckan, resources, meta.resource_linkage, ckan_packages, auth, num_threads, do_uploads)
