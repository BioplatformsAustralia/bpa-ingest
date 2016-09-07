from __future__ import print_function

import ckanapi
from unipath import Path

import bpaingest.ops as ops
from ..util import make_logger, bpa_id_to_ckan_name, prune_dict
from ..bpa import bpa_mirror_url, get_bpa
from .metadata import parse_metadata
from .samples import samples_from_metadata
from .files import files_from_md5
from ..libs.fetch_data import get_password

logger = make_logger(__name__)


def sync_package(ckan, obj):
    try:
        ckan_obj = ops.ckan_method(ckan, 'package', 'show')(id=obj['name'])
    except ckanapi.errors.NotFound:
        ckan_obj = ops.ckan_method(ckan, 'package', 'create')(
            type=obj['type'], id=obj['id'], name=obj['name'],
            owner_org=obj['owner_org'])
        logger.info('created package object: %s' % (obj['id']))
    patch_obj = obj.copy()
    patch_obj['id'] = ckan_obj['id']
    was_patched, ckan_obj = ops.patch_if_required(ckan, 'package', ckan_obj, patch_obj)
    if was_patched:
        logger.info('patched package object: %s' % (obj['id']))
    return ckan_obj


def sync_samples(ckan, group_obj, samples):
    bpa_org = get_bpa(ckan)
    packages = []
    api_group_obj = prune_dict(group_obj, ('display_name', 'description', 'title', 'image_display_url', 'id', 'name'))
    for bpa_id, data in samples.items():
        name = bpa_id_to_ckan_name(bpa_id)
        obj = data.copy()
        obj.update({
            'owner_org': bpa_org['id'],
            'name': name,
            'groups': [api_group_obj],
            'id': bpa_id,
            'bpa_id': bpa_id,
            'title': 'Amplicon {}'.format(bpa_id),
            'notes': 'Amplicon Data for Great Barrier Reef Sample {}'.format(bpa_id),
            'type': 'great-barrier-reef-metagenomics',
        })
        packages.append(sync_package(ckan, obj))
    return packages


def ckan_resource_from_file(package_obj, file_obj):
    ckan_obj = file_obj.copy()
    url = bpa_mirror_url('gbr/amplicons/{}/{}'.format(file_obj['amplicon'].lower(), file_obj['filename']))
    ckan_obj.update({
        'id': file_obj['md5'],
        'package_id': package_obj['id'],
    })
    return url, ckan_obj


def sync_files(ckan, packages, files):
    # for each package, find the files which should attach to it, and
    # then sync up
    file_idx = {}
    for bpa_id, obj in files:
        if bpa_id not in file_idx:
            file_idx[bpa_id] = []
        file_idx[bpa_id].append(obj)

    auth = ('bpa', get_password('gbr'))

    for package in packages:
        files = file_idx.get(package['id'], [])
        # grab a copy of the package with all current resources
        package_obj = ops.ckan_method(ckan, 'package', 'show')(id=package['id'])
        current_resources = package_obj['resources']
        existing_files = dict((t['id'], t) for t in current_resources)
        needed_files = dict((t['md5'], t) for t in files)
        to_create = set(needed_files) - set(existing_files)
        to_delete = set(existing_files) - set(needed_files)

        # check the existing resources exist, and have a size which matches the
        # legacy mirror
        to_reupload = []
        for current_ckan_obj in current_resources:
            obj_id = current_ckan_obj['id']
            file_obj = needed_files[obj_id]
            legacy_url, _ = ckan_resource_from_file(package_obj, file_obj)
            current_url = current_ckan_obj.get('url')
            if not current_url or not ops.check_resource(ckan, current_url, legacy_url, auth):
                logger.error('resource check failed, queued for re-upload: %s' % (obj_id))
                to_reupload.append(current_ckan_obj)
            else:
                logger.info('resource check OK')

        for obj_id in to_create:
            file_obj = needed_files[obj_id]
            legacy_url, ckan_obj = ckan_resource_from_file(package_obj, file_obj)
            ops.create_resource(ckan, ckan_obj, legacy_url, auth=auth)
            logger.info('created resource: %s' % (obj_id))

        for obj_id in to_delete:
            ops.ckan_method(ckan, 'resource', 'delete')(id=obj_id)
            logger.info('deleted resource: %s' % (obj_id))

        for reupload_obj in to_reupload:
            obj_id = reupload_obj['id']
            file_obj = needed_files[obj_id]
            legacy_url, ckan_obj = ckan_resource_from_file(package_obj, file_obj)
            ops.reupload_resource(ckan, reupload_obj, legacy_url, auth=auth)

        # patch all the resources, to ensure everything is synced on
        # existing resources
        package_obj = ops.ckan_method(ckan, 'package', 'show')(id=package['id'])
        current_resources = package_obj['resources']
        for current_ckan_obj in current_resources:
            obj_id = current_ckan_obj['id']
            file_obj = needed_files[obj_id]
            legacy_url, ckan_obj = ckan_resource_from_file(package_obj, file_obj)
            was_patched, ckan_obj = ops.patch_if_required(ckan, 'resource', current_ckan_obj, ckan_obj)
            if was_patched:
                logger.info('patched resource: %s' % (obj_id))


def ckan_sync_data(ckan, group_obj, samples, files):
    logger.info("syncing {} samples, {} files".format(len(samples), len(files)))
    # create the samples, if necessary, and sync them
    packages = sync_samples(ckan, group_obj, samples)
    sync_files(ckan, packages, files)


def ingest(ckan, metadata_path):
    path = Path(metadata_path)
    group_obj = ops.make_group(ckan, {
        'name': 'great_barrier_reef',
        'title': 'Great Barrier Reef',
        'display_name': 'Great Barrier Reef',
        'image_url': 'https://downloads.bioplatforms.com/static/gbr/coral.png',
    })
    metadata = parse_metadata(path)
    samples = samples_from_metadata(metadata)

    files = files_from_md5(path)
    ckan_sync_data(ckan, group_obj, samples, files)
