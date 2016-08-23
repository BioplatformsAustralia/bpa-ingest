from __future__ import print_function

import ckanapi
from unipath import Path

from ..ops import make_group, ckan_method, patch_if_required, create_resource, check_resource, reupload_resource, get_size
from ..util import make_logger, bpa_id_to_ckan_name, prune_dict
from ..bpa import bpa_mirror_url, get_bpa
from .files import parse_file_data
from .samples import parse_sample_data
from .runs import parse_run_data, BLANK_RUN

logger = make_logger(__name__)


def sync_package(ckan, obj):
    try:
        ckan_obj = ckan_method(ckan, 'package', 'show')(id=obj['name'])
    except ckanapi.errors.NotFound:
        ckan_obj = ckan_method(ckan, 'package', 'create')(
            type=obj['type'], id=obj['id'], name=obj['name'],
            owner_org=obj['owner_org'])
        logger.info('created package object: %s' % (obj['id']))
    patch_obj = obj.copy()
    patch_obj['id'] = ckan_obj['id']
    was_patched, ckan_obj = patch_if_required(ckan, 'package', ckan_obj, patch_obj)
    if was_patched:
        logger.info('patched package object: %s' % (obj['id']))
    return ckan_obj


def sync_samples(ckan, group_obj, samples):
    bpa_org = get_bpa(ckan)
    packages = []
    api_group_obj = prune_dict(group_obj, ('display_name', 'description', 'title', 'image_display_url', 'id', 'name'))
    for bpa_id, data in samples.items():
        name = bpa_id_to_ckan_name(bpa_id)
        obj = {
            'owner_org': bpa_org['id'],
            'name': name,
            'groups': [api_group_obj],
            'id': bpa_id,
            'bpa_id': bpa_id,
            'title': bpa_id,
            'notes': '%s (%s): %s' % (data.variety, data.code, data.classification),
            'type': 'wheat-cultivars',
        }
        for field in ('source_name', 'code', 'characteristics', 'classification', 'organism', 'variety',
                      'organism_part', 'pedigree', 'dev_stage', 'yield_properties', 'morphology', 'maturity',
                      'pathogen_tolerance', 'drought_tolerance', 'soil_tolerance', 'url'):
            obj[field] = getattr(data, field)
        packages.append(sync_package(ckan, obj))
    return packages


def ckan_resource_from_file(package_obj, file_obj, run_obj):
    ckan_obj = {
        'id': file_obj['md5'],
        'package_id': package_obj['id'],
        'casava_version': run_obj['casava_version'],
        'library_construction_protocol': run_obj['library_construction_protocol'],
        'library_range': run_obj['library_range'],
        'run_number': run_obj['number'],
        'sequencer': run_obj['sequencer'],
        'barcode': file_obj['barcode'],
        'base_pairs': file_obj['base_pairs'],
        'name': file_obj['filename'],  # FIXME
        'filename': file_obj['filename'],
        'flowcell': file_obj['flowcell'],
        'lane_number': file_obj['lane_number'],
        'library_type': file_obj['library_type'],
        'md5': file_obj['md5'],
        'read_number': file_obj['read_number'],
    }
    url = bpa_mirror_url('wheat_cultivars/all/' + file_obj['filename'])
    return url, ckan_obj


def sync_files(ckan, packages, files, runs):
    # for each package, find the files which should attach to it, and
    # then sync up
    file_idx = {}
    for obj in files:
        bpa_id = obj['bpa_id']
        if bpa_id not in file_idx:
            file_idx[bpa_id] = []
        file_idx[bpa_id].append(obj)

    to_reupload = []
    for package in packages:
        files = file_idx[package['id']]
        # grab a copy of the package with all current resources
        package_obj = ckan_method(ckan, 'package', 'show')(id=package['id'])
        current_resources = package_obj['resources']
        existing_files = dict((t['id'], t) for t in current_resources)
        needed_files = dict((t['md5'], t) for t in files)
        to_create = set(needed_files) - set(existing_files)
        to_delete = set(existing_files) - set(needed_files)

        # check the existing resources exist, and have a size which matches the
        # legacy mirror
        for current_ckan_obj in current_resources:
            obj_id = current_ckan_obj['id']
            file_obj = needed_files[obj_id]
            run_obj = runs.get(file_obj['run'], BLANK_RUN)
            legacy_url, _ = ckan_resource_from_file(package_obj, file_obj, run_obj)
            current_url = current_ckan_obj.get('url')
            if not check_resource(ckan, current_url, legacy_url):
                logger.error('resource check failed, queued for re-upload: %s' % (obj_id))
                to_reupload.append((current_ckan_obj, legacy_url))
            else:
                logger.info('resource check OK')

        for obj_id in to_create:
            file_obj = needed_files[obj_id]
            run_obj = runs.get(file_obj['run'], BLANK_RUN)
            legacy_url, ckan_obj = ckan_resource_from_file(package_obj, file_obj, run_obj)
            if create_resource(ckan, ckan_obj, legacy_url):
                logger.info('created resource: %s' % (obj_id))

        for obj_id in to_delete:
            ckan_method(ckan, 'resource', 'delete')(id=obj_id)
            logger.info('deleted resource: %s' % (obj_id))

        # patch all the resources, to ensure everything is synced on
        # existing resources
        package_obj = ckan_method(ckan, 'package', 'show')(id=package['id'])
        current_resources = package_obj['resources']
        for current_ckan_obj in current_resources:
            obj_id = current_ckan_obj['id']
            file_obj = needed_files[obj_id]
            legacy_url, ckan_obj = ckan_resource_from_file(package_obj, file_obj, run_obj)
            was_patched, ckan_obj = patch_if_required(ckan, 'resource', current_ckan_obj, ckan_obj)
            if was_patched:
                logger.info('patched resource: %s' % (obj_id))

    for reupload_obj, legacy_url in sorted(to_reupload, key=lambda x: get_size(x[1], None)):
        reupload_resource(ckan, reupload_obj, legacy_url)


def ckan_sync_data(ckan, organism, group_obj, samples, runs, files):
    logger.info("syncing {} samples, {} runs, {} files".format(len(samples), len(runs), len(files)))
    # create the samples, if necessary, and sync them
    packages = sync_samples(ckan, group_obj, samples)
    sync_files(ckan, packages, files, runs)


def ingest(ckan, metadata_path):
    path = Path(metadata_path)
    group_obj = make_group(ckan, {
        'name': 'wheat-cultivars',
        'title': 'Wheat Cultivars',
        'display_name': 'Wheat Cultivars',
        'image_url': 'https://downloads.bioplatforms.com/static/wheat_cultivars/wheat.png',
    })
    organism = {'genus': 'Triticum', 'species': 'Aestivum'}
    runs = parse_run_data(path)
    samples = parse_sample_data(path)
    files = parse_file_data(path)
    ckan_sync_data(ckan, organism, group_obj, samples, runs, files)
