from __future__ import print_function

import ckanapi
from unipath import Path
from pprint import pprint

from ..ops import update_or_create
from ..util import make_logger, bpa_id_to_ckan_name
from ..bpa import BPA_ID
from .files import parse_file_data
from .samples import parse_sample_data
from .runs import parse_run_data

logger = make_logger(__name__)


def make_group(ckan):
    return update_or_create(ckan, 'group', {
        'name': 'wheat-cultivars',
        'title': 'Wheat Cultivars',
        'display_name': 'Wheat Cultivars'
    })


def sync_package(ckan, obj):
    print(obj)
    try:
        ckan_obj = ckan.action.package_show(id=obj['name'])
    except ckanapi.errors.NotFound:
        ckan_obj = ckan.action.package_create(type=obj['type'], id=obj['id'], name=obj['name'], owner_org=obj['owner_org'])
    print(ckan_obj)
    updated_obj = ckan.action.package_patch(**obj)
    pprint(updated_obj)


def sync_samples(ckan, samples):
    for bpa_id, data in samples.items():
        name = bpa_id_to_ckan_name(bpa_id)
        obj = {
            'owner_org': BPA_ID,
            'name': name,
            'id': bpa_id,
            'title': bpa_id,
            'type': 'wheat-cultivars',
        }
        for field in ('source_name', 'code', 'characteristics', 'organism', 'variety', 'organism_part', 'pedigree', 'dev_stage', 'yield_properties', 'morphology', 'maturity', 'pathogen_tolerance', 'drought_tolerance', 'soil_tolerance', 'url'):
            obj[field] = getattr(data, field)
        sync_package(ckan, obj)


def ckan_sync_data(ckan, organism, samples, runs, files):
    logger.info("syncing {} samples, {} runs, {} files".format(len(samples), len(runs), len(files)))
    # create the samples, if necessary, and sync them
    sync_samples(ckan, samples)
    print("example sample")
    pprint(samples[samples.keys()[0]])
    print("example run")
    pprint(runs[runs.keys()[0]])
    print("example file")
    pprint(files[0])


def ingest(ckan, metadata_path):
    path = Path(metadata_path)
    make_group(ckan)
    organism = {
        'genus': 'Triticum',
        'species': 'Aestivum'
    }
    runs = parse_run_data(path)
    samples = parse_sample_data(path)
    files = parse_file_data(path)
    ckan_sync_data(ckan, organism, samples, runs, files)
