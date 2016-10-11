from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata
from ...util import clean_tag_name
from .files import parse_file_data
from .samples import parse_sample_data
from .runs import parse_run_data, BLANK_RUN

logger = make_logger(__name__)


class WheatCultivarsMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/wheat_cultivars/tracking/'
    organization = 'bpa-wheat-cultivars'

    def __init__(self, metadata_path):
        path = Path(metadata_path)
        self.runs = parse_run_data(path)
        self.samples = parse_sample_data(path)
        self.files = parse_file_data(path)

    def get_packages(self):
        packages = []
        for bpa_id, data in self.samples.items():
            name = bpa_id_to_ckan_name(bpa_id)
            obj = {
                'name': name,
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
            tag_names = []
            if obj['organism']:
                tag_names.append(clean_tag_name(obj['organism']))
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_resources(self):
        resources = []
        for file_obj in self.files:
            run_obj = self.runs.get(file_obj['run'], BLANK_RUN)
            legacy_url, resource = ckan_resource_from_file(file_obj, run_obj)
            resources.append((file_obj['bpa_id'], legacy_url, resource))
        return resources


def ckan_resource_from_file(file_obj, run_obj):
    ckan_obj = {
        'id': file_obj['md5'],
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
