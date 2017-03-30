from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata

from .metadata import parse_metadata
from .samples import samples_from_metadata
from .files import files_from_metadata

logger = make_logger(__name__)


class WheatPathogensGenomesMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/wheat_pathogens/metadata/']
    organization = 'bpa-wheat-pathogens-genomes'
    ckan_data_type = 'wheat-pathogens'

    def __init__(self, metadata_path, track_csv_path=None, metadata_info=None):
        path = Path(metadata_path)
        self.metadata = parse_metadata(path)

    def get_packages(self):
        packages = []
        for bpa_id, data in samples_from_metadata(self.metadata).items():
            name = bpa_id_to_ckan_name(bpa_id)
            obj = data.copy()
            obj.update({
                'name': name,
                'id': bpa_id,
                'bpa_id': bpa_id,
                'title': bpa_id,
                'notes': '%s' % (data['official_variety_name']),
                'type': self.ckan_data_type,
            })
            packages.append(obj)
        return packages

    def get_resources(self):
        resources = []
        for bpa_id, file_obj in files_from_metadata(self.metadata):
            legacy_url, resource = ckan_resource_from_file(file_obj)
            resources.append(((bpa_id,), legacy_url, resource))
        return resources


def ckan_resource_from_file(file_obj):
    ckan_obj = file_obj.copy()
    url = bpa_mirror_url('wheat_pathogens/all/' + file_obj['filename'])
    ckan_obj.update({
        'id': file_obj['md5'],
        'resource_type': WheatPathogensGenomesMetadata.ckan_data_type,
    })
    return url, ckan_obj
