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
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/wheat_pathogens/metadata/'

    def __init__(self, metadata_path):
        path = Path(metadata_path)
        self.metadata = parse_metadata(path)

    def get_group(self):
        desc = """
This dataset contains the genomic sequence from 10 fungal and 2 bacterial pathogen species. Among the pathogens sequenced are the causal agents of stripe rust, stem rust, tan spot, glume blotch, septoria leaf blotch, bare patch and crown rot/head blight. A total of 27 genomes will be made available.

The genomes were selected for analysis by a consortium of Australian wheat pathogen researchers from the following organisations:

 - Australian National University
 - CSIRO
 - Curtin University
 - Charles Sturt University
 - NSW Department of Primary industry
 - Grains Research and Development Corporation

For more information please visit: http://www.bioplatforms.com/wheat-defense/
"""
        return {
            'name': 'wheat-pathogens',
            'title': 'Wheat Pathogens Genomes',
            'display_name': 'Wheat Pathogens',
            'image_url': 'https://data.bioplatforms.com/stagonospora_nodorum.png',
        }

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
                'type': 'wheat-pathogens',
            })
            packages.append(obj)
        return packages

    def get_resources(self):
        resources = []
        for bpa_id, file_obj in files_from_metadata(self.metadata):
            legacy_url, resource = ckan_resource_from_file(file_obj)
            resources.append((bpa_id, legacy_url, resource))
        return resources


def ckan_resource_from_file(file_obj):
    ckan_obj = file_obj.copy()
    url = bpa_mirror_url('wheat_pathogens/all/' + file_obj['filename'])
    ckan_obj.update({
        'id': file_obj['md5'],
        'resource_type': 'wheat-pathogens'
    })
    return url, ckan_obj
