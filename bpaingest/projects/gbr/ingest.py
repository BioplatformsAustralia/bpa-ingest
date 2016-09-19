from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from .metadata import parse_metadata
from .samples import samples_from_metadata
from .files import files_from_md5
from ...abstract import BaseMetadata
from ...bpa import BPA_ORGANIZATION_ID

logger = make_logger(__name__)


class GbrMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/gbr/metadata/amplicons/'
    parent_organization = BPA_ORGANIZATION_ID
    auth = ("bpa", "gbr")

    def __init__(self, metadata_path):
        path = Path(metadata_path)
        self.metadata = parse_metadata(path)
        self.files = files_from_md5(path)

    def get_organization(self):
        # Markdown
        desc = """
The Sea-quence Project is generating core genetic data for corals from the Great Barrier Reef and Red Sea to ultimately help guide reef management practices. The project aims to sequence the genomes of 10 coral species across 6 different coral types, 3-4 algal symbionts and generate a new suite of microbial symbiont sequence data. This project is an initiative of the ReFuGe 2020 Consortium. Membership of the consortium includes:

 - Great Barrier Reef Foundation
 - James Cook University
 - Australian Institute of Marine Science
 - University of Queensland
 - The Great Barrier Reef Marine Park Authority
 - King Abdullah University of Science and Technology (Saudi Arabia)
 - Australian National University
 - Bioplatforms Australia
 
For more information please visit: http://www.bioplatforms.com/great-barrier-reef/
        """
        return {
            'name': 'bpa-great-barrier-reef',
            'title': 'Great Barrier Reef',
            'display_name': 'Great Barrier Reef',
            'image_url': 'https://data.bioplatforms.com/coral.png',
            'description': desc,
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
                'title': 'Amplicon {}'.format(bpa_id),
                'notes': 'Amplicon Data for Great Barrier Reef Sample {}'.format(bpa_id),
                # 'tags': [{'Amplicon': data['amplicon']}],
                'type': 'great-barrier-reef-amplicon',
                'private': True,
            })
            packages.append(obj)
        return packages

    def get_resources(self):
        resources = []
        for bpa_id, file_obj in self.files:
            legacy_url, resource = ckan_resource_from_file(file_obj)
            resources.append((bpa_id, legacy_url, resource))
        return resources


def ckan_resource_from_file(file_obj):
    ckan_obj = file_obj.copy()
    url = bpa_mirror_url('gbr/amplicons/{}/{}'.format(file_obj['amplicon'].lower(), file_obj['filename']))
    ckan_obj.update({
        'id': file_obj['md5']
    })
    return url, ckan_obj
