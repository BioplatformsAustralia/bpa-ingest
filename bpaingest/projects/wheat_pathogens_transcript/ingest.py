from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata
from ...bpa import BPA_ORGANIZATION_ID

logger = make_logger(__name__)


class WheatPathogensTranscriptMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/wheat_pathogens/tracking/'
    parent_organization = BPA_ORGANIZATION_ID
    auth = ('marine', 'mm')

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

    def get_organization(self):
        # Markdown
        desc = """
This dataset contains transcript sequence data from 8 different fungal pathogen species of wheat.

The samples analysed cover various developmental stages of the pathogens and their interaction with the wheat host.

The pathogens included are the causal agents of stripe rust, stem rust, tan spot, glume blotch, bare patch and crown rot/head blight.

The data for generation was prioritised by a consortium of Australian wheat pathogen researchers from the following organisations:

 - Australian National University
 - CSIRO
 - Curtin University
 - Charles Sturt University
 - NSW Department of Primary industry

For more information please visit: http://www.bioplatforms.com/wheat-defense/
        """
        return {
            'name': 'bpa-wheat-pathogens-transcript',
            'title': 'Wheat Pathogens Transcript',
            'display_name': 'Wheat Pathogens Transcript',
            'image_url': 'https://data.bioplatforms.com/wheat.png',
            'description': desc
        }

    def get_packages(self):
        return []

    def get_resources(self):
        return []
