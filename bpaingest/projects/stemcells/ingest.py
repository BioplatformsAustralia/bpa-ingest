from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata
from ...bpa import BPA_ORGANIZATION_ID

logger = make_logger(__name__)


class StemcellsMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/stemcells/tracking/'
    parent_organization = BPA_ORGANIZATION_ID

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

    def get_organization(self):
        # Markdown
        desc = """
Stem cells allow us to study fundamental processes in tissue growth, development, aging and disease. The next waves of medicine will build on personalised therapies for drug treatments that require an understanding of drug-genetic-tissue interactions. Stem cell programs that can direct the differentiation of cells lead to the assembly of mini-organs in a dish that are already in use for disease screening in Australia, and around the world.

http://www.bioplatforms.com/stem-cells/
        """
        return {
            'name': 'bpa-stemcells',
            'title': 'Stemcells',
            'display_name': 'Stemcells',
            'image_url': 'https://data.bioplatforms.com/stemcell.png',
            'description': desc
        }

    def get_packages(self):
        return []

    def get_resources(self):
        return []
