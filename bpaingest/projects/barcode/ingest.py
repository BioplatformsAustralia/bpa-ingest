from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class BarcodeMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/barcode/tracking/'

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

    def get_group(self):
        # Markdown
        desc = """
Of the estimated 10 million species that exist on our planet, only just over a million have so far been identified and described.

Using traditional taxonomy, it would take at least another 2000 years to identify Earth's remaining species. With DNA barcoding, we can vastly accelerate this rate of biodiversity discovery and conservation as well as generate significant scientific and economic benefits for the mining, fisheries and forestry industries.

The national collaborative project will focus on five key areas with immediate strategic value:

 - verifying timber origins to combat illegal timber trading
 - authenticating labelling and geographical origin of fish in the retail marketplace
 - mapping plant biodiversity in the Pilbara to help with mine site environmental impact assessment and restoration management
 - biodiversity discovery and impact assessment of invertebrates that inhabit underground aquifers utilised by mining and farming
 - generating barcodes for Australia's orchids to enhance conservation.

For more information please visit:
http://www.bioplatforms.com/dna-barcoding/
        """
        return {
            'name': 'barcode',
            'title': 'Barcode',
            'display_name': 'Barcode',
            'image_url': 'https://data.bioplatforms.com/barcode.png',
            'description': desc
        }

    def get_packages(self):
        return []

    def get_resources(self):
        return []
