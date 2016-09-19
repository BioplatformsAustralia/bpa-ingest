from __future__ import print_function

from unipath import Path

from ...abstract import BaseMetadata

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...bpa import BPA_ORGANIZATION_ID

logger = make_logger(__name__)


class SoilMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/base/tracking/'
    parent_organization = BPA_ORGANIZATION_ID

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

    def get_organization(self):
        # Markdown
        desc = """
The Biome of Australia Soil Environments (BASE) is a collaborative project to create a public resource containing microbial genome information from a range of Australian soil environments.

Soil along with historical, physical and chemical contextual information (including photos) has been collected from 600+ diverse sites around Australia including Christmas Island and the Australian Antarctic Territory.

DNA is extracted from all samples using a standardised protocol (PowerSoil, MO BIO) and amplicon analysis undertaken using the Illumina MiSeq platform for bacterial 16S, fungal ITS and eukaryotic 18S targets.

OTU data has been generated through standardised pipelines for each target and is available through this repository.

For more information please visit: http://www.bioplatforms.com/soil-biodiversity
        """
        return {
            'name': 'bpa-base',
            'title': 'BASE',
            'display_name': 'Biome of Australian Soil Environments',
            'image_url': 'https://data.bioplatforms.com/base.png',
            'description': desc
        }

    def get_packages(self):
        return []

    def get_resources(self):
        return []
