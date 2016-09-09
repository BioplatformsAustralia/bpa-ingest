from __future__ import print_function

from unipath import Path

from ...abstract import BaseMetadata

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url

logger = make_logger(__name__)


class SoilMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/base/tracking/'

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

    def get_group(self):
        # Markdown
        desc = """
        The Biome of Australia Soil Environments (BASE) is a collaborative project to create a
        public resource containing microbial genome information from a range of Australian
        soil environments.
        """
        return {
            'name': 'base',
            'title': 'BASE',
            'display_name': 'Biome of Australian Soil Environments',
            'image_url': 'https://data.bioplatforms.com/base.png',
            'description': desc
        }

    def get_packages(self):
        return []

    def get_resources(self):
        return []
