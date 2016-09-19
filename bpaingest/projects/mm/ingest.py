from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata
from ...bpa import BPA_ORGANIZATION_ID

logger = make_logger(__name__)


class MarineMicrobesMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/tracking/'
    auth = ('marine', 'mm')
    parent_organization = BPA_ORGANIZATION_ID

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

    def get_organization(self):
        # Markdown
        desc = """
The Marine Microbes project will establish how Australia's marine microbial communities change over time in various locations and environments. The consortium of researchers will investigate the microbial communities of seawater, sediment, sponges and sea grass utilising the extensive capability of Australia's Integrated Marine Observing System (IMOS).

For more information please visit: http://www.bioplatforms.com/marine-microbes/
        """
        return {
            'name': 'bpa-marine-microbes',
            'title': 'Marine Microbes',
            'display_name': 'Marine Microbes',
            'image_url': 'https://data.bioplatforms.com/marine_microbes.png',
            'description': desc
        }

    def get_packages(self):
        return []

    def get_resources(self):
        return []
