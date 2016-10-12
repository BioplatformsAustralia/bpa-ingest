from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class MarineMicrobesMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/tracking/'
    auth = ('marine', 'marine')
    organization = 'bpa-marine-microbes'

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

    def get_packages(self):
        return []

    def get_resources(self):
        return []
