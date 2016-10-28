from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class MelanomaMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/melanoma/tracking/']
    auth = ('melanoma', 'melanoma')
    organization = 'bpa-melanoma'

    def __init__(self, metadata_path, track_csv_path=None):
        self.path = Path(metadata_path)

    def get_packages(self):
        return []

    def get_resources(self):
        return []
