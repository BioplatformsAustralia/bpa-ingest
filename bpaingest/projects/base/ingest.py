from __future__ import print_function

from unipath import Path

from ...abstract import BaseMetadata

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url

logger = make_logger(__name__)


class SoilMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/base/tracking/'
    organization = 'bpa-base'

    def __init__(self, metadata_path, track_csv_path=None):
        self.path = Path(metadata_path)

    def get_packages(self):
        return []

    def get_resources(self):
        return []
