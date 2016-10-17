from __future__ import print_function

from unipath import Path

from ...util import make_logger
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class BarcodeMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/barcode/tracking/'
    organization = 'bpa-barcode'

    def __init__(self, metadata_path, track_csv_path=None):
        self.path = Path(metadata_path)

    def get_packages(self):
        return []

    def get_resources(self):
        return []
