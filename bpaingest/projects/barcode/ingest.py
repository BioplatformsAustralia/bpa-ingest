

from unipath import Path

from ...util import make_logger
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class BarcodeMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/barcode/tracking/']
    organization = 'bpa-barcode'

    def __init__(self, metadata_path):
        super().__init__()
        self.path = Path(metadata_path)

    def _get_packages(self):
        return []

    def _get_resources(self):
        return []
