from unipath import Path

from ...abstract import BaseMetadata


class BarcodeMetadata(BaseMetadata):
    metadata_urls = ["https://downloads-qcif.bioplatforms.com/bpa/barcode/tracking/"]
    organization = "bpa-barcode"

    def __init__(self, logger, metadata_path):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)

    def _get_packages(self):
        return []

    def _get_resources(self):
        return []
