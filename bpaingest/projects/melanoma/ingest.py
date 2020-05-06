from unipath import Path

from ...abstract import BaseMetadata


class MelanomaMetadata(BaseMetadata):
    metadata_urls = ["https://downloads-qcif.bioplatforms.com/bpa/melanoma/tracking/"]
    organization = "bpa-melanoma"

    def __init__(self, logger, metadata_path):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)

    def _get_packages(self):
        return []

    def _get_resources(self):
        return []
