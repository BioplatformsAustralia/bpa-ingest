

from unipath import Path

from ...util import make_logger
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class MelanomaMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/melanoma/tracking/']
    organization = 'bpa-melanoma'

    def __init__(self, metadata_path):
        super().__init__()
        self.path = Path(metadata_path)

    def _get_packages(self):
        return []

    def _get_resources(self):
        return []
