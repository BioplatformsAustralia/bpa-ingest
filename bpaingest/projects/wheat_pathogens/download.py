from __future__ import print_function
from ...libs.fetch_data import Fetcher
from ...util import make_logger

# all metadata and checksums should be linked out here
METADATA_URL = 'https://downloads-qcif.bioplatforms.com/bpa/wheat_pathogens/metadata/'

logger = make_logger(__name__)


def download(metadata_path, clean):
    fetcher = Fetcher(metadata_path, METADATA_URL)
    if clean:
        fetcher.clean()
    fetcher.fetch_metadata_from_folder()
