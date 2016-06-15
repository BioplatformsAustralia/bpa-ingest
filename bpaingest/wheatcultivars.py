from __future__ import print_function

from .ops import update_or_create
from unipath import Path
from .util import make_logger
from .libs import ingest_utils
from .libs.fetch_data import Fetcher

# all metadata and checksums should be linked out here
METADATA_URL = 'https://downloads-qcif.bioplatforms.com/bpa/wheat_cultivars/tracking/'

logger = make_logger('wheatcultivars')


def make_group(ckan):
    return update_or_create(ckan, 'group', {
        'name': 'wheat-cultivars',
        'title': 'Wheat Cultivars',
        'display_name': 'Wheat Cultivars'
    })


def do_metadata(path):
    def is_metadata(path):
        if path.isfile() and path.ext == '.xlsx':
            return True

    logger.info('Ingesting Wheat Cultivars metadata from {0}'.format(path))
    for metadata_file in path.walk(filter=is_metadata):
        logger.info('Processing Wheat Cultivars {0}'.format(metadata_file))
        # sample_data = list(get_cultivar_sample_characteristics(metadata_file))
        # bpa_id_utils.ingest_bpa_ids(sample_data, 'WHEAT_CULTIVAR', 'Wheat Cultivars')
        # ingest_samples(sample_data)


def download(metadata_path, clean):
    fetcher = Fetcher(metadata_path, METADATA_URL)
    if clean:
        fetcher.clean()
    fetcher.fetch_metadata_from_folder()


def ingest(ckan, metadata_path):
    path = Path(metadata_path)
    group = make_group(ckan)
    do_metadata(path)
