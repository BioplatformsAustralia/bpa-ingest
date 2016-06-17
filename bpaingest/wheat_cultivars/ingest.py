from __future__ import print_function

from unipath import Path
from pprint import pprint

from ..ops import update_or_create
from ..util import make_logger
from .files import parse_file_data
from .samples import parse_sample_data

logger = make_logger(__name__)


def make_group(ckan):
    return update_or_create(ckan, 'group', {
        'name': 'wheat-cultivars',
        'title': 'Wheat Cultivars',
        'display_name': 'Wheat Cultivars'
    })


def ingest(ckan, metadata_path):
    path = Path(metadata_path)
    make_group(ckan)

    samples = parse_sample_data(path)
    files = parse_file_data(path)
    logger.info("{} samples, {} files".format(len(samples), len(files)))
    pprint(samples[0])
