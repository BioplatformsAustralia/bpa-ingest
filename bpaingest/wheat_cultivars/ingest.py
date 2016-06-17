from __future__ import print_function

from unipath import Path
from pprint import pprint

from ..ops import update_or_create
from ..util import make_logger
from .files import parse_file_data
from .samples import parse_sample_data
from .runs import parse_run_data

logger = make_logger(__name__)


def make_group(ckan):
    return update_or_create(ckan, 'group', {
        'name': 'wheat-cultivars',
        'title': 'Wheat Cultivars',
        'display_name': 'Wheat Cultivars'
    })


def ckan_sync_data(organism, samples, runs, files):
    logger.info("syncing {} samples, {} runs, {} files".format(len(samples), len(runs), len(files)))
    print("example sample")
    pprint(samples[samples.keys()[0]])
    print("example run")
    pprint(runs[runs.keys()[0]])
    print("example file")
    pprint(files[0])


def ingest(ckan, metadata_path):
    path = Path(metadata_path)
    make_group(ckan)
    organism = {
        'genus': 'Triticum',
        'species': 'Aestivum'
    }
    runs = parse_run_data(path)
    samples = parse_sample_data(path)
    files = parse_file_data(path)
    ckan_sync_data(organism, samples, runs, files)
