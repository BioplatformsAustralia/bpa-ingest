from ..libs.excel_wrapper import ExcelWrapper
from ..libs import ingest_utils
from ..util import make_logger

logger = make_logger(__name__)


def make_run(**kwargs):
    fields = ('number', 'casava_version', 'library_construction_protocol', 'library_range', 'sequencer')
    return dict((t, kwargs.get(t)) for t in fields)


BLANK_RUN = make_run(number=-1, casava_version="-", library_construction_protocol="-", library_range="-", sequencer="-")


def get_run_data(file_name):
    """
    The run metadata for this set
    """

    field_spec = [('bpa_id', 'Soil sample unique ID', lambda s: s.replace('/', '.')),
                  ('variety', 'Variety', None),
                  ('cultivar_code', 'Code', None),
                  ('library', 'Library code', None),
                  ('library_construction', 'Library Construction - average insert size', None),
                  ('library_range', 'Range', None),
                  ('library_construction_protocol', 'Library construction protocol', None),
                  ('sequencer', 'Sequencer', None),
                  ('run_number', 'Run number', ingest_utils.get_clean_number),
                  ('flowcell', 'Flow Cell ID', None),
                  ('index', 'Index', None),
                  ('casava_version', 'CASAVA version', None), ]

    wrapper = ExcelWrapper(field_spec, file_name, sheet_name='Metadata', header_length=1)
    return wrapper.get_all()


def parse_run_data(path):
    """
    Run data is uniquely defined by
    - bpa_id
    - flowcell
    - library type
    - library size

    Values recorded per key are:
    - run_number
    - CASAVA version
    - library construction protocol
    - sequencer
    - range
    """

    def is_metadata(path):
        if path.isfile() and path.ext == '.xlsx':
            return True

    logger.info('Ingesting Wheat Cultivars run metadata from {0}'.format(path))
    run_data = {}

    for metadata_file in path.walk(filter=is_metadata):
        logger.info('Processing Wheat Cultivars {0}'.format(metadata_file))
        run_data = list(get_run_data(metadata_file))

    run_lookup = {}

    for run in run_data:
        key = run.bpa_id + run.flowcell + run.library + run.library_construction
        run_lookup[key] = make_run(number=run.run_number,
                                   casava_version=run.casava_version,
                                   library_construction_protocol=run.library_construction_protocol,
                                   library_range=run.library_range,
                                   sequencer=run.sequencer)
    return run_lookup
