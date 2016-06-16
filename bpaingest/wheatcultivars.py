from __future__ import print_function

from unipath import Path
from collections import namedtuple

from .ops import update_or_create
from .util import make_logger
from .libs import ingest_utils
from .libs import bpa_id_utils
from .libs.excel_wrapper import ExcelWrapper
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


def parse_base_pair(val):
    if val.find("bp") > -1:
        return int(val[:-2])
    elif val.find("kb") > -1:
        return int(val[:-2]) * 1000


def make_protocol(**kwargs):
    fields = ('library_type', 'base_pairs', 'library_construction_protocol', 'sequencer')
    return dict((t, kwargs.get(t)) for t in fields)


def make_run(**kwargs):
    fields = ('number', 'casava_version', 'library_construction_protocol', 'library_range', 'sequencer')
    return dict((t, kwargs.get(t)) for t in fields)


def get_cultivar_sample_characteristics(file_name):
    """
    This is the data from the Characteristics Sheet
    """

    field_spec = [
        ("source_name", "BPA ID", None),
        ("code", "CODE", None),
        ("bpa_id", "BPA ID", lambda s: s.replace("/", ".")),
        ("characteristics", "Characteristics", None),
        ("organism", "Organism", None),
        ("variety", "Variety", None),
        ("organism_part", "Organism part", None),
        ("pedigree", "Pedigree", None),
        ("dev_stage", "Developmental stage", None),
        ("yield_properties", "Yield properties", None),
        ("morphology", "Morphology", None),
        ("maturity", "Maturity", None),
        ("pathogen_tolerance", "Pathogen tolerance", None),
        ("drought_tolerance", "Drought tolerance", None),
        ("soil_tolerance", "Soil tolerance", None),
        ("classification", "International classification", None),
        ("url", "Link", None),
    ]

    wrapper = ExcelWrapper(
        field_spec,
        file_name,
        sheet_name="Characteristics",
        header_length=1)
    return wrapper.get_all()


def get_run_lookup(path):
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
        run_lookup[key] = make_run(
            number=run.run_number,
            casava_version=run.casava_version,
            library_construction_protocol=run.library_construction_protocol,
            library_range=run.library_range,
            sequencer=run.sequencer)
    return run_lookup


def parse_md5_file(md5_file):
    """
    Parse md5 file
    PAS_AD08TAACXX_GCCAAT_L002_R1.fastq.gz
    """

    class MD5ParsedLine(object):
        Cultivar = namedtuple('Cultivar', 'desc bpa_id')
        cultivars = {
            'DRY': Cultivar('Drysdale', '102.100.100.13703'),
            'GLA': Cultivar('Gladius', '102.100.100.13704'),
            'RAC': Cultivar('RAC 875', '102.100.100.13705'),
            'EXC': Cultivar('Excalibur', '102.100.100.13706'),
            'KUK': Cultivar('Kukri', '102.100.100.13707'),
            'ACB': Cultivar('AC Barry', '102.100.100.13708'),
            'BAX': Cultivar('Baxter', '102.100.100.13709'),
            'CH7': Cultivar('Chara', '102.100.100.13710'),
            'VOL': Cultivar('Volcani DD1', '102.100.100.13711'),
            'WES': Cultivar('Westonia', '102.100.100.13712'),
            'PAS': Cultivar('Pastor', '102.100.100.13713'),
            'XIA': Cultivar('Xiaoyan 54', '102.100.100.13714'),
            'YIT': Cultivar('Yitpi', '102.100.100.13715'),
            'ALS': Cultivar('Alsen', '102.100.100.13716'),
            'WYA': Cultivar('Wyalcatchem', '102.100.100.13717'),
            'H45': Cultivar('H45', '102.100.100.13718'),
        }

        def __init__(self, line):
            self._line = line

            self.cultivar_key = None
            self.cultivar = None
            self.bpa_id = None
            self.lib_type = None
            self.lib_size = None
            self.flowcell = None
            self.barcode = None

            self.md5 = None
            self.filename = None

            self._lane = None
            self._read = None

            self._ok = False

            self.__parse_line()

        def is_ok(self):
            return self._ok

        @property
        def lane(self):
            return self._lane

        @lane.setter
        def lane(self, val):
            self._lane = int(val[1:])

        @property
        def read(self):
            return self._read

        @read.setter
        def read(self, val):
            self._read = int(val[1:])

        def __parse_line(self):
            """ unpack the md5 line """
            self.md5, self.filename = self._line.split()

            filename_parts = self.filename.split('.')[0].split('_')
            self.cultivar_key = filename_parts[0]

            # there are some files with an unknown cultivar code
            self.cultivar = self.cultivars.get(self.cultivar_key, None)
            if self.cultivar is None:
                self._ok = False
                return

            self.bpa_id = self.cultivar.bpa_id

            # WYA_PE_300bp_AD0ALYACXX_ATCACG_L003_R2.fastq.gz
            # [Cultivar_key]_[Library_Type]_[Library_Size]_[FLowcel]_[Barcode]_L[Lane_number]_R[Read_Number].
            if len(filename_parts) == 7:
                _key, self.lib_type, self.lib_size, self.flowcell, self.barcode, self.lane, self.read = filename_parts
                self._ok = True
            else:
                self._ok = False  # be explicit

    data = []

    with open(md5_file) as f:
        for line in f.read().splitlines():
            line = line.strip()
            if line == '':
                continue

            parsed_line = MD5ParsedLine(line)
            if parsed_line.is_ok():
                data.append(parsed_line)

    return data


def add_md5(md5_lines, run_data):
    """
    Add md5 data
    """

    organism = {
        'genus': 'Triticum',
        'species': 'Aestivum'
    }

    for md5_line in md5_lines:
        bpa_idx = md5_line.bpa_id
        bpa_id = bpa_id_utils.get_bpa_id(bpa_idx)
        if bpa_id is None:
            continue

        key = md5_line.bpa_id + md5_line.flowcell + md5_line.lib_type + md5_line.lib_size
        run = run_data.get(key, make_run(number=-1, casava_version="-", library_construction_protocol="-", library_range="-", sequencer="-"))
        protocol = make_protocol(
            library_type=md5_line.lib_type,
            base_pairs=parse_base_pair(md5_line.lib_size),
            library_construction_protocol=run['library_construction_protocol'],
            sequencer=run['sequencer'])
        file_info = {
            "sample": {
                'bpa_id': bpa_id,
                'organism': organism,
            },
            "protocol": protocol,
            "flowcell": md5_line.flowcell,
            "barcode": md5_line.barcode,
            "read_number": md5_line.read,
            "lane_number": md5_line.lane,
            "run_number": run['number'],
            "casava_version": run['casava_version'],
            "md5": md5_line.md5,
            "filename": md5_line.filename
        }
        from pprint import pprint
        pprint(file_info)


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
                  ('casava_version', 'CASAVA version', None),
                  ]

    wrapper = ExcelWrapper(
        field_spec,
        file_name,
        sheet_name='Metadata',
        header_length=1)
    return wrapper.get_all()


def do_md5(path):
    """
    Ingest the md5 files
    """

    run_data = get_run_lookup(path)

    def is_md5file(path):
        if path.isfile() and path.ext == '.md5':
            return True

    logger.info('Ingesting Wheat Cultivar md5 file information from {0}'.format(path))
    for md5_file in path.walk(filter=is_md5file):
        logger.info('Processing Wheat Cultivar md5 file {0}'.format(md5_file))
        data = parse_md5_file(md5_file)
        add_md5(data, run_data)


def do_metadata(path):
    def is_metadata(path):
        if path.isfile() and path.ext == '.xlsx':
            return True

    logger.info('Ingesting Wheat Cultivars metadata from {0}'.format(path))
    sample_info = []
    for metadata_file in path.walk(filter=is_metadata):
        logger.info('Processing Wheat Cultivars {0}'.format(metadata_file))
        sample_info += list(get_cultivar_sample_characteristics(metadata_file))
    sample_lookup = dict((t.bpa_id, t) for t in sample_info)
    return sample_lookup


def download(metadata_path, clean):
    fetcher = Fetcher(metadata_path, METADATA_URL)
    if clean:
        fetcher.clean()
    fetcher.fetch_metadata_from_folder()


def ingest(ckan, metadata_path):
    path = Path(metadata_path)
    group = make_group(ckan)
    sample_lookup = do_metadata(path)
    do_md5(path)
