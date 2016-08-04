import os
import re

from ..libs import ingest_utils
from ..libs import md5parser
from ..util import make_logger

logger = make_logger(__name__)
AMPLICON_FILENAME_PATTERN = """
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<library>PE|MP)_
    (?P<size>\d*bp)_
    SEP_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_cell_id>\w{5})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>\S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
"""
AMPLICON_FILENAME = re.compile(AMPLICON_FILENAME_PATTERN, re.VERBOSE)


def _file_from_row(e):
    def get_file_name(_fname):
        """ The filenames in the spreadsheet has paths prepended, strip them out """
        head, tail = os.path.split(_fname.strip())
        return tail

    obj = {
        'lane_number': ingest_utils.get_clean_number(e.lane_number),
        'filename': get_file_name(e.sequence_filename),
        'name': get_file_name(e.sequence_filename),
        'md5': e.md5_checksum,
        'file_size': e.file_size,
        'note': ingest_utils.pretty_print_namedtuple(e),
    }
    return e.bpa_id, obj



def files_from_md5(path):
    def is_md5(path):
        if path.isfile() and path.ext == '.md5':
            return True

    logger.info('Ingesting GBR Amplicon File data from md5 files found in {0}'.format(path))
    files = []
    for md5_file in path.walk(filter=is_md5):
        logger.info('Processing GBR md5 checksum file {0}'.format(md5_file))
        for sample in get_amplicon_data(metadata_file):
            rows.append(sample)
    return rows
