import os

from ..libs import ingest_utils
from ..util import make_logger

logger = make_logger(__name__)


def _file_from_row(e):
    print(e)
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


def files_from_metadata(metadata):
    files = []
    for row in metadata:
        bpa_id, obj = _file_from_row(row)
        files.append((bpa_id, obj))
    return files
