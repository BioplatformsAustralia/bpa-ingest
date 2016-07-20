import os

from ..libs import ingest_utils
from ..util import make_logger

logger = make_logger(__name__)


def run_from_row(e):
    obj = {
        'flowcell': e.flow_cell_id,
        'run_number': ingest_utils.get_clean_number(e.run_number),
        'sequencer': e.sequencer or "Unknown",
        'run_index_number': e.index_number,
        'run_lane_number': ingest_utils.get_clean_number(e.lane_number),
        'run_protocol': e.library_construction_protocol,
        'run_protocol_base_pairs': ingest_utils.get_clean_number(e.library_construction),
        'run_protocol_library_type': e.library,
    }
    return e.bpa_id, obj


def file_from_row(e):
    def get_file_name(_fname):
        """ The filenames in the spreadsheet has paths prepended, strip them out """
        head, tail = os.path.split(_fname.strip())
        return tail
    obj = {
        'index_number': ingest_utils.get_clean_number(e.index_number),
        'lane_number': ingest_utils.get_clean_number(e.lane_number),
        'filename': get_file_name(e.sequence_filename),
        'md5': e.md5_checksum,
        'file_size': e.file_size,
        'note': ingest_utils.pretty_print_namedtuple(e),
    }
    return obj


def files_from_metadata(metadata):
    files = []
    for row in metadata:
        bpa_id, obj = run_from_row(row)
        obj.update(file_from_row(row))
        files.append((bpa_id, obj))
    return files
