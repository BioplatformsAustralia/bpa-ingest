from __future__ import print_function

from ..libs.excel_wrapper import ExcelWrapper
from ..util import make_logger
from ..libs import ingest_utils

logger = make_logger(__name__)

def fix_dilution(val):
    """
    Some source xcell files ship with the dilution column type as time.
    xlrd advertises support for format strings but not implemented.
    """
    if isinstance(val, float):
        return u"1:10"  # yea, that's how we roll...
    return val


def fix_pcr(pcr):
    """
    Check pcr value
    """
    val = pcr.strip()
    if val not in ("P", "F", ""):
        logger.error("PCR value [{0}] is neither F, P or " ", setting to X".format(pcr.encode("utf8")))
        val = "X"
    return val


def get_amplicon_data(file_name):
    """ Get amplion data from metadata spreadsheets """

    field_spec = [
        ("bpa_id", "Sample unique ID", lambda s: s.replace("/", ".")),
        ("sample_extraction_id", "Sample extraction ID", ingest_utils.get_int),
        ("sequencing_facility", "Sequencing facility", None),
        ("amplicon", "Target", lambda s: s.upper().strip()),
        ("i7_index", "I7_Index_ID", None),
        ("index1", "index", None),
        ("index2", "index2", None),
        ("pcr_1_to_10", "1:10 PCR, P=pass, F=fail", fix_pcr),
        ("pcr_1_to_100", "1:100 PCR, P=pass, F=fail", fix_pcr),
        ("pcr_neat", "neat PCR, P=pass, F=fail", fix_pcr),
        ("dilution", "Dilution used", fix_dilution),
        ("sequencing_run_number", "Sequencing run number", None),
        ("flow_cell_id", "Flowcell", None),
        ("reads", "# of reads", ingest_utils.get_int),
        ("name", "Sample name on sample sheet", None),
        ("analysis_software_version", "AnalysisSoftwareVersion", None),
        ("comments", "Comments", None),
    ]

    wrapper = ExcelWrapper(field_spec,
                           file_name,
                           sheet_name="Sheet1",
                           header_length=4,
                           column_name_row_index=1,
                           formatting_info=True,
                           pick_first_sheet=True)

    return wrapper.get_all()


def parse_metadata(path):
    def is_metadata(path):
        if path.isfile() and path.ext == '.xlsx':
            return True

    logger.info('Ingesting GBR Amplicon metadata from {0}'.format(path))
    rows = []
    for metadata_file in path.walk(filter=is_metadata):
        logger.info('Processing GBR Amplicon {0}'.format(metadata_file))
        for sample in get_amplicon_data(metadata_file):
            rows.append(sample)
    return rows
