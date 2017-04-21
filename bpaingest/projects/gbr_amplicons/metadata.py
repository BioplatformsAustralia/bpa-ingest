from __future__ import print_function

from ...libs.excel_wrapper import ExcelWrapper
from ...libs.ingest_utils import fix_pcr
from ...util import make_logger
from ...libs import ingest_utils
import os

logger = make_logger(__name__)


def strip_bpa_prefix(val):
    ''' Strips BPA prefix '''
    return val.split('/')[1]


def get_amplicon_data(file_name, additional_context):
    ''' Get amplion data from metadata spreadsheets '''

    field_spec = [
        ('bpa_id', 'Sample unique ID', strip_bpa_prefix),
        ('sample_extraction_id', 'Sample extraction ID', ingest_utils.get_int),
        ('sequencing_facility', 'Sequencing facility', None),
        ('target_range', 'Target Range', None),
        ('amplicon', 'Target', lambda s: s.upper().strip().lower()),
        ('i7_index', 'I7_Index_ID', None),
        ('i5_index', 'I5_Index_ID', None),
        ('index1', 'index', None),
        ('index2', 'index2', None),
        ('pcr_1_to_10', '1:10 PCR, P=pass, F=fail', fix_pcr),
        ('pcr_1_to_100', '1:100 PCR, P=pass, F=fail', fix_pcr),
        ('pcr_neat', 'neat PCR, P=pass, F=fail', fix_pcr),
        ('dilution', 'Dilution used', ingest_utils.fix_date_interval),
        ('sequencing_run_number', 'Sequencing run number', None),
        ('flow_cell_id', 'Flowcell', None),
        ('reads', '# of reads', ingest_utils.get_int),
        ('name', 'Sample name on sample sheet', None),
        ('analysis_software_version', 'AnalysisSoftwareVersion', None),
        ('comments', 'Comments', None),
    ]

    wrapper = ExcelWrapper(field_spec,
                           file_name,
                           sheet_name=None,
                           header_length=4,
                           column_name_row_index=1,
                           formatting_info=True,
                           additional_context=additional_context)

    return wrapper.get_all()


def parse_metadata(path, metadata_info):
    def is_metadata(path):
        if path.isfile() and path.ext == '.xlsx':
            return True

    logger.info('Ingesting GBR Amplicon metadata from {0}'.format(path))
    rows = []
    for metadata_file in path.walk(filter=is_metadata):
        xlsx_info = metadata_info[os.path.basename(metadata_file)]
        logger.info('Processing GBR Amplicon {0}'.format(metadata_file))
        for sample in get_amplicon_data(metadata_file, xlsx_info):
            rows.append(sample)
    return rows
