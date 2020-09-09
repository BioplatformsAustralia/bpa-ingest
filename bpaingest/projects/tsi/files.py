from ...util import make_logger
import re


logger = make_logger(__name__)

# VERIFY
NOVASEQ_FILENAME_PATTERN = r"""
    (?P<bpa_library_id>\d{4,6})_
    (?P<voucher_id>\w+)_
    (pool_)?
    (?P<flow_cell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])
    \.fastq\.gz
"""
novaseq_filename_re = re.compile(NOVASEQ_FILENAME_PATTERN, re.VERBOSE)

# VERIFY
PACBIO_HIFI_PATTERN = r"""
    (?P<bpa_library_id>\d{4,6})_
    (?P<facility>UNSW)_
    PAC_
    (?P<run_date>\d{8})_
    (?P<run_code>.{3})
    \.tar\.gz
"""
pacbio_hifi_filename_re = re.compile(PACBIO_HIFI_PATTERN, re.VERBOSE)

# VERIFY
METADATA_SHEET_PATTERN = r"""
    TSI_
    (?P<facility_id>(UNSW))_
    (?P<flowcell_id>\w{9,10})_
    metadata.xlsx
"""
metadata_sheet_re = re.compile(METADATA_SHEET_PATTERN, re.VERBOSE)
