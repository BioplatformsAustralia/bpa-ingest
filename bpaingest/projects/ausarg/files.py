from ...util import make_logger
import re


logger = make_logger(__name__)


ILLUMINA_FASTQ_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    AusARG_
    (?P<facility_id>(UNSW))_
    (?P<flowcell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])_
    001
    \.fastq\.gz$
"""
illumina_fastq_re = re.compile(ILLUMINA_FASTQ_PATTERN, re.VERBOSE)

METADATA_SHEET_PATTERN = r"""
    AusARG_
    (?P<facility_id>(UNSW))_
    (?P<flowcell_id>\w{9,10})_
    metadata.xlsx
"""
metadata_sheet_re = re.compile(METADATA_SHEET_PATTERN, re.VERBOSE)
