from ...util import make_logger
import re


logger = make_logger(__name__)


ILLUMINA_SHORTREAD_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    GAP_NGS_
    (?P<facility_id>(AGRF))_
    (?P<flow_cell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])_
    001
    \.fastq\.gz$
"""
illumina_shortread_re = re.compile(ILLUMINA_SHORTREAD_PATTERN, re.VERBOSE)

ONT_MINION_PATTERN= r"""
    (?P<sample_id>\d{4,6})_
    (?P<run_id>FAK\d{5})_
    GAP_
    (?P<facility_id>(AGRF))_
    ONTMinion\.tar
"""
ont_minion_re = re.compile(ONT_MINION_PATTERN, re.VERBOSE)
