from ...util import make_logger
import re


logger = make_logger(__name__)

ILLUMINA_SHORTREAD_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    IPM_
    (?P<facility_id>(AGRF|UNSW|BRF))_
    (?P<flow_cell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    ((?P<runsamplenum>S\d*)_)?
    ((?P<lane>L\d{3})_)?
    (?P<read>[R|I][1|2])
    (_001|)
    \.fastq\.gz$
"""
illumina_shortread_re = re.compile(ILLUMINA_SHORTREAD_PATTERN, re.VERBOSE)

ONT_PROMETHION_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    IPM_
    (?P<facility_id>(BRF))_
    (?P<flow_cell_id>P[AB][ABCDEFGKMOQW]\d{5})_
    ONTPromethION_
    (?P<archive_type>\w+)
    \.(tar|html|txt|tsv)
"""
ont_promethion_re = re.compile(ONT_PROMETHION_PATTERN, re.VERBOSE)

ONT_PROMETHION_COMMON_PATTERN = r"""
    IPM_
    (?P<facility_id>(AGRF|BRF|UNSW))_
    (?P<flow_cell_id>P[AB][ABCDEFGKMOQW]\d{5})_
    (ONTPromethION_)
    (?P<archive_type>\w+)
    \.(html|tsv|txt|tar)
"""
ont_promethion_common_re = re.compile(ONT_PROMETHION_COMMON_PATTERN, re.VERBOSE)