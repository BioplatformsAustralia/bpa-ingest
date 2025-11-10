from ...util import make_logger
import re


logger = make_logger(__name__)



ONT_PROMETHION_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    (LibID(?P<library_id>\d{4,6})_)?
    AD_
    (?P<facility_id>(AGRF|BRF|UNSW))_
    (?P<flowcell_id>P[A|B][ADEFGKMOQSW]\d{5})_
    (Run(?P<run_number>\d+)_)?
    ONTPromethION_
    ((?P<experiment_run_name>[A-Z]{4}\d{5})_)?
    (?P<archive_type>\w+)
    \.(tar|fastq.gz|blow5|html|txt)
"""
ont_promethion_re = re.compile(ONT_PROMETHION_PATTERN, re.VERBOSE)

ONT_PROMETHION_COMMON_PATTERN = r"""
    AD_
    (?P<facility_id>(AGRF|BRF|UNSW))_
    (?P<flowcell_id>P[A|B][ADEFGKMOQSW]\d{5})_
    (ONTPromethION_)
    (?P<archive_type>\w+)
    \.(html|tsv|txt|tar)
"""
ont_promethion_common_re = re.compile(ONT_PROMETHION_COMMON_PATTERN, re.VERBOSE)

