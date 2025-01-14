from ...util import make_logger
import re


logger = make_logger(__name__)

METAGENOMICS_NOVASEQ_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AM_MGE_
    (?P<vendor>(AGRF|UNSW))_
    (?P<flowcell_id>\w{9})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>R[1|2])\.fastq\.gz
"""

metagenomics_novaseq_re = re.compile(METAGENOMICS_NOVASEQ_PATTERN, re.VERBOSE)


ONT_PROMETHION_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    (?P<flowcell_id>PA[DEFGKMOQW]\d{5})_
    AM_
    (?P<facility_id>(AGRF|BRF|UNSW))_
    (Run(?P<run_number>\d+)_)?
    ONTPromethION_
    ((?P<experiment_run_name>[A-Z]{4}\d{5})_)?
    (?P<archive_type>\w+)
    \.(tar|fastq.gz|blow5|html|txt|csv|tsv|json|md)
"""
ont_promethion_re = re.compile(ONT_PROMETHION_PATTERN, re.VERBOSE)
