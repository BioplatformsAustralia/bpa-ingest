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

PACBIO_HIFI_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    CANETOAD_
    (?P<facility>AGRF|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (_ccs_statistics\.csv
      |_final\.consensusreadset\.xml
      |\.ccs\.bam
      |\.pdf
      |\.hifi_reads\.default\.bam
      |\.hifi_reads\.bc\d{4}\.bam
      |\.hifi_reads\.bam
      |\.subreads\.bam)

"""
pacbio_hifi_filename_re = re.compile(PACBIO_HIFI_PATTERN, re.VERBOSE)

PACBIO_HIFI_METADATA_SHEET_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    CANETOAD_
    (?P<facility>(AGRF)|C?(AGRF)\d+|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    ([\._]metadata\.xlsx)
"""
pacbio_hifi_metadata_sheet_re = re.compile(
    PACBIO_HIFI_METADATA_SHEET_PATTERN, re.VERBOSE
)
PACBIO_HIFI_COMMON_PATTERN = r"""
    CANETOAD_
    (?P<facility>AGRF|BRF)_
    (?P<flowcell_id>\w{23})
    (\.pdf)
"""
pacbio_hifi_common_re = re.compile(PACBIO_HIFI_COMMON_PATTERN, re.VERBOSE)

