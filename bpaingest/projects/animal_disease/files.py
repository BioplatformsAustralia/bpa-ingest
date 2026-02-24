from ...util import make_logger
import re


logger = make_logger(__name__)



ONT_PROMETHION_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AD_
    (?P<facility_id>(AGRF|BRF|UNSW))_
    (?P<flowcell_id>P[A|B][ADEFGIKMOQSW]\d{5})_
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
    (?P<flowcell_id>P[A|B][ADEFGIKMOQSW]\d{5})_
    (ONTPromethION_)
    (?P<archive_type>\w+)
    \.(html|tsv|txt|tar)
"""
ont_promethion_common_re = re.compile(ONT_PROMETHION_COMMON_PATTERN, re.VERBOSE)

ILLUMINA_SHORTREAD_PATTERN = r"""(?P<library_id>\d{4,6})_
    (AD_
    (?P<facility_id>(BRF|UNSW|AGRF))_)?
    (?P<flowcell_id>\w{5,10})_
    (?P<index>[G|A|T|C|-]{8,12}([-][G|A|T|C|-]{8,12})?)_
    (?P<runsamplenum>S?\d*)_
    (?P<read>[R|I][1|2])
    (_001|)
    (\.fastq)?
    \.gz$
"""
illumina_shortread_re = re.compile(ILLUMINA_SHORTREAD_PATTERN, re.VERBOSE)

PACBIO_HIFI_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AD_
    (?P<facility>(AGRF)|C?(AGRF)\d+|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    (_ccs_statistics\.csv
      |_final\.consensusreadset\.xml
      |\.ccs\.bam
      |[\._]subreads\.bam
      |[\._]HiFi_qc\.pdf
      |\.pdf)
"""

pacbio_hifi_filename_re = re.compile(PACBIO_HIFI_PATTERN, re.VERBOSE)

PACBIO_HIFI_2_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AD_
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
      |\.subreads\.bam
      |\.hifi_reads\.fastq\.gz)

"""
pacbio_hifi_filename_2_re = re.compile(PACBIO_HIFI_2_PATTERN, re.VERBOSE)

PACBIO_HIFI_METADATA_SHEET_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AD_
    (?P<facility>(AGRF)|C?(AGRF)\d+|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    ([\._]metadata\.xlsx)
"""
pacbio_hifi_metadata_sheet_re = re.compile(
    PACBIO_HIFI_METADATA_SHEET_PATTERN, re.VERBOSE
)
PACBIO_HIFI_COMMON_PATTERN = r"""
    AD_
    (?P<facility>AGRF|BRF)_
    (?P<flowcell_id>\w{23})
    (\.pdf)
"""
pacbio_hifi_common_re = re.compile(PACBIO_HIFI_COMMON_PATTERN, re.VERBOSE)

