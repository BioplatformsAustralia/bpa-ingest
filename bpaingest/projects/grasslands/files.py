from ...util import make_logger
import re


logger = make_logger(__name__)


ILLUMINA_SHORTREAD_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    AG_NGS_
    (?P<facility_id>(AGRF))_
    (?P<flowcell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])_
    001
    \.fastq\.gz$
"""
illumina_shortread_re = re.compile(ILLUMINA_SHORTREAD_PATTERN, re.VERBOSE)

ILLUMINA__RNA_AND_PHYLO_SHORTREAD_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    (LibID)?(?P<library_id>\d{4,6})_
    (AG_)
    (?P<facility_id>(BRF|UNSW|AGRF)_)?
    (?P<flowcell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]{8,12}([_-][G|A|T|C|-]{8,12})?)_
    (?P<runsamplenum>S?\d*)_?
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])
    (_001|)
    \.fastq\.gz$
"""

illumina_shortread_rna_phylo_re = re.compile(
    ILLUMINA__RNA_AND_PHYLO_SHORTREAD_PATTERN, re.VERBOSE
)

DDRAD_FASTQ_FILENAME_PATTERN = r"""
    (?P<bpa_dataset_id>\d{4,6})_
    (AG_
    (?P<facility_id>(BRF|UNSW|AGRF))_)?
    (?P<flowcell_id>\w{9})_
    (?P<index>[G|A|T|C|-]*|N)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
"""
ddrad_fastq_filename_re = re.compile(DDRAD_FASTQ_FILENAME_PATTERN, re.VERBOSE)

DDRAD_ANALYSED_TAR_PATTERN = r"""
    (?P<bpa_dataset_id>\d{4,6})_
    AG_
    AGRF_
    (?P<facility_contract>CAGRF\d{9})_
    (?P<flowcell_id>\w{9})_
    analysed
    \.tar
"""
ddrad_analysed_tar_re = re.compile(DDRAD_ANALYSED_TAR_PATTERN, re.VERBOSE)

DDRAD_METADATA_SHEET_PATTERN = r"""
    AG_
    AGRF_
    (?P<flowcell_id>\w{9})_
    (library_)?
    Metadata
    \.xlsx
"""
ddrad_metadata_sheet_re = re.compile(DDRAD_METADATA_SHEET_PATTERN, re.VERBOSE)

PACBIO_HIFI_REVIO_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    (LibID(?P<library_id>\d{4,6})_)?
    AG_
    (?P<facility>AGRF|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (\.hifi_reads\.default\.bam
      |\.ccs\.bam
      |\.hifi_reads\.bam)
"""
pacbio_hifi_filename_revio_re = re.compile(PACBIO_HIFI_REVIO_PATTERN, re.VERBOSE)

PACBIO_HIFI_REVIO_PDF_PATTERN = r"""
    AG_
    (?P<facility>AGRF|BRF)_
    (CAGRF\d+_)?
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (\.pdf)
"""
pacbio_hifi_revio_pdf_re = re.compile(PACBIO_HIFI_REVIO_PDF_PATTERN, re.VERBOSE)


PACBIO_HIFI_REVIO_METADATA_SHEET_PATTERN = r"""
    AG_
    (?P<facility>(AGRF|BRF))_
    (CAGRF\d+\_)?
    (?P<flowcell_id>\w{23})
    ((_|\.)metadata\.xlsx)
"""

pacbio_hifi_revio_metadata_sheet_re = re.compile(
    PACBIO_HIFI_REVIO_METADATA_SHEET_PATTERN, re.VERBOSE
)

ONT_PROMETHION_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    (LibID(?P<library_id>\d{4,6})_)?
    AG_
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
    AG_
    (?P<facility_id>(AGRF|BRF|UNSW))_
    (?P<flowcell_id>P[A|B][ADEFGKMOQSW]\d{5})_
    (ONTPromethION_)
    (?P<archive_type>\w+)
    \.(html|tsv|txt|tar)
"""
ont_promethion_common_re = re.compile(ONT_PROMETHION_COMMON_PATTERN, re.VERBOSE)
