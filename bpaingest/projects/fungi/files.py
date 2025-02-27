from ...util import make_logger
import re


logger = make_logger(__name__)

ILLUMINA_SHORTREAD_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
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

ILLUMINA_FASTQ_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility_id>(UNSW|AGRF|BRF))_
    (?P<flowcell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    ((?P<runsamplenum>S\d*)_)?
    ((?P<lane>L\d{3})_)?
    (?P<read>[R|I][1|2])
    (_001)?
    \.fastq\.gz$
"""
illumina_fastq_re = re.compile(ILLUMINA_FASTQ_PATTERN, re.VERBOSE)

PACBIO_HIFI_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility>(AGRF)|C?(AGRF)\d+)_
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

PACBIO_HIFI_METADATA_SHEET_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility>(AGRF)|C?(AGRF)\d+)_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    ([\._]metadata\.xlsx)
"""
pacbio_hifi_metadata_sheet_re = re.compile(
    PACBIO_HIFI_METADATA_SHEET_PATTERN, re.VERBOSE
)

DDRAD_FASTQ_FILENAME_PATTERN = r"""
    (?P<bpa_dataset_id>\d{4,6})_
    (?P<flowcell_id>\w{9})_
    (?P<index>[G|A|T|C|-]*|N)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
"""
ddrad_fastq_filename_re = re.compile(DDRAD_FASTQ_FILENAME_PATTERN, re.VERBOSE)

DDRAD_METADATA_SHEET_PATTERN = r"""
    FUN_
    NGS_
    (?P<flowcell_id>\w{9})_
    library_metadata_
    (?P<bpa_dataset_id>\d{4,6})
    \.xlsx
"""
ddrad_metadata_sheet_re = re.compile(DDRAD_METADATA_SHEET_PATTERN, re.VERBOSE)

METADATA_SHEET_PATTERN = r"""
    FUN_
    (?P<facility_id>(UNSW))_
    (?P<flowcell_id>\w{9,10})_
    metadata.xlsx
"""
metadata_sheet_re = re.compile(METADATA_SHEET_PATTERN, re.VERBOSE)

GENOME_ASSEMBLY_PATTERN = r"""
    (?P<bioplatforms_secondarydata_id>\d{4,6})_
    .*
    \.
    fasta
"""
genome_assembly_filename_re = re.compile(GENOME_ASSEMBLY_PATTERN, re.VERBOSE)

ILLUMINA_HIC_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility_id>(BRF))_
    (?P<flowcell_id>\w{5,10})_
    ((?P<index>[G|A|T|C|-]*)_)?
    (?P<runsamplenum>S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])_
    001
    \.fastq\.gz$
"""
illumina_hic_re = re.compile(ILLUMINA_HIC_PATTERN, re.VERBOSE)

ONT_PROMETHION_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility_id>(BRF))_
    (?P<flow_cell_id>P[AB][ABCDEFGKMOQW]\d{5})_
    ONTPromethION_
    (?P<archive_type>\w+)
    (\.tar
      |\.html
      |\.txt)
"""
ont_promethion_re = re.compile(ONT_PROMETHION_PATTERN, re.VERBOSE)

ONT_PROMETHION_COMMON_PATTERN = r"""
    FUN_
    (?P<facility_id>(AGRF|BRF|UNSW))_
    (?P<flow_cell_id>P[AB][ABCDEFGKMOQW]\d{5})_
    (ONTPromethION_)
    (?P<archive_type>\w+)
    \.(html|tsv|txt|tar)
"""
ont_promethion_common_re = re.compile(ONT_PROMETHION_COMMON_PATTERN, re.VERBOSE)

METABOLOMICS_LCMS_FILENAME_PATTERN = r"""
    (?P<dataset_id>\d{4,6})_
    (FUN)_
    (?P<facility_id>(QMAP))_
    (?P<facility_project_code>\w+)_metabolomics_
    (?P<platform>LCMS)
    \.(tar)
"""
metabolomics_lcms_filename_re = re.compile(
    METABOLOMICS_LCMS_FILENAME_PATTERN, re.VERBOSE
)
METABOLOMICS_METADATA_SHEET_PATTERN = r"""
    FUN_
    (?P<facility>(QMAP))_
    (?P<facility_project_code>\w+)_
    (?P<dataset_id>\d{4,6})_
    metabolomics_metadata\.xlsx
"""
metabolomics_metadata_sheet_re = re.compile(
    METABOLOMICS_METADATA_SHEET_PATTERN, re.VERBOSE
)