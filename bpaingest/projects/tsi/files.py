from ...util import make_logger
import re


logger = make_logger(__name__)

ILLUMINA_SHORTREAD_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    TSI_
    (?P<facility_id>(AGRF))_
    (?P<flow_cell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])
    \.fastq\.gz$
"""
illumina_shortread_re = re.compile(ILLUMINA_SHORTREAD_PATTERN, re.VERBOSE)

ILLUMINA_FASTQ_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    TSI_
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
    TSI_
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

PACBIO_HIFI_2_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    TSI_
    (?P<facility>AGRF|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (_ccs_statistics\.csv
      |_final\.consensusreadset\.xml
      |\.ccs\.bam
      |([\._]HiFi_qc)?\.pdf
      |\.subreads\.bam
      |\.hifi_reads\.default\.bam
      |\.hifi_reads\.bc\d{4}\.bam
      |\.hifi_reads\.bam)
"""
pacbio_hifi_filename_2_re = re.compile(PACBIO_HIFI_2_PATTERN, re.VERBOSE)

PACBIO_HIFI_METADATA_SHEET_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    TSI_
    (?P<facility>(AGRF)|C?(AGRF)\d+)_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    ([\._]metadata\.xlsx)
"""
pacbio_hifi_metadata_sheet_re = re.compile(
    PACBIO_HIFI_METADATA_SHEET_PATTERN, re.VERBOSE
)
PACBIO_HIFI_COMMON_PATTERN = r"""
    TSI_
    (?P<facility>AGRF|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (\.pdf)
"""
pacbio_hifi_common_re = re.compile(PACBIO_HIFI_COMMON_PATTERN, re.VERBOSE)

DDRAD_FASTQ_FILENAME_PATTERN = r"""
    (?P<bpa_dataset_id>\d{4,6})_
    (TSI_
    (?P<facility_id>(BRF|UNSW|AGRF))_)?
    (?P<flowcell_id>\w{9})_
    (?P<index>[G|A|T|C|-]*|N)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
"""
ddrad_fastq_filename_re = re.compile(DDRAD_FASTQ_FILENAME_PATTERN, re.VERBOSE)

DDRAD_ANALYSED_TAR_PATTERN = r"""
    (?P<bpa_dataset_id>\d{4,6})_
    TSI_
    AGRF_
    (?P<facility_contract>CAGRF\d{9})_
    (?P<flowcell_id>\w{9})_
    analysed
    \.tar
"""
ddrad_analysed_tar_re = re.compile(DDRAD_ANALYSED_TAR_PATTERN, re.VERBOSE)

DDRAD_METADATA_SHEET_PATTERN = r"""
    TSI_
    NGS_
    (?P<flowcell_id>\w{9})_
    (library_)?
    metadata_
    (?P<bpa_dataset_id>\d{4,6})
    \.xlsx
"""
ddrad_metadata_sheet_re = re.compile(DDRAD_METADATA_SHEET_PATTERN, re.VERBOSE)

DDRAD_XLSX_PATTERN = r"""
    TSI_
    (?P<flowcell_id>\w{9})_
    (?P<bpa_dataset_id>\d{4,6})_
    (librarymetadata|samplemetadata_ingest)
    \.xlsx
"""
ddrad_xlsx_filename_re = re.compile(DDRAD_XLSX_PATTERN, re.VERBOSE)

METADATA_SHEET_PATTERN = r"""
    TSI_
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
    TSI_
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
DART_PATTERN = r"""
    (?P<file_archive_date>\d{8})_
    TSI_
    (?P<facility_id>(BRFDArT))_
    (?P<flowcell_id>\w{5,9})
    (_(?P<run>\d{1}))?
    \.
    tar
"""
dart_filename_re = re.compile(DART_PATTERN, re.VERBOSE)

DART_XLSX_PATTERN = r"""
    TSI_
    (?P<facility_id>(BRFDArT))_
    (?P<dataset_id>\d{4,6})_
    (librarymetadata|samplemetadata_ingest)
    \.xlsx
"""
dart_xlsx_filename_re = re.compile(DART_XLSX_PATTERN, re.VERBOSE)

DART_MD5_PATTERN = r"""
    TSI_
    (?P<facility_id>(BRFDArT))_
    (?P<dataset_id>\d{4,6})_
    checksums
    \.md5
"""
dart_md5_filename_re = re.compile(DART_MD5_PATTERN, re.VERBOSE)

ONT_PROMETHION_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    TSI_
    (?P<facility_id>(BRF))_
    (?P<flow_cell_id>P[AB][ABCDEFGKMOQW]\d{5})_
    ONTPromethION_
    (?P<archive_type>\w+)
    \.(tar|html|txt|tsv)
"""
ont_promethion_re = re.compile(ONT_PROMETHION_PATTERN, re.VERBOSE)

ONT_PROMETHION_COMMON_PATTERN = r"""
    TSI_
    (?P<facility_id>(AGRF|BRF|UNSW))_
    (?P<flow_cell_id>P[AB][ABCDEFGKMOQW]\d{5})_
    (ONTPromethION_)
    (?P<archive_type>\w+)
    \.(html|tsv|txt|tar)
"""
ont_promethion_common_re = re.compile(ONT_PROMETHION_COMMON_PATTERN, re.VERBOSE)
