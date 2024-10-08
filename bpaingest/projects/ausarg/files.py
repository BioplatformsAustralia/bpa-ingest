from ...util import make_logger
import re


logger = make_logger(__name__)


ILLUMINA_FASTQ_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AusARG_
    (?P<facility_id>(UNSW|BRF|AGRF))_
    (?P<flowcell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    ((?P<runsamplenum>S\d*)_)?
    ((?P<lane>L\d{3})_)?
    (?P<read>[R|I][1|2])
    (_001)?
    \.fastq\.gz$
"""
illumina_fastq_re = re.compile(ILLUMINA_FASTQ_PATTERN, re.VERBOSE)

METADATA_SHEET_PATTERN = r"""
    AusARG_
    (?P<facility_id>(UNSW|BRF))_
    (?P<flowcell_id>\w{9,10})_
    metadata.xlsx
"""
metadata_sheet_re = re.compile(METADATA_SHEET_PATTERN, re.VERBOSE)

ONT_PROMETHION_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    (?P<flowcell_id>PA[DEFG]\d{5})_
    AusARG_
    (?P<facility_id>(AGRF|RamaciottiGarvan))_
    ONTPromethION_
    (?P<archive_type>\w+)
    (\.tar
      |\.html
      |\.txt)
"""
ont_promethion_re = re.compile(ONT_PROMETHION_PATTERN, re.VERBOSE)

ONT_PROMETHION_2_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AusARG_
    (?P<facility_id>(BRF))_
    (?P<flowcell_id>PA[M]\d{5})_
    ONTPromethION_
    (?P<archive_type>\w+)
    (\.tar
      |\.html
      |\.txt)
"""
ont_promethion_2_re = re.compile(ONT_PROMETHION_2_PATTERN, re.VERBOSE)

PACBIO_HIFI_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AusARG_
    (?P<facility>AGRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    (_(?P<flowcell2_id>\w{8}))?
    (_ccs_statistics\.csv
      |_final\.consensusreadset\.xml
      |\.ccs\.bam
      |([\._]HiFi_qc)?\.pdf
      |\.subreads\.bam)
"""
pacbio_hifi_filename_re = re.compile(PACBIO_HIFI_PATTERN, re.VERBOSE)

PACBIO_HIFI_2_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AusARG_
    (?P<facility>AGRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (_ccs_statistics\.csv
      |_final\.consensusreadset\.xml
      |\.ccs\.bam
      |([\._]HiFi_qc)?\.pdf
      |\.subreads\.bam)
"""
pacbio_hifi_filename_2_re = re.compile(PACBIO_HIFI_2_PATTERN, re.VERBOSE)

PACBIO_HIFI_METADATA_SHEET_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AusARG_
    (?P<facility>AGRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    (_(?P<flowcell2_id>\w{8}))?
    ([\._]metadata\.xlsx)
"""
pacbio_hifi_metadata_sheet_re = re.compile(
    PACBIO_HIFI_METADATA_SHEET_PATTERN, re.VERBOSE
)
PACBIO_HIFI_COMMON_PATTERN = r"""
    AusARG_
    (?P<facility>AGRF)_
    (?P<flowcell_id>\w{23})
    (\.pdf)
"""
pacbio_hifi_common_re = re.compile(PACBIO_HIFI_COMMON_PATTERN, re.VERBOSE)

EXON_FILENAME_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    (AusARG_BRF_)?
    (?P<flowcell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])_
    (?P<flowcellindex>\d{3})
    \.fastq\.gz$
"""
exon_filename_re = re.compile(EXON_FILENAME_PATTERN, re.VERBOSE)

ILLUMINA_HIC_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AusARG_
    (?P<facility_id>(BRF))_
    (?P<flowcell_id>\w{5,10})_
    ((?P<index>[G|A|T|C|-]*)_)?
    (?P<runsamplenum>S\d*)_
    ((?P<lane>L\d{3})_)?
    (?P<read>[R|I][1|2])_
    001
    \.fastq\.gz$
"""
illumina_hic_re = re.compile(ILLUMINA_HIC_PATTERN, re.VERBOSE)

DART_PATTERN = r"""
    (?P<file_archive_date>\d{8})_
    AusARG_
    (?P<facility_id>(BRFDArT))_
    ((?P<dataset_id>\d{4,6})_)?
    (?P<flowcell_id>\w{9})
    (_(?P<run>\d{1}))?
    \.
    tar
"""

dart_filename_re = re.compile(DART_PATTERN, re.VERBOSE)

DART_XLSX_PATTERN = r"""
    AusARG_
    (?P<facility_id>(BRFDArT))_
    (?P<dataset_id>\d{4,6})_
    (librarymetadata|samplemetadata_ingest)
    \.xlsx
"""
dart_xlsx_filename_re = re.compile(DART_XLSX_PATTERN, re.VERBOSE)

DART_MD5_PATTERN = r"""
    AusARG_
    (?P<facility_id>(BRFDArT))_
    (?P<dataset_id>\d{4,6})_
    checksums
    \.md5
"""
dart_md5_filename_re = re.compile(DART_MD5_PATTERN, re.VERBOSE)

DDRAD_FASTQ_FILENAME_PATTERN = r"""
    (?P<bpa_dataset_id>\d{4,6})_
    AusARG_
    AGRF_
    (?P<flowcell_id>\w{9})_
    (?P<index>[G|A|T|C|-]*|N)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
"""
ddrad_fastq_filename_re = re.compile(DDRAD_FASTQ_FILENAME_PATTERN, re.VERBOSE)

DDRAD_ANALYSED_TAR_PATTERN = r"""
    (?P<bpa_dataset_id>\d{4,6})_
    AusARG_
    AGRF_
    (?P<facility_contract>CAGRF\d{9})_
    (?P<flowcell_id>\w{9})_
    analysed
    \.tar
"""

ddrad_analysed_tar_re = re.compile(DDRAD_ANALYSED_TAR_PATTERN, re.VERBOSE)

DDRAD_METADATA_SHEET_PATTERN = r"""
    AusARG_
    [NGS_]
    (?P<flowcell_id>\w{9})_
    [library_]
    metadata_
    (?P<bpa_dataset_id>\d{4,6})
    \.xlsx
"""
ddrad_metadata_sheet_re = re.compile(DDRAD_METADATA_SHEET_PATTERN, re.VERBOSE)
