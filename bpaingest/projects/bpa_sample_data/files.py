from ...util import make_logger
import re


logger = make_logger(__name__)

ILLUMINA_SHORTREAD_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    BSD_
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
    BSD_
    (?P<facility_id>(UNSW|AGRF))_
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
    BSD_
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

pacbio_hifi_filename_re = re.compile(PACBIO_HIFI_PATTERN, re.VERBOSE)

PACBIO_HIFI_2_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    BSD_
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
    BSD_
    (?P<facility>(AGRF)|C?(AGRF)\d+)_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    ([\._]metadata\.xlsx)
"""
pacbio_hifi_metadata_sheet_re = re.compile(
    PACBIO_HIFI_METADATA_SHEET_PATTERN, re.VERBOSE
)
PACBIO_HIFI_COMMON_PATTERN = r"""
    BSD_
    (?P<facility>AGRF)_
    (?P<flowcell_id>\w{23})
    (\.pdf)
"""
pacbio_hifi_common_re = re.compile(PACBIO_HIFI_COMMON_PATTERN, re.VERBOSE)


METADATA_SHEET_PATTERN = r"""
    BSD_
    (?P<facility_id>(BPA))_
    (?P<flowcell_id>\w{6,10})_
    metadata.xlsx
"""
metadata_sheet_re = re.compile(METADATA_SHEET_PATTERN, re.VERBOSE)

bsd_site_image_filename_re = re.compile(
    r"""
   (?P<library_id>\d{4,6})_
    BSD_
    (?P<facility_id>(BPA))_
    (?P<flowcell_id>\w{6,10})_
    image.(jpg|png)
""",
    re.VERBOSE,
)
bsd_site_pdf_filename_re = re.compile(
    r"""
    BSD_
    (?P<facility_id>(BPA))_
    (?P<flowcell_id>\w{6,10})_map.pdf
""",
    re.VERBOSE,
)
ONT_PROMETHION_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    (?P<flowcell_id>PA[DEFG]\d{5})_
    BSD_
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
    BSD_
    (?P<facility_id>(BRF))_
    (?P<flowcell_id>PA[M]\d{5})_
    ONTPromethION_
    (?P<archive_type>\w+)
    (\.tar
      |\.html
      |\.txt)
"""
ont_promethion_2_re = re.compile(ONT_PROMETHION_2_PATTERN, re.VERBOSE)
