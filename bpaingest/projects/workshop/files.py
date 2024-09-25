from ...util import make_logger
import re


logger = make_logger(__name__)


PACBIO_HIFI_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    PP_
    (?P<facility>(AGRF))_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    (_ccs_statistics\.csv
      |\.ccs\.bam
      |[\._]subreads\.bam
      |\.xlsx
      |.*\.pdf)
"""
pacbio_hifi_filename_re = re.compile(PACBIO_HIFI_PATTERN, re.VERBOSE)

PACBIO_HIFI_2_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    PP_
    (?P<facility>AGRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (_ccs_statistics\.csv
      |_final\.consensusreadset\.xml
      |\.ccs\.bam
      |\.pdf
      |\.subreads\.bam)
"""
pacbio_hifi_filename_2_re = re.compile(PACBIO_HIFI_2_PATTERN, re.VERBOSE)

PACBIO_HIFI_METADATA_SHEET_PATTERN = r"""
    PP_
    (?P<facility>(AGRF))_
    ?(?P<flowcell_id>\w{8})
    ((_|\.)metadata\.xlsx)
"""
pacbio_hifi_metadata_sheet_re = re.compile(
    PACBIO_HIFI_METADATA_SHEET_PATTERN, re.VERBOSE
)
PACBIO_HIFI_COMMON_PATTERN = r"""
    PP_
    (?P<facility>AGRF)_
    (?P<flowcell_id>\w{23})
    (\.pdf)
"""
pacbio_hifi_common_re = re.compile(PACBIO_HIFI_COMMON_PATTERN, re.VERBOSE)

#FUNGI


ILLUMINA_SHORTREAD_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility_id>(AGRF))_
    (?P<flow_cell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])
    \.fastq\.gz$
"""
illumina_shortread_re = re.compile(ILLUMINA_SHORTREAD_PATTERN, re.VERBOSE)
