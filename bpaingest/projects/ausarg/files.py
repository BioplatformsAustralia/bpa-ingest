from ...util import make_logger
import re


logger = make_logger(__name__)


ILLUMINA_FASTQ_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AusARG_
    (?P<facility_id>(UNSW))_
    (?P<flowcell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>S\d*)_
    ((?P<lane>L\d{3})_)?
    (?P<read>[R|I][1|2])_
    001
    \.fastq\.gz$
"""
illumina_fastq_re = re.compile(ILLUMINA_FASTQ_PATTERN, re.VERBOSE)

METADATA_SHEET_PATTERN = r"""
    AusARG_
    (?P<facility_id>(UNSW))_
    (?P<flowcell_id>\w{9,10})_
    metadata.xlsx
"""
metadata_sheet_re = re.compile(METADATA_SHEET_PATTERN, re.VERBOSE)

PACBIO_HIFI_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AusARG_
    (?P<facility>AGRF)_
    PacBio_
    (?P<flowcell_id>\w{8})
    (_(?P<flowcell2_id>\w{8}))?
    (_ccs_statistics\.csv
      |_final\.consensusreadset\.xml
      |\.ccs\.bam
      |\.pdf
      |\.subreads\.bam)
"""
pacbio_hifi_filename_re = re.compile(PACBIO_HIFI_PATTERN, re.VERBOSE)

PACBIO_HIFI_METADATA_SHEET_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    AusARG_
    (?P<facility>AGRF)_
    PacBio_
    (?P<flowcell_id>\w{8})
    (_(?P<flowcell2_id>\w{8}))?
    (_metadata\.xlsx)
"""
pacbio_hifi_metadata_sheet_re = re.compile(
    PACBIO_HIFI_METADATA_SHEET_PATTERN, re.VERBOSE
)
