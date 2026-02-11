from ...util import make_logger
import re


logger = make_logger(__name__)

PACBIO_HIFI_REVIO_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FOR_
    (?P<facility>AGRF|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (\.hifi_reads\.default\.bam
      |\.hifi_reads\.bc\d{4}\.bam
      |\.hifi_reads\.bam)
"""
pacbio_hifi_filename_revio_re = re.compile(PACBIO_HIFI_REVIO_PATTERN, re.VERBOSE)

PACBIO_HIFI_REVIO_PDF_PATTERN = r"""
    FOR_
    (?P<facility>AGRF|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (\.pdf)
"""
pacbio_hifi_revio_pdf_re = re.compile(PACBIO_HIFI_REVIO_PDF_PATTERN, re.VERBOSE)

PACBIO_HIFI_REVIO_METADATA_SHEET_PATTERN = r"""
    FOR_
    (?P<facility>(AGRF|BRF))_
    ?(?P<flowcell_id>\w{23})
    ((_|\.)metadata\.xlsx)
"""

pacbio_hifi_revio_metadata_sheet_re = re.compile(
    PACBIO_HIFI_REVIO_METADATA_SHEET_PATTERN, re.VERBOSE
)

ILLUMINA_SHORTREAD_PATTERN = r"""(?P<library_id>\d{4,6})_
    (FOR_
    (?P<facility_id>(BRF|UNSW|AGRF))_)?
    (?P<flowcell_id>\w{5,10})_
    (?P<index>[G|A|T|C|-]{8,12}([-][G|A|T|C|-]{8,12})?)_
    (?P<runsamplenum>S?\d*)_?
    ((?P<lane>L\d{3})_)?
    (?P<read>[R|I][1|2])
    (_001|)
    (\.fastq)?
    \.gz$
"""
illumina_shortread_re = re.compile(ILLUMINA_SHORTREAD_PATTERN, re.VERBOSE)
