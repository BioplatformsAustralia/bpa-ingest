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
