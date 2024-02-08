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

PACBIO_HIFI_REVIO_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    AG_
    (?P<facility>AGRF|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (\.hifi_reads\.default\.bam
      |\.hifi_reads\.bam)
"""
pacbio_hifi_filename_revio_re = re.compile(PACBIO_HIFI_REVIO_PATTERN, re.VERBOSE)

PACBIO_HIFI_REVIO_PDF_PATTERN = r"""
    AG_
    (?P<facility>AGRF|BRF)_
    (PacBio_)?
    (?P<flowcell_id>\w{23})
    (\.pdf)
"""
pacbio_hifi_revio_pdf_re = re.compile(PACBIO_HIFI_REVIO_PDF_PATTERN, re.VERBOSE)


PACBIO_HIFI_REVIO_METADATA_SHEET_PATTERN = r"""
    AG_
    (?P<facility>(AGRF|BRF))_
    ?(?P<flowcell_id>\w{23})
    ((_|\.)metadata\.xlsx)
"""

pacbio_hifi_revio_metadata_sheet_re = re.compile(
    PACBIO_HIFI_REVIO_METADATA_SHEET_PATTERN, re.VERBOSE)
