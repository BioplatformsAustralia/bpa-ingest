from ...util import make_logger
import re


logger = make_logger(__name__)



ILLUMINA_SHORTREAD_PATTERN = r"""(?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    (PP_
    (?P<facility_id>(BRF|UNSW|AGRF))_)?
    (?P<flowcell_id>\w{5,10})_
    (?P<index>[G|A|T|C|-]{8,12}([_-][G|A|T|C|-]{8,12})?)_
    (?P<runsamplenum>S?\d*)_?
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])
    (_001|)
    \.fastq\.gz$
"""
illumina_shortread_re = re.compile(ILLUMINA_SHORTREAD_PATTERN, re.VERBOSE)


