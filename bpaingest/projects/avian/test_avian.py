# VERIFY
from bpaingest.projects.avian.files import (
    pacbio_hifi_filename_2_re,
)


# VERIFY


def test_pacbio_hifi_filename():
    filenames = [
        "611513_AVIAN_AGRF_m84073_241220_104627_s4.ccs.bam",
    ]
    for filename in filenames:
        assert pacbio_hifi_filename_2_re.match(filename) is not None
