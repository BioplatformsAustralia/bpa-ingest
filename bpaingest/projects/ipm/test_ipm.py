# VERIFY
from bpaingest.projects.ipm.files import (
    illumina_shortread_re,
)


# VERIFY


def test_illumina_shortread_filename():
    filenames = [
        "605582_IPM_AGRF_22YMTVLT3_TCCGGACTAG-GACGAACAAT_L008_R1.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None
