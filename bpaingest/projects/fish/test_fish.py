# VERIFY
from bpaingest.projects.fish.files import (
    illumina_shortread_re,
)


# VERIFY


def test_illumina_shortread():
    filenames = [
        "355598_FISH_AGRF_H3GYVDSX2_AACTGAGC-CAATCAGG_L004_R2.fastq.gz",
        "45_5_FISH_AGRF_HNGM7DSX5_AATTCTTGGA-AAGTTGACAA_L004_R1.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None
