# VERIFY
from bpaingest.projects.plant_protein_atlas.files import (
    illumina_shortread_re,
 )


# VERIFY


def test_illumina_shortread():
    filenames = [
        "355598_PPA_AGRF_H3GYVDSX2_AACTGAGC-CAATCAGG_L004_R2.fastq.gz",
        "45_5_PPA_AGRF_HNGM7DSX5_AATTCTTGGA-AAGTTGACAA_L004_R1.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None


