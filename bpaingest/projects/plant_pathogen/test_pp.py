# VERIFY
from bpaingest.projects.plant_pathogen.files import (
    illumina_shortread_re,
 )


# VERIFY


def test_illumina_shortread():
    filenames = [
        "394529_LibID396779_PP_AGRF_KTMDN_GCCACAGG-CATGCCAT_L001_R1.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None


