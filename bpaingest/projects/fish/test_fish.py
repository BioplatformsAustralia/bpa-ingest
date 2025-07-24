# VERIFY
from bpaingest.projects.fish.files import (
    illumina_shortread_re,
    pacbio_hifi_filename_2_re,
)


def test_illumina_shortread():
    filenames = [
        "616598_FISH_UNSW_233HYTLT3_GATCGTCGCG-CTGGATATGT_S10_L008_R1_001.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None


def test_pacbio_hifi():
    filenames = [
        "616628_FISH_BRF_m84118_250714_235739_s1.hifi_reads.bc2069.bam",
    ]
    for filename in filenames:
        assert pacbio_hifi_filename_2_re.match(filename) is not None