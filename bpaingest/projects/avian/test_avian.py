# VERIFY
from bpaingest.projects.avian.files import (
    pacbio_hifi_filename_2_re,
    illumina_shortread_re,
    illumina_shortread_common_re,
)


# VERIFY


def test_pacbio_hifi_filename():
    filenames = [
        "611513_AVIAN_AGRF_m84073_241220_104627_s4.ccs.bam",
    ]
    for filename in filenames:
        assert pacbio_hifi_filename_2_re.match(filename) is not None

def test_illumina_shortread_filename():
    filenames = [
        "614100_AVIAN_BRF_23MWLYLT4_N502_C21_L001_L002_R1_001.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None

def test_illumina_shortread_common_filename():
    filenames = [
        "AVIAN_BRF_613567_23MWLYLT4_additional_info.pdf",
        "AVIAN_BRF_613568_23MWLYLT4_Sequencing-and-demultiplexing-information.pdf"
    ]
    for filename in filenames:
        assert illumina_shortread_common_re.match(filename) is not None