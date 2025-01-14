# VERIFY
from bpaingest.projects.cipps.files import (
    illumina_shortread_re,
    pacbio_hifi_filename_re,
)


def test_illumina_shortread():
    filenames = [
        # "355598_CIPPS_AGRF_H3GYVDSX2_AACTGAGC-CAATCAGG_L004_R2.fastq.gz",
        "415490_CIPPS_UNSW_H3MCVDSX7_CTATGAAGGA-CTTATACCTG_S5_L001_R1_001.fastq.gz"
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None


def test_pacbio_hifi_filename_re():
    filenames = [
        "417513_CIPPS_AGRF_m84073_231129_043559_s4.ccs.bam",
    ]
    for filename in filenames:
        assert pacbio_hifi_filename_re.match(filename) is not None
