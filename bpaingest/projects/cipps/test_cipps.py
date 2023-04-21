# VERIFY
from bpaingest.projects.cipps.files import (
    illumina_shortread_re,
    #illumina_fastq_re,

)



def test_illumina_shortread():
    filenames = [
        "355598_CIPPS_AGRF_H3GYVDSX2_AACTGAGC-CAATCAGG_L004_R2.fastq.gz",
        "415490_CIPPS_UNSW_H3MCVDSX7_CTATGAAGGA-CTTATACCTG_S5_L001_R1_001.fastq.gz"
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None


"""
def test_fastq_filename_re():
    filenames = [
        "355638_CIPPS_UNSW_H2KN2DRXY_CTCGCTTCGG-TTGACTAGTA_S26_L001_R1_001.fastq.gz",
        "355638_CIPPS_UNSW_H2KN2DRXY_CTCGCTTCGG-TTGACTAGTA_S26_L001_R2_001.fastq.gz",
        "355638_CIPPS_UNSW_H2KN2DRXY_CTCGCTTCGG-TTGACTAGTA_S26_L002_R1_001.fastq.gz",
	    "355719_CIPPS_UNSW_HG2H3DSX2_CTCCACTAAT-AACAAGTACA_S5_L001_R2_001.fastq.gz",
        "357733_CIPPS_AGRF_HFVFMDRXY_TTGTATCAGG-TGGCCTCTGT_L001_R1.fastq.gz",
        "415490_CIPPS_UNSW_H3MCVDSX7_CTATGAAGGA-CTTATACCTG_S5_L001_R1_001.fastq.gz"
    ]
    for filename in filenames:
        assert illumina_fastq_re.match(filename) is not None
"""

