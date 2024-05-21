# VERIFY
from bpaingest.projects.collaborations.files import (
    metagenomics_novaseq_re,
)



def test_metagenomics_novaseq():
    filenames = [
        "138515_AM_MGE_AGRF_22KM7FLT3_CACAGCGGTC-ATTCCTATTG_L007_R1.fastq.gz",
    ]
    for filename in filenames:
        assert metagenomics_novaseq_re.match(filename) is not None




