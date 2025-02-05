# VERIFY
from bpaingest.projects.fungi.files import (
    illumina_shortread_re,
    metabolomics_metadata_sheet_re,
)


# VERIFY


def test_illumina_shortread():
    filenames = [
        "355598_FUN_AGRF_H3GYVDSX2_AACTGAGC-CAATCAGG_L004_R2.fastq.gz",
        "465935_FUN_UNSW_22NG7YLT3_CTGAGGAATA-CTTAACCACT_S14_L002_R1_001.fastq.gz",
        "45_5_FUN_AGRF_HNGM7DSX5_AATTCTTGGA-AAGTTGACAA_L004_R1.fastq.gz",

    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None

def test_metabolomics():
    filenames = [
        "FUN_QMAP_SUB01142_467794_metabolomics_metadata.xlsx",
    ]
    for filename in filenames:
        assert metabolomics_metadata_sheet_re.match(filename) is not None
