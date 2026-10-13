# VERIFY
from bpaingest.projects.ipm.files import (
    illumina_shortread_re,
    ont_promethion_re,
    ont_promethion_common_re,
)


# VERIFY


def test_illumina_shortread_filename():
    filenames = [
        "605582_AVID_AGRF_22YMTVLT3_TCCGGACTAG-GACGAACAAT_L008_R1.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None

def test_ont_promethion_filename():
    filenames = [
        "669271_BTI_BRF_PBO70267_ONTPromethION_pod5_pass.tar",
        "669271_BTI_BRF_PBO70267_ONTPromethION_bam_pass.tar",
    ]
    for filename in filenames:
        assert ont_promethion_re.match(filename) is not None

def test_ont_promethion_common_filename():
    filenames = [
        "BTI_BRF_PBO70267_ONTPromethION_barcode_alignment.tsv",
    ]
    for filename in filenames:
        assert ont_promethion_common_re.match(filename) is not None