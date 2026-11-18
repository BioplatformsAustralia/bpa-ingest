# VERIFY
from bpaingest.projects.dinoflagellates.files import (
    illumina_shortread_re,
    ont_promethion_re,
)


# VERIFY


def test_illumina_shortread_filename():
    filenames = [
        "606830_DINO_SAGC_E250147280_TGCCAGATGT-TACGGTTAGA_S1_L01_R2_001.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None


def test_ont_filename():
    filenames = [
        "606831_DINO_SAGC_PBM44547_Run4_ONTPromethION_fastq_pass.tar",
        "606831_DINO_SAGC_PBM44547_Run4_ONTPromethION_pod5.tar",
        "606831_DINO_SAGC_PBM44547_Run4_ONTPromethION_report.html",
        "606831_DINO_SAGC_PBM44547_Run4_ONTPromethION_sequencing_summary.txt",
    ]
    for filename in filenames:
        assert ont_promethion_re.match(filename) is not None