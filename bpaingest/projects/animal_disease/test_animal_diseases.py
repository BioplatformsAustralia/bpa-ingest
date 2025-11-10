# VERIFY
from bpaingest.projects.plant_pathogen.files import (
    ont_promethion_re,
    ont_promethion_common_re,
)

def test_ont_promethion():
    filenames = [
        "395601_LibID397855_AD_BRF_PAQ21103_ONTPromethION_fastq_fail.tar",
        "395601_LibID397855_AD_BRF_PAQ21103_ONTPromethION_fastq_pass.tar",
        "395601_LibID397855_AD_BRF_PAQ21103_ONTPromethION_pod5_fail.tar",
        "395602_LibID397856_AD_BRF_PAQ21103_ONTPromethION_fastq_pass.tar",
    ]
    for filename in filenames:
        assert ont_promethion_re.match(filename) is not None


def test_ont_promethion_common():
    filenames = [
        "AD_BRF_PAQ21103_ONTPromethION_barcode_alignment.tsv"
        "AD_BRF_PAQ21103_ONTPromethION_report.html",
        "AD_BRF_PAQ21103_ONTPromethION_sequencing_summary.txt",
        "AD_BRF_PAW33991_ONTPromethION_pod5.tar",
    ]
    for filename in filenames:
        assert ont_promethion_common_re.match(filename) is not None
