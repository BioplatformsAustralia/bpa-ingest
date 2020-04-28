from .files import (
    illumina_shortread_re,
    ont_minion_re,
    ont_promethion_re,
    genomics_10x_re,
)


def test_illumina_shortread():
    filenames = [
        "79638_GAP_NGS_AGRF_HLCH5DSXX_CAATTAAC-CGAGATAT_S7_L003_R1_001.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None


def test_ont_minion_re():
    filenames = [
        "79638_FAK90583_GAP_AGRF_ONTMinion_fast5.tar",
        "79638_FAK90583_GAP_AGRF_ONTMinion_fastq_pass.tar",
        "79639_FAL87718_GAP_AGRF_ONTMinion_fast5_pass.tar",
        "79638_FAK90583_GAP_AGRF_ONTMinion_sequencing_summary.tar",
    ]
    for filename in filenames:
        assert ont_minion_re.match(filename) is not None


def test_ont_promethion_re():
    filenames = [
        "79638_PAD92744_GAP_AGRF_ONTPromethION_fast5_fail.tar",
        "79638_PAD92744_GAP_AGRF_ONTPromethION_sequencing_summary.tar",
        "79639_PAE47351_GAP_AGRF_ONTPromethION_fast5_pass.tar",
    ]
    for filename in filenames:
        assert ont_promethion_re.match(filename) is not None


def test_genomics_10x_re():
    filenames = [
        "79638_GAP_AGRF_10X_HFLC3DRXX_processed.tar",
        "79638_GAP_AGRF_HFLC3DRXX_bcl.tar",
    ]
    for filename in filenames:
        assert genomics_10x_re.match(filename) is not None
