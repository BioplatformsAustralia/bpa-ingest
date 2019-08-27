from .files import (
    illumina_shortread_re,
    ont_minion_re)


def test_illumina_shortread():
    filenames = [
        '79638_GAP_NGS_AGRF_HLCH5DSXX_CAATTAAC-CGAGATAT_S7_L003_R1_001.fastq.gz',
    ]
    for filename in filenames:
        assert(illumina_shortread_re.match(filename) is not None)


def test_ont_minion_re():
    filenames = [
        '79648_FAK90583_GAP_AGRF_ONTMinion.tar',
    ]
    for filename in filenames:
        assert(ont_minion_re.match(filename) is not None)
