from .files import (
    illumina_shortread_re)


def test_illumina_shortread():
    filenames = [
        '79638_GAP_NGS_AGRF_HLCH5DSXX_CAATTAAC-CGAGATAT_S7_L003_R1_001.fastq.gz',
    ]
    for filename in filenames:
        assert(illumina_shortread_re.match(filename) is not None)
