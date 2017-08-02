from .files import (
    tenxtar_filename_re,
    exon_filename_re,
    hiseq_filename_re)


# placeholder until AGRF confirm filename structure
def test_tenxtar_filename_re():
    filenames = [
        'HFMKJBCXY.tar',
        '170314_D00626_0270_BHCGFNBCXY.tar'
    ]
    for filename in filenames:
        assert(tenxtar_filename_re.match(filename) is not None)


def test_exon():
    filenames = [
        '40109_BHLFLYBCXY_AAGGTCT_S41_L002_R1_001.fastq.gz',
    ]

    for filename in filenames:
        assert(exon_filename_re.match(filename) is not None)


def test_hiseq():
    filenames = [
        '40066_HGTV5ALXX_N_S1_L001_R1_001.fastq.gz'
    ]

    for filename in filenames:
        assert(hiseq_filename_re.match(filename) is not None)
