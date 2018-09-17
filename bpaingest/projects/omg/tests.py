from .files import (
    tenxtar_filename_re,
    tenx_raw_xlsx_filename_re,
    tenxfastq_filename_re,
    exon_filename_re,
    hiseq_filename_re,
    ddrad_fastq_filename_re)


def test_tenxtar_raw_xlsx_filename_re():
    filenames = [
        '40066_OMG_UNSW_10X_HJJCWALXX_metadata.xlsx',
    ]
    for filename in filenames:
        assert(tenx_raw_xlsx_filename_re.match(filename) is not None)


def test_tenxtar_filename_re():
    filenames = [
        'HFMKJBCXY.tar',
        '170314_D00626_0270_BHCGFNBCXY.tar'
    ]
    for filename in filenames:
        assert(tenxtar_filename_re.match(filename) is not None)


def test_fastq_filename_re():
    filenames = [
        '40066_S1_L001_R1_001.fastq.gz',
        '40066_S1_L001_R2_001.fastq.gz',
        '40066_S1_L002_R1_001.fastq.gz',
        '40066_S1_L002_R2_001.fastq.gz',
    ]
    for filename in filenames:
        assert(tenxfastq_filename_re.match(filename) is not None)


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


def test_genomics_ddrad_fastq():
    filenames = [
        '52588_HHVM5BGX7_ACAGTG_L001_R1.fastq.gz',
        '52588_HHVM5BGX7_ACAGTG_L002_R1.fastq.gz',
        '52588_HHVM5BGX7_ACAGTG_L003_R1.fastq.gz',
        '52588_HHVM5BGX7_ACAGTG_L004_R1.fastq.gz',
        '52588_HHVM5BGX7_GCCAAT_L001_R1.fastq.gz',
        '52588_HHVM5BGX7_GCCAAT_L002_R1.fastq.gz',
        '52588_HHVM5BGX7_GCCAAT_L003_R1.fastq.gz',
        '52588_HHVM5BGX7_GCCAAT_L004_R1.fastq.gz',
        '52588_HHVM5BGX7_GTGAAA_L001_R1.fastq.gz',
        '52588_HHVM5BGX7_GTGAAA_L002_R1.fastq.gz',
        '52588_HHVM5BGX7_GTGAAA_L003_R1.fastq.gz',
        '52588_HHVM5BGX7_GTGAAA_L004_R1.fastq.gz',
    ]

    for filename in filenames:
        assert(ddrad_fastq_filename_re.match(filename) is not None)
