from .files import (
    tenxtar_filename_re,
    tenx_raw_xlsx_filename_re,
    tenxfastq_filename_re,
    exon_filename_re,
    novaseq_filename_re,
    hiseq_filename_re,
    ddrad_fastq_filename_re,
    ddrad_metadata_sheet_re,
    pacbio_filename_re,
    ont_promethion_re)


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
        '53921_HTVLWBCX2_ACCAACT_S9_L002_R2_001.fastq.gz',
    ]

    for filename in filenames:
        assert(exon_filename_re.match(filename) is not None)


def test_novaseq():
    filenames = [
        '53911_ABTC50957_pool_HJKTTDSXX_CCAAGTCT-AAGGATGA_L001_R1.fastq.gz',
        '54311_NMVC37546_HT25VDSXX_TGGATCGA-GTGCGATA_L003_R2.fastq.gz',
    ]

    for filename in filenames:
        assert(novaseq_filename_re.match(filename) is not None)


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
        '52588_HHYNNBGX7_ACAGTG_L001_R1.fastq.gz',
        '52588_HHYNNBGX7_ACAGTG_L002_R1.fastq.gz',
        '52588_HHYNNBGX7_ACAGTG_L003_R1.fastq.gz',
        '52588_HHYNNBGX7_ACAGTG_L004_R1.fastq.gz',
        '52588_HHYNNBGX7_GCCAAT_L001_R1.fastq.gz',
        '52588_HHYNNBGX7_GCCAAT_L002_R1.fastq.gz',
        '52588_HHYNNBGX7_GCCAAT_L003_R1.fastq.gz',
        '52588_HHYNNBGX7_GCCAAT_L004_R1.fastq.gz',
        '52588_HHYNNBGX7_GTGAAA_L001_R1.fastq.gz',
        '52588_HHYNNBGX7_GTGAAA_L002_R1.fastq.gz',
        '52588_HHYNNBGX7_GTGAAA_L003_R1.fastq.gz',
        '52588_HHYNNBGX7_GTGAAA_L004_R1.fastq.gz',
    ]

    for filename in filenames:
        assert(ddrad_fastq_filename_re.match(filename) is not None)


def test_genomics_ddrad_metadata_sheet():
    filenames = [
        'OMG_NGS_AGRF_52588_HHVM5BGX7_metadata.xlsx',
        'OMG_NGS_AGRF_52588_HHYNNBGX7_metadata.xlsx',
    ]

    for filename in filenames:
        assert(ddrad_metadata_sheet_re.match(filename) is not None)


def test_pacbio():
    filenames = [
        '53816_UNSW_PAC_20190321_A01.tar.gz',
    ]

    for filename in filenames:
        assert(pacbio_filename_re.match(filename) is not None)

def test_ont_promethion_re():
    filenames = [
        '79638_PAD92744_GAP_AGRF_ONTPromethION_fast5_fail.tar',
        '79638_PAD92744_GAP_AGRF_ONTPromethION_sequencing_summary.tar',
    ]
    for filename in filenames:
        assert(ont_promethion_re.match(filename) is not None)
