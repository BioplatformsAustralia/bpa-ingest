# VERIFY
from bpaingest.projects.tsi.files import (
    novaseq_filename_re,
    pacbio_hifi_filename_re,
    pacbio_hifi_metadata_sheet_re,
    metadata_sheet_re,
    ddrad_fastq_filename_re,
    ddrad_metadata_sheet_re,
)


# VERIFY
def test_raw_xlsx_filename_re():
    filenames = [
        "TSI_UNSW_HH2JJBGXG_metadata.xlsx",
    ]
    for filename in filenames:
        assert metadata_sheet_re.match(filename) is not None


# VERIFY
def test_novaseq():
    filenames = [
        "53911_ABTC50957_pool_HJKTTDSXX_CCAAGTCT-AAGGATGA_L001_R1.fastq.gz",
        "54311_NMVC37546_HT25VDSXX_TGGATCGA-GTGCGATA_L003_R2.fastq.gz",
    ]

    for filename in filenames:
        assert novaseq_filename_re.match(filename) is not None


def test_pacbio_hifi():
    filenames = [
        "355356_TSI_AGRF_PacBio_DA052899_ccs_statistics.csv"
        "355356_TSI_AGRF_PacBio_DA052899_final.consensusreadset.xml"
        "355356_TSI_AGRF_PacBio_DA052899.ccs.bam"
        "355356_TSI_AGRF_PacBio_DA052899.subreads.bam"
        "355356_TSI_AGRF_PacBio_DA052899.pdf"
    ]

    for filename in filenames:
        assert pacbio_hifi_filename_re.match(filename) is not None


def test_pacbio_hifi_metadata_sheet():
    filenames = ["355356_TSI_AGRF_PacBio_DA052899_metadata.xlsx"]

    for filename in filenames:
        assert pacbio_hifi_metadata_sheet_re.match(filename) is not None


def test_tsi_ddrad_fastq():
    filenames = [
        "358724_HTW2NDRXX_ACAGTG_L001_R1.fastq.gz",
        "358724_HTW2NDRXX_ACAGTG_L002_R1.fastq.gz",
        "358724_HTW2NDRXX_GCCAAT_L001_R1.fastq.gz",
        "358724_HTW2NDRXX_GCCAAT_L002_R1.fastq.gz",
        "358724_HTW2NDRXX_GTGAAA_L001_R1.fastq.gz",
        "358724_HTW2NDRXX_GTGAAA_L002_R1.fastq.gz",
    ]

    for filename in filenames:
        assert ddrad_fastq_filename_re.match(filename) is not None


def test_tsi_ddrad_metadata_sheet():
    filenames = [
        "TSI_NGS_HTW2NDRXX_library_metadata_358724.xlsx",
    ]

    for filename in filenames:
        assert ddrad_metadata_sheet_re.match(filename) is not None
