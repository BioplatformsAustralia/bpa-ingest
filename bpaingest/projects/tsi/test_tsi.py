# VERIFY
from bpaingest.projects.tsi.files import (
    illumina_shortread_re,
    illumina_fastq_re,
    # novaseq_filename_re,
    pacbio_hifi_filename_re,
    pacbio_hifi_filename_2_re,
    pacbio_hifi_metadata_sheet_re,
    metadata_sheet_re,
    ddrad_fastq_filename_re,
    ddrad_metadata_sheet_re,
    ddrad_analysed_tar_re,
    genome_assembly_filename_re,
    illumina_hic_re,
)


# VERIFY
def test_raw_xlsx_filename_re():
    filenames = [
        "TSI_UNSW_HH2JJBGXG_metadata.xlsx",
    ]
    for filename in filenames:
        assert metadata_sheet_re.match(filename) is not None


# VERIFY
"""
def test_novaseq():
    filenames = [
        "53911_ABTC50957_pool_HJKTTDSXX_CCAAGTCT-AAGGATGA_L001_R1.fastq.gz",
        "54311_NMVC37546_HT25VDSXX_TGGATCGA-GTGCGATA_L003_R2.fastq.gz",
    ]

    for filename in filenames:
        assert novaseq_filename_re.match(filename) is not None
"""

def test_illumina_shortread():
    filenames = [
        "355598_TSI_AGRF_H3GYVDSX2_AACTGAGC-CAATCAGG_L004_R2.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None


def test_fastq_filename_re():
    filenames = [
        "355638_TSI_UNSW_H2KN2DRXY_CTCGCTTCGG-TTGACTAGTA_S26_L001_R1_001.fastq.gz",
        "355638_TSI_UNSW_H2KN2DRXY_CTCGCTTCGG-TTGACTAGTA_S26_L001_R2_001.fastq.gz",
        "355638_TSI_UNSW_H2KN2DRXY_CTCGCTTCGG-TTGACTAGTA_S26_L002_R1_001.fastq.gz",
	"355719_TSI_UNSW_HG2H3DSX2_CTCCACTAAT-AACAAGTACA_S5_L001_R2_001.fastq.gz",
        "357733_TSI_AGRF_HFVFMDRXY_TTGTATCAGG-TGGCCTCTGT_L001_R1.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_fastq_re.match(filename) is not None


def test_pacbio_hifi():
    filenames = [
        "355356_TSI_AGRF_PacBio_DA052899_ccs_statistics.csv",
        "355356_TSI_AGRF_PacBio_DA052899_final.consensusreadset.xml",
        "355356_TSI_AGRF_PacBio_DA052899.ccs.bam",
        "355356_TSI_AGRF_PacBio_DA052899.subreads.bam",
        "355356_TSI_AGRF_PacBio_DA052899.pdf",
        "357368_TSI_AGRF_DA060252.ccs.bam",
        "357368_TSI_AGRF_DA060252.subreads.bam",
        "357368_TSI_AGRF_DA060252_HiFi_qc.pdf",
        "357368_TSI_AGRF_DA060252_ccs_statistics.csv",
        "357368_TSI_AGRF_DA060252_final.consensusreadset.xml",
	    "357368_TSI_CAGRF20114490_DA060254_subreads.bam",
    ]

    for filename in filenames:
        assert pacbio_hifi_filename_re.match(filename) is not None

def test_pacbio_hifi_2():
    filenames = [
        "460864_TSI_AGRF_m84073_230601_030428_s2.pdf",
        "460864_TSI_AGRF_m84073_230601_030428_s2.ccs.bam",
    ]

    for filename in filenames:
        assert pacbio_hifi_filename_2_re.match(filename) is not None
def test_pacbio_hifi_metadata_sheet():
    filenames = [
        "355356_TSI_AGRF_PacBio_DA052899_metadata.xlsx",
        "357368_TSI_AGRF_DA060252_metadata.xlsx",
	"357368_TSI_CAGRF20114490_DA060254_metadata.xlsx",
    ]

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

def test_tsi_ddrad_analysed_tar():
    filenames = [
        "358804_TSI_AGRF_CAGRF220811739_HVLNTDRX2_analysed.tar",
    ]

    for filename in filenames:
        assert ddrad_analysed_tar_re.match(filename) is not None

def test_genome_assembly_filename_re():
    filenames = [
        "359774_Galaxy63-Purge_overlaps_on_data_23_split.fasta",
    ]
    for filename in filenames:
        assert genome_assembly_filename_re.match(filename) is not None


def test_illumina_hic():
    filenames = [
        "350764_TSI_BRF_DD2M2_TGACCA_S2_L001_R1_001.fastq.gz",
        "350769_TSI_BRF_DD2M2_CAGATC_S5_L001_R1_001.fastq.gz",
        "350821_TSI_BRF_DD2M2_CGATGT_S1_L001_R1_001.fastq.gz",
        "350752_TSI_BRF_HCN7WDRXY_S4_L001_R1_001.fastq.gz",
        "350752_TSI_BRF_HCN7WDRXY_S4_L001_R2_001.fastq.gz",
        "350752_TSI_BRF_HCN7WDRXY_S4_L002_R1_001.fastq.gz",
        "408042_TSI_BRF_HG5YLDMXY_S2_L001_R1_001.fastq.gz",
        "408044_TSI_BRF_HG5YLDMXY_S4_L002_R1_001.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_hic_re.match(filename) is not None

