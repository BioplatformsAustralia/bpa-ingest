from .files import (
    tenxtar_filename_re,
    tenx_raw_xlsx_filename_re,
    tenxfastq_filename_re,
    exon_filename_re,
    whole_genome_filename_re,
    novaseq_filename_re,
    novaseq_filename_2_re,
    hiseq_filename_re,
    ddrad_fastq_filename_re,
    ddrad_metadata_sheet_re,
    ddrad_metadata_sheet_2_re,
    pacbio_filename_re,
    ont_promethion_re,
    transcriptomics_nextseq_fastq_filename_re,
    pacbio_secondary_filename_re,
    pacbio_secondary_raw_filename_re,
    analysed_data_filename_re,
    dart_filename_re,
    dart_xlsx_filename_re,
    dart_md5_filename_re,
)


def test_tenxtar_raw_xlsx_filename_re():
    filenames = [
        "40066_OMG_UNSW_10X_HJJCWALXX_metadata.xlsx",
    ]
    for filename in filenames:
        assert tenx_raw_xlsx_filename_re.match(filename) is not None


def test_tenxtar_filename_re():
    filenames = ["HFMKJBCXY.tar", "170314_D00626_0270_BHCGFNBCXY.tar"]
    for filename in filenames:
        assert tenxtar_filename_re.match(filename) is not None


def test_fastq_filename_re():
    filenames = [
        "40066_S1_L001_R1_001.fastq.gz",
        "40066_S1_L001_R2_001.fastq.gz",
        "40066_S1_L002_R1_001.fastq.gz",
        "40066_S1_L002_R2_001.fastq.gz",
    ]
    for filename in filenames:
        assert tenxfastq_filename_re.match(filename) is not None


def test_exon():
    filenames = [
        "40109_BHLFLYBCXY_AAGGTCT_S41_L002_R1_001.fastq.gz",
        "53921_HTVLWBCX2_ACCAACT_S9_L002_R2_001.fastq.gz",
    ]

    for filename in filenames:
        assert exon_filename_re.match(filename) is not None


def test_whole_genome_re():
    filenames = [
        "55985_HVC7KDMXX_ATGCCGC_S17_L001_R2_001.fastq.gz",
        "55985_HVC7KDMXX_ATGCCGC_S17_L002_R1_001.fastq.gz",
    ]

    for filename in filenames:
        assert whole_genome_filename_re.match(filename) is not None


def test_novaseq():
    filenames = [
        "53911_ABTC50957_pool_HJKTTDSXX_CCAAGTCT-AAGGATGA_L001_R1.fastq.gz",
        "54311_NMVC37546_HT25VDSXX_TGGATCGA-GTGCGATA_L003_R2.fastq.gz",
    ]

    for filename in filenames:
        assert novaseq_filename_re.match(filename) is not None


def test_novaseq_2():
    filenames = [
        "352334_OMG_UNSW_HM7TVDRXY_CGATGT_S1_L001_R1_001.fastq.gz",
    ]

    for filename in filenames:
        assert novaseq_filename_2_re.match(filename) is not None


def test_hiseq():
    filenames = ["40066_HGTV5ALXX_N_S1_L001_R1_001.fastq.gz"]

    for filename in filenames:
        assert hiseq_filename_re.match(filename) is not None


def test_genomics_ddrad_fastq():
    filenames = [
        "52588_HHVM5BGX7_ACAGTG_L001_R1.fastq.gz",
        "52588_HHVM5BGX7_ACAGTG_L002_R1.fastq.gz",
        "52588_HHVM5BGX7_ACAGTG_L003_R1.fastq.gz",
        "52588_HHVM5BGX7_ACAGTG_L004_R1.fastq.gz",
        "52588_HHVM5BGX7_GCCAAT_L001_R1.fastq.gz",
        "52588_HHVM5BGX7_GCCAAT_L002_R1.fastq.gz",
        "52588_HHVM5BGX7_GCCAAT_L003_R1.fastq.gz",
        "52588_HHVM5BGX7_GCCAAT_L004_R1.fastq.gz",
        "52588_HHVM5BGX7_GTGAAA_L001_R1.fastq.gz",
        "52588_HHVM5BGX7_GTGAAA_L002_R1.fastq.gz",
        "52588_HHVM5BGX7_GTGAAA_L003_R1.fastq.gz",
        "52588_HHVM5BGX7_GTGAAA_L004_R1.fastq.gz",
        "52588_HHYNNBGX7_ACAGTG_L001_R1.fastq.gz",
        "52588_HHYNNBGX7_ACAGTG_L002_R1.fastq.gz",
        "52588_HHYNNBGX7_ACAGTG_L003_R1.fastq.gz",
        "52588_HHYNNBGX7_ACAGTG_L004_R1.fastq.gz",
        "52588_HHYNNBGX7_GCCAAT_L001_R1.fastq.gz",
        "52588_HHYNNBGX7_GCCAAT_L002_R1.fastq.gz",
        "52588_HHYNNBGX7_GCCAAT_L003_R1.fastq.gz",
        "52588_HHYNNBGX7_GCCAAT_L004_R1.fastq.gz",
        "52588_HHYNNBGX7_GTGAAA_L001_R1.fastq.gz",
        "52588_HHYNNBGX7_GTGAAA_L002_R1.fastq.gz",
        "52588_HHYNNBGX7_GTGAAA_L003_R1.fastq.gz",
        "52588_HHYNNBGX7_GTGAAA_L004_R1.fastq.gz",
    ]

    for filename in filenames:
        assert ddrad_fastq_filename_re.match(filename) is not None


def test_genomics_ddrad_metadata_sheet():
    filenames = [
        "OMG_NGS_AGRF_52588_HHVM5BGX7_metadata.xlsx",
        "OMG_NGS_AGRF_52588_HHYNNBGX7_metadata.xlsx",
    ]

    for filename in filenames:
        assert ddrad_metadata_sheet_re.match(filename) is not None


def test_genomics_ddrad_metadata_sheet_2():
    filenames = [
        "OMG_NGS_HTHTLDRXX_library_metadata_52628.xlsx",
    ]

    for filename in filenames:
        assert ddrad_metadata_sheet_2_re.match(filename) is not None


def test_pacbio():
    filenames = [
        "53816_UNSW_PAC_20190321_A01.tar.gz",
    ]

    for filename in filenames:
        assert pacbio_filename_re.match(filename) is not None


def test_ont_promethion_re():
    filenames = [
        "54312_PAD87066_OMG_AGRF_ONTPromethion_fast5_pass.tar",
        "55395_PAE34122_OMG_AGRF_ONTPromethion_all.tar",
    ]
    for filename in filenames:
        assert ont_promethion_re.match(filename) is not None


def test_transcriptomics_hiseq_re():
    filenames = [
        "53817_H775FAFX2_TTGACT_S1_L002_R2_001.fastq.gz",
    ]
    for filename in filenames:
        assert transcriptomics_nextseq_fastq_filename_re.match(filename) is not None


def test_pacbio_secondary_re():
    filenames = [
        "53816_Scras_dunnart_assem1.0_pb-ont-illsr_flyeassem_red-rd-scfitr2_pil2xwgs2_60chr.fasta",
    ]
    for filename in filenames:
        assert pacbio_secondary_filename_re.match(filename) is not None


def test_pacbio_raw_secondary_re():
    filenames = [
        "40065_m54196_190321_124859.subreads.bam",
    ]
    for filename in filenames:
        assert pacbio_secondary_raw_filename_re.match(filename) is not None


def test_analysed_data_re():
    filenames = [
        "352388_0007C7B720.bam",
        "352388_0007C7B720.bam.bai",
        "352388_0007C7B720.bam.md5sum",
        "352388_0007C7B720.hard-filtered.gvcf.gz",
        "352388_0007C7B720.hard-filtered.gvcf.gz.md5sum",
        "352388_0007C7B720.hard-filtered.vcf.gz",
        "352388_0007C7B720.hard-filtered.vcf.gz.md5sum",
        "352388_0007C7B720.multiqc_report.html",
        "352342_dragen_joint_greaterbilby.hard-filtered.vcf.gz",
        "352342_dragen_joint_greaterbilby.hard-filtered.vcf.gz.md5sum",
        "352342_dragen_joint_greaterbilby.vc_metrics.csv",
    ]
    for filename in filenames:
        assert analysed_data_filename_re.match(filename) is not None


def test_dart_filename_re():
    filenames = [
        "20210504_OMG_BRFDArT_HGKTCDRXY.tar",
        "20210504_OMG_BRFDArT_HH5VLDRXY.tar",
        "20210504_OMG_BRFDArT_CD58NANXX.tar",
    ]
    for filename in filenames:
        assert dart_filename_re.match(filename) is not None


def test_dart_xlsx_filename_re():
    filenames = [
        "OMG_BRFDArT_351829_samplemetadata_ingest.xlsx",
        "OMG_BRFDArT_351829_librarymetadata.xlsx",
    ]
    for filename in filenames:
        assert dart_xlsx_filename_re.match(filename) is not None


def test_dart_md5_filename_re():
    filenames = [
        "OMG_BRFDArT_351829_checksums.md5",
        "OMG_BRFDArT_52655_checksums.md5",
    ]
    for filename in filenames:
        assert dart_md5_filename_re.match(filename) is not None
