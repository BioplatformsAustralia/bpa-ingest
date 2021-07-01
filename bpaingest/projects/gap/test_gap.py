from .files import (
    illumina_shortread_re,
    ont_minion_re,
    ont_promethion_re,
    ont_promethion_re_2,
    genomics_10x_re,
    illumina_shortread_rna_phylo_re,
    ddrad_fastq_filename_re,
    ddrad_metadata_sheet_re,
)


def test_illumina_shortread():
    filenames = [
        "79638_GAP_NGS_AGRF_HLCH5DSXX_CAATTAAC-CGAGATAT_S7_L003_R1_001.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None


def test_illumina_rna_and_phylo_shortread():
    filenames = [
        "79697_LibID81791_H7LV7AFX2_TAGTGGCA-AGCAGATG_L001_R1.fastq.gz",
        "79697_LibID81791_H7LV7AFX2_TAGTGGCA-AGCAGATG_S1_L001_R1.fastq.gz",
        "79638_LibID81644_HLCH5DSXX_CAATTAAC-CGAGATAT_S7_L003_R1_001.fastq.gz",
        "376315_LibID380534_GAP_BRF_AH5TJYDRXY_AAGAACCG_CTAGAATT_S7_L002_R2_001.fastq.gz",
        "376316_LibID380531_GAP_BRF_AH5TJYDRXY_AAGAACCG_GCATTCGG_S4_L001_R1_001.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_rna_phylo_re.match(filename) is not None


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


def test_ont_promethion_re_2():
    filenames = [
        "376315_GAP_BRF_PAG02700_ONTPromethION_report.tar",
        "376315_GAP_BRF_PAG02700_ONTPromethION_sequencing_summary.tar",
        "376315_GAP_BRF_PAG03810_Run2_ONTPromethION_fast5_fail.tar",
        "376316_GAP_BRF_PAF32853_ONTPromethION_fast5_pass.tar",
    ]
    for filename in filenames:
        assert ont_promethion_re_2.match(filename) is not None


def test_genomics_10x_re():
    filenames = [
        "79638_GAP_AGRF_10X_HFLC3DRXX_processed.tar",
        "79638_GAP_AGRF_HFLC3DRXX_bcl.tar",
    ]
    for filename in filenames:
        assert genomics_10x_re.match(filename) is not None


def test_genomics_ddrad_fastq():
    filenames = [
        "83666_HY7LHDRXX_ACAGTG_L001_R1.fastq.gz",
        "83666_HY7LHDRXX_ACAGTG_L002_R1.fastq.gz",
        "83666_HY7LHDRXX_CTTGTA_L001_R1.fastq.gz",
        "83666_HY7LHDRXX_CTTGTA_L002_R1.fastq.gz",
        "83666_HY7LHDRXX_GCCAAT_L001_R1.fastq.gz",
        "83666_HY7LHDRXX_GCCAAT_L002_R1.fastq.gz",
        "83666_HY7LHDRXX_GTGAAA_L001_R1.fastq.gz",
        "83666_HY7LHDRXX_GTGAAA_L002_R1.fastq.gz",
    ]

    for filename in filenames:
        assert ddrad_fastq_filename_re.match(filename) is not None


def test_genomics_ddrad_metadata_sheet():
    filenames = [
        "GAP_NGS_HY7LHDRXX_library_metadata_83666.xlsx",
    ]

    for filename in filenames:
        assert ddrad_metadata_sheet_re.match(filename) is not None
