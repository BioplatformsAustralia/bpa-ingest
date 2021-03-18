from bpaingest.projects.ausarg.files import (
    illumina_fastq_re,
    metadata_sheet_re,
    ont_promethion_re,
    pacbio_hifi_filename_re,
    pacbio_hifi_metadata_sheet_re,
)


def test_raw_xlsx_filename_re():
    filenames = [
        "AusARG_UNSW_HH2JJBGXG_metadata.xlsx",
    ]
    for filename in filenames:
        assert metadata_sheet_re.match(filename) is not None


def test_fastq_filename_re():
    filenames = [
        "350728_AusARG_UNSW_HH2JJBGXG_CCTGAACT-CCAACAGA_S9_L001_R1_001.fastq.gz",
        "350733_AusARG_UNSW_HGGKCBGXH_ATATGCAT-CCAGGCAC_S5_R2_001.fastq.gz",
        "350734_AusARG_UNSW_HGGKCBGXH_ATGGCGCC-AGGCCGTG_S6_R1_001.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_fastq_re.match(filename) is not None


def test_ont_promethion_re():
    filenames = [
        "350767_PAG18256_AusARG_RamaciottiGarvan_ONTPromethION_fast5_fail.tar",
        "350767_PAG18256_AusARG_RamaciottiGarvan_ONTPromethION_fast5_pass.tar",
        "350767_PAG18256_AusARG_RamaciottiGarvan_ONTPromethION_fastq_fail.tar",
        "350767_PAG18256_AusARG_RamaciottiGarvan_ONTPromethION_fastq_pass.tar",
        "350767_PAG18256_AusARG_RamaciottiGarvan_ONTPromethION_plots.tar",
        "350767_PAG18256_AusARG_RamaciottiGarvan_ONTPromethION_sequencing_summary.tar",
    ]
    for filename in filenames:
        assert ont_promethion_re.match(filename) is not None


def test_pacbio_hifi():
    filenames = [
        "355356_AusARG_AGRF_PacBio_DA052899_ccs_statistics.csv",
        "355356_AusARG_AGRF_PacBio_DA052899_final.consensusreadset.xml",
        "355356_AusARG_AGRF_PacBio_DA052899.ccs.bam",
        "355356_AusARG_AGRF_PacBio_DA052899.subreads.bam",
        "355356_AusARG_AGRF_PacBio_DA052899.pdf",
        "350719_AusARG_AGRF_PacBio_DA052894_DA052873.ccs.bam",
        "350719_AusARG_AGRF_PacBio_DA052894_DA052873_ccs_statistics.csv",
        "350719_AusARG_AGRF_PacBio_DA052894_DA052873_final.consensusreadset.xml",
        "349741_AusARG_AGRF_PacBio_DA043673.pdf",
        "350719_AusARG_AGRF_PacBio_DA052873.pdf",
        "350719_AusARG_AGRF_PacBio_DA052894.pdf",
        "349741_AusARG_AGRF_PacBio_DA043669.pdf",
    ]

    for filename in filenames:
        assert pacbio_hifi_filename_re.match(filename) is not None


def test_pacbio_hifi_metadata_sheet():
    filenames = [
        "355356_AusARG_AGRF_PacBio_DA052899_metadata.xlsx",
        "350719_AusARG_AGRF_PacBio_DA052894_DA052873_metadata.xlsx",
    ]

    for filename in filenames:
        assert pacbio_hifi_metadata_sheet_re.match(filename) is not None
