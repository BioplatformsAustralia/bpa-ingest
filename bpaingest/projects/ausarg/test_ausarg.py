from bpaingest.projects.ausarg.files import illumina_fastq_re, metadata_sheet_re


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
        "350734_AusARG_UNSW_HGGKCBGXH_ATGGCGCC-AGGCCGTG_S6_R1_001.fastq.gz"
    ]
    for filename in filenames:
        assert illumina_fastq_re.match(filename) is not None
