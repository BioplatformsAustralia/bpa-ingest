# VERIFY
from bpaingest.projects.workshop.files import (
    pacbio_hifi_filename_re,
    pacbio_hifi_filename_2_re,
    pacbio_hifi_metadata_sheet_re,
    pacbio_hifi_common_re,
    illumina_shortread_re,
)



# VERIFY


def test_pacbio_hifi():
    filenames = [
        "394855_PP_AGRF_DA218825.subreads.bam",
        "394855_PP_AGRF_DA218825.ccs.bam",
        "394855_PP_AGRF_DA218825.pdf",
        "394855_PP_AGRF_DA218825_ccs_statistics.csv",
    ]

    for filename in filenames:
        assert pacbio_hifi_filename_re.match(filename) is not None


def test_pacbio_hifi2():
    filenames = [
        "394568_PP_AGRF_m84073_230616_014416_s1.ccs.bam",
        "394568_PP_AGRF_m84073_230616_014416_s1.pdf",
    ]

    for filename in filenames:
        assert pacbio_hifi_filename_2_re.match(filename) is not None


def test_pacbio_hifi_metadata_sheet():
    filenames = [
        "PP_AGRF_DA218825_metadata.xlsx",
    ]
    for filename in filenames:
        assert pacbio_hifi_metadata_sheet_re.match(filename) is not None


def test_pacbio_hifi_common():
    filenames = ["PP_AGRF_m84073_230616_014416_s1.pdf"]
    for filename in filenames:
        assert pacbio_hifi_common_re.match(filename) is not None




# VERIFY


def test_illumina_shortread():
    filenames = [
        "355598_FUN_AGRF_H3GYVDSX2_AACTGAGC-CAATCAGG_L004_R2.fastq.gz",
        "45_5_FUN_AGRF_HNGM7DSX5_AATTCTTGGA-AAGTTGACAA_L004_R1.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None
