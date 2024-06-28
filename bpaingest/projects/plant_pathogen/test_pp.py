# VERIFY
from bpaingest.projects.plant_pathogen.files import (
    illumina_shortread_re,
    pacbio_hifi_filename_re,
    pacbio_hifi_filename_2_re,
    pacbio_hifi_metadata_sheet_re,
    pacbio_hifi_common_re,
    ont_promethion_re,
    ont_promethion_common_re,
 )


# VERIFY


def test_illumina_shortread():
    filenames = [
        "394529_LibID396779_PP_AGRF_KTMDN_GCCACAGG-CATGCCAT_L001_R1.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None

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
    filenames = [
        "PP_AGRF_m84073_230616_014416_s1.pdf"

    ]
    for filename in filenames:
        assert pacbio_hifi_common_re.match(filename) is not None

def test_ont_promethion():
    filenames = [
        "395601_LibID397855_PP_BRF_PAQ21103_ONTPromethION_fastq_fail.tar",
        "395601_LibID397855_PP_BRF_PAQ21103_ONTPromethION_fastq_pass.tar",
        "395601_LibID397855_PP_BRF_PAQ21103_ONTPromethION_pod5_fail.tar",
        "395602_LibID397856_PP_BRF_PAQ21103_ONTPromethION_fastq_pass.tar"
    ]
    for filename in filenames:
        assert ont_promethion_re.match(filename) is not None

def test_ont_promethion_common():
    filenames = [
        "PP_BRF_PAQ21103_ONTPromethION_barcode_alignment.tsv"
        "PP_BRF_PAQ21103_ONTPromethION_report.html",
        "PP_BRF_PAQ21103_ONTPromethION_sequencing_summary.txt",
        "PP_BRF_PAW33991_ONTPromethION_pod5.tar",
    ]
    for filename in filenames:
        assert ont_promethion_common_re.match(filename) is not None

