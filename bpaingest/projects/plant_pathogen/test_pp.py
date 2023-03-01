# VERIFY
from bpaingest.projects.plant_pathogen.files import (
    illumina_shortread_re,
    pacbio_hifi_filename_re,
    pacbio_hifi_metadata_sheet_re,
    illumina_shortread_2_re,
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


def test_pacbio_hifi_metadata_sheet():
    filenames = [
        "PP_AGRF_DA218825_metadata.xlsx",
    ]
    for filename in filenames:
        assert pacbio_hifi_metadata_sheet_re.match(filename) is not None
