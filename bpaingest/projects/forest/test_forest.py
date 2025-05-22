from .files import (
    pacbio_hifi_filename_revio_re,
    pacbio_hifi_revio_pdf_re,
    pacbio_hifi_revio_metadata_sheet_re,
    illumina_shortread_re,
)


def test_pacbio_hifi_revio():
    filenames = [
        "645250_FOR_BRF_m84118_250322_030318_s3.hifi_reads.bc2013.bam",
    ]

    for filename in filenames:
        assert pacbio_hifi_filename_revio_re.match(filename) is not None


def test_pacbio_hifi_revio_pdf():
    filenames = ["FOR_BRF_m84118_250322_030318_s3.pdf"]

    for filename in filenames:
        assert pacbio_hifi_revio_pdf_re.match(filename) is not None



def test_pacbio_hifi_revio_metadata_sheet():
    filenames = [
        "FOR_BRF_m84118_250322_030318_s3_metadata.xlsx",
    ]

    for filename in filenames:
        assert pacbio_hifi_revio_metadata_sheet_re.match(filename) is not None

def test_illumina_shortread():
    filenames = [
        "645459_FOR_BRF_22KYV5LT4_GCTAGACTAT-AATACGACAT_S209_R1_001.fastq.gz",
    ]

    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None
