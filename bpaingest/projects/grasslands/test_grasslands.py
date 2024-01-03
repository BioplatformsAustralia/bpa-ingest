from .files import (
    illumina_shortread_rna_phylo_re,
    pacbio_hifi_filename_revio_re,
    pacbio_hifi_revio_pdf_re,
    pacbio_hifi_revio_metadata_sheet_re,

)



def test_illumina_rna_and_phylo_shortread():
    filenames = [
        "369564_LibID371565_AG_BRF_HMMYTDRX3_TCAGCATC_S1_L001_R2_001.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_rna_phylo_re.match(filename) is not None


def test_pacbio_hifi_revio():
    filenames = [
        "369564_AG_BRF_m84118_231208_115614_s4.hifi_reads.bam",
    ]

    for filename in filenames:
        assert pacbio_hifi_filename_revio_re.match(filename) is not None


def test_pacbio_hifi_revio_pdf():
    filenames = [
        "AG_BRF_m84118_231208_115614_s4.pdf"
    ]

    for filename in filenames:
        assert pacbio_hifi_revio_pdf_re.match(filename) is not None



def test_pacbio_hifi_revio_metadata_sheet():
    filenames = [
        "AG_BRF_m84118_231208_115614_s4_metadata.xlsx",
    ]

    for filename in filenames:
        assert pacbio_hifi_revio_metadata_sheet_re.match(filename) is not None

