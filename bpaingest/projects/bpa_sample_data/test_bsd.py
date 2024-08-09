# VERIFY
from bpaingest.projects.bpa_sample_data.files import (
    #ont_promethion_re,
    #ont_promethion_common_re,
    bsd_site_image_filename_re,
    bsd_site_pdf_filename_re,
    illumina_shortread_re,
    pacbio_hifi_filename_re,
    pacbio_hifi_filename_2_re,
    pacbio_hifi_metadata_sheet_re,
    pacbio_hifi_common_re,

)

def test_bsd_site_image():
    filenames = ["468315_BSD_BPA_QUOKKA_image.jpg",
                 "468315_BSD_BPA_QUOKKA_image.png"]
    for filename in filenames:
        assert bsd_site_image_filename_re.match(filename) is not None

def test_bsd_site_pdf():
            filenames = ["BSD_BPA_QUOKKA_map.pdf"]
            for filename in filenames:
                assert bsd_site_pdf_filename_re.match(filename) is not None


"""
def test_illumina_shortread():
    filenames = [
        "355598_TSI_AGRF_H3GYVDSX2_AACTGAGC-CAATCAGG_L004_R2.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_re.match(filename) is not None



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


def test_pacbio_hifi_common():
    filenames = [
        "TSI_AGRF_m84073_230616_024551_s3.pdf"

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
"""

