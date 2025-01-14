# VERIFY
from bpaingest.projects.plant_protein_atlas.files import (
    phenoct_xray_raw_re,
    phenoct_xray_analysed_re,
    hyperspect_re,
    asd_spectro_re,
    xlsx_filename_re,
    metabolomics_pooled_filename_re,
)


def test_phenoct_xray_raw():
    filenames = [
        "448803_LibID450863_PPA_APPF_PhenoCT_Xray.rek",
    ]
    for filename in filenames:
        assert phenoct_xray_raw_re.match(filename) is not None


def test_phenoct_xray_analysed():
    filenames = [
        "448802_LibID450862_PPA_APPF_PhenoCT_analysed_data_files.zip",
    ]
    for filename in filenames:
        assert phenoct_xray_analysed_re.match(filename) is not None


def test_hyperspect():
    filenames = [
        "448846_LibID451026_PPA_APPF_SPECIM_FX10_Hyperspectral.zip",
        "448772_LibID450952_PPA_APPF_SPECIM_FX10_Hyperspectral.zip",
    ]
    for filename in filenames:
        assert hyperspect_re.match(filename) is not None


def test_asd_spectro():
    filenames = [
        "453279_PPA_APPF_ASD_FieldSpec_Spectroradiometer.xlsx",
    ]
    for filename in filenames:
        assert asd_spectro_re.match(filename) is not None


def test_xlsx_files():
    filenames = [
        "PPA_APPF_453279_librarymetadata.xlsx",
        "PPA_APPF_453278_librarymetadata.xlsx",
        "PPA_APPF_453277_librarymetadata.xlsx",
        # "PPA_APPF_Analysed_453277_librarymetadata.xlsx",
    ]
    for filename in filenames:
        assert xlsx_filename_re.match(filename) is not None

def test_metabolomics_pooled_files():
    filenames = [
                "453268_PPA_MA_AWRI_LCMS_nonpolar_metabolite_profiling_negative_R1_Nura.raw",
                "453268_PPA_MA_AWRI_LCMS_nonpolar_metabolite_profiling_negative_R1_PBA_Amberley.raw",
    ]
    for filename in filenames:
        assert metabolomics_pooled_filename_re.match(filename) is not None
