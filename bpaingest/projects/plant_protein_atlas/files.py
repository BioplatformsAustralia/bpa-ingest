from ...util import make_logger
import re

logger = make_logger(__name__)

PHENOCT_XRAY_RAW_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(APPF)))?
    (_PhenoCT_Xray|)
    \.rek$
"""

phenoct_xray_raw_re = re.compile(
    PHENOCT_XRAY_RAW_PATTERN, re.VERBOSE
)

PHENOCT_XRAY_ANALYSED_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(APPF)))?
    (_PhenoCT_analysed_data_files|)
    \.zip$
"""

phenoct_xray_analysed_re = re.compile(
    PHENOCT_XRAY_ANALYSED_PATTERN, re.VERBOSE
)
HYPERSPECT_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    PPA_
    (?P<facility_id>(APPF|)?)
    _SPECIM_FX10_Hyperspectral
    \.zip$
"""

hyperspect_re = re.compile(
    HYPERSPECT_PATTERN, re.VERBOSE
)

ASD_SPECTRO_PATTERN = r"""
    (?P<dataset_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(APPF)))?
    (_ASD_FieldSpec_Spectroradiometer|)
    \.xlsx$
"""

asd_spectro_re = re.compile(
    ASD_SPECTRO_PATTERN, re.VERBOSE
)

XLSX_PATTERN = r"""
    PPA_
    (?P<facility_id>(APPF))_
    (Analysed_|)
    (?P<dataset_id>\d{4,6})_
    (librarymetadata|samplemetadata_ingest)
    \.xlsx
"""

xlsx_filename_re = re.compile(XLSX_PATTERN, re.VERBOSE)

