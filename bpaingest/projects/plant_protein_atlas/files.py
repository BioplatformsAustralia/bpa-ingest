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

phenoct_xray_raw_re = re.compile(PHENOCT_XRAY_RAW_PATTERN, re.VERBOSE)

PHENOCT_XRAY_ANALYSED_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(APPF)))?
    (_PhenoCT_analysed_data_files|)
    \.zip$
"""

phenoct_xray_analysed_re = re.compile(PHENOCT_XRAY_ANALYSED_PATTERN, re.VERBOSE)
HYPERSPECT_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    PPA_
    (?P<facility_id>(APPF|)?)
    _SPECIM_FX10_Hyperspectral
    \.zip$
"""

hyperspect_re = re.compile(HYPERSPECT_PATTERN, re.VERBOSE)

ASD_SPECTRO_PATTERN = r"""
    (?P<dataset_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(APPF)))?
    (_ASD_FieldSpec_Spectroradiometer|)
    \.xlsx$
"""

asd_spectro_re = re.compile(ASD_SPECTRO_PATTERN, re.VERBOSE)

XLSX_PATTERN = r"""
    PPA_
    (?P<facility_id>(APPF|LTU|MA_AWRI|UNISA|CSIRO))_
    (Analysed_|)
    (?P<dataset_id>\d{4,6})_
    (librarymetadata|samplemetadata_ingest|metadata)
    \.xlsx
"""

xlsx_filename_re = re.compile(XLSX_PATTERN, re.VERBOSE)

METABOLOMICS_SAMPLE_RAW_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(MA_AWRI)))?
    (_LCMS_nonpolar_metabolite_profiling_negative|)
    \.raw$
"""
metabolomics_sample_filename_re = re.compile(METABOLOMICS_SAMPLE_RAW_PATTERN, re.VERBOSE )

METABOLOMICS_POOLED_RAW_PATTERN = r"""
(?P<dataset_id>\d{4,6})
_PPA_
(?P<facility_id>(APPF|MA_AWRI)?)
_LCMS_nonpolar_metabolite_profiling_negative_
(?P<run>[R|I][1|2])
(_)
(?P<variety>\w+)\.raw$
"""
metabolomics_pooled_filename_re = re.compile(METABOLOMICS_POOLED_RAW_PATTERN, re.VERBOSE)

METABOLOMICS_ANALYSED_PATTERN = r"""
    (Analysed_)
    (?P<dataset_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(MA_AWRI)))?
    (_LCMS_nonpolar_metabolite_profiling_negative|)
    \.(pdf|xlsx)
"""
metabolomics_analysed_filename_re = re.compile(METABOLOMICS_ANALYSED_PATTERN, re.VERBOSE)

PROTEOMICS_SAMPLE_RAW_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(UniSA)))?
    (_LCMS_DIA|_LCMS_DDA)
    \.(timeseries\.data|wiff|wiff2|wiff\.scan|raw)$
"""
proteomics_sample_filename_re = re.compile(PROTEOMICS_SAMPLE_RAW_PATTERN, re.VERBOSE )

PROTEOMICS_ANALYSED_PATTERN = r"""
    (Analysed_)
    (?P<dataset_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(UNISA)))?
    (_LCMS_|_)?
    (DIA_report|DIA|amino_acid_tryptophan_report|amino_acid_standard_report|amino_acid_cysteine_methionine_report|trypsin_inhibitors_quantitation_report)
    \.(pdf|xlsx)
"""
proteomics_analysed_filename_re = re.compile(PROTEOMICS_ANALYSED_PATTERN, re.VERBOSE)

PROTEOMICS_ANALYSED_DATABASE_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    (proteome_database_\d{8}_)
    (PPA_
    (?P<facility_id>(UniSA|CSIRO)))?
    \.(fasta)$
"""
proteomics_analysed_database_pattern_re = re.compile(PROTEOMICS_ANALYSED_DATABASE_PATTERN, re.VERBOSE )


ANALYSED_XLSX_PATTERN = r"""
    Analysed_PPA_
    (?P<facility_id>(APPF|LTU|MA_AWRI|UNISA|CSIRO))_
    (?P<dataset_id>\d{4,6})_
    (metadata)
    \.xlsx
"""

analysed_xlsx_filename_re = re.compile(ANALYSED_XLSX_PATTERN, re.VERBOSE)
