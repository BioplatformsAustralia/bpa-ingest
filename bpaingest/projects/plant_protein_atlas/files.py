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
    (_LCMS_nonpolar_metabolite_profiling_negative|_LCMS_polar_metabolite_profiling_positive|
    _LCMS_lipid_profiling_negative|_LCMS_lipid_profiling_positive|_NMR_profiling_1D|
    _NMR_profiling_2D|_LCMS_antinutritive_quantitation|)
    (_Batch(?P<Batch>\d)|)
    \.(raw|wiff2|ser|fid|wiff)
"""
metabolomics_sample_filename_re = re.compile(METABOLOMICS_SAMPLE_RAW_PATTERN, re.VERBOSE )

METABOLOMICS_POOLED_RAW_PATTERN = r"""
(?P<dataset_id>\d{4,6})
_PPA_
(?P<facility_id>(APPF|MA_AWRI)?)
(_LCMS_nonpolar_metabolite_profiling_negative_|_LCMS_polar_metabolite_profiling_positive_|
_LCMS_lipid_profiling_positive_|_LCMS_lipid_profiling_negative_|_NMR_profiling_1D_|
_NMR_profiling_2D_|_LCMS_antinutritive_quantitation|)
(?P<run>[R|I][1|2])
(_)
(?P<variety>\w+)\.(raw|wiff2|ser|fid|wiff)$
"""
metabolomics_pooled_filename_re = re.compile(METABOLOMICS_POOLED_RAW_PATTERN, re.VERBOSE)

METABOLOMICS_SCAN_RAW_PATTERN = r"""
(?P<dataset_id>\d{4,6})
_PPA_
(?P<facility_id>(APPF|MA_AWRI)?)
(_LCMS_nonpolar_metabolite_profiling_negative_|_LCMS_polar_metabolite_profiling_positive_|
_LCMS_lipid_profiling_positive_|_LCMS_lipid_profiling_negative_|_LCMS_antinutritive_quantitation|)
(_Batch)
(?P<Batch>\d)\.wiff\.scan$
"""
metabolomics_scan_filename_re = re.compile(METABOLOMICS_SCAN_RAW_PATTERN, re.VERBOSE)

METABOLOMICS_ANALYSED_PATTERN = r"""
    (Analysed_)
    (?P<dataset_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(MA_AWRI)))?
    (_LCMS_nonpolar_metabolite_profiling_negative|_LCMS_polar_metabolite_profiling_positive|
    _LCMS_lipid_profiling_positive|_LCMS_lipid_profiling_negative|_phenolic_quantitation|_saponin_quantitation|
    _total_lipid_content|_NMR_profiling_1D_2D|_LCMS_antinutritive_quantitation|)
    (_report|)
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

PROTEOMICS_SAMPLE_RAW_PATTERN_2 = r"""
    (?P<dataset_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(UNISA)))?
    (_LCMS_DIA|_LCMS_DDA)
    (_Albumin_Fraction_\d_Replicate_\d|_Globulin_Fraction_\d_Replicate_\d|_Glutelin_Fraction_\d_Replicate_\d|_Prolamin_Fraction_\d_Replicate_\d|_Residual_Fraction_\d_Replicate_\d)
    \.(timeseries\.data|wiff|wiff2|wiff\.scan|raw)$
"""
proteomics_sample_filename_2_re = re.compile(PROTEOMICS_SAMPLE_RAW_PATTERN_2, re.VERBOSE )

PROTEOMICS_ANALYSED_PATTERN = r"""
    (Analysed_)
    (?P<dataset_id>\d{4,6})_
    (PPA_
    (?P<facility_id>(UNISA)))?
    (_LCMS_|_)?
    (DIA_report|DIA|DDA_report|DDA|amino_acid_tryptophan_report|amino_acid_standard_report|amino_acid_cysteine_methionine_report|trypsin_inhibitors_quantitation_report|DIA_protein_fractionation|Protein_fractionation_report|Fractionation_protein_analysis_report)
    \.(pdf|xlsx)
"""
proteomics_analysed_filename_re = re.compile(PROTEOMICS_ANALYSED_PATTERN, re.VERBOSE)

ANALYSED_XLSX_PATTERN = r"""
    Analysed_PPA_
    (?P<facility_id>(APPF|LTU|MA_AWRI|UNISA|CSIRO))_
    (?P<dataset_id>\d{4,6})_
    (metadata)
    \.xlsx
"""

analysed_xlsx_filename_re = re.compile(ANALYSED_XLSX_PATTERN, re.VERBOSE)


PROTEOME_DATABASE_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    (proteome_database_\d{8}_)
    (PPA_
    (?P<facility_id>(UniSA|CSIRO)))?
    \.(fasta)$
"""
proteome_database_pattern_re = re.compile(PROTEOME_DATABASE_PATTERN, re.VERBOSE )


PROTEOME_XLSX_PATTERN = r"""
    Analysed_PPA_
    (?P<facility_id>(APPF|LTU|MA_AWRI|UNISA|CSIRO))_
    (?P<dataset_id>\d{4,6})_
    (metadata)
    \.xlsx
"""

proteome_xlsx_filename_re = re.compile(PROTEOME_XLSX_PATTERN, re.VERBOSE)
