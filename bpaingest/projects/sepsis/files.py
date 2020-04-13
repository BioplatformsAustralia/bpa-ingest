# -*- coding: utf-8 -*-

import re
from ...util import make_logger


logger = make_logger(__name__)

PACBIO_FILENAME_PATTERN = r"""
    (?P<id>\d{4,6})_
    SEP_
    (?P<vendor>AGRF|UNSW)_
    PAC_
    (?P<run_id>m\d{6}_\d{6})_
    (?P<machine_data>\S*)_
    (?P<data_type>\S*)
"""
pacbio_filename_re = re.compile(PACBIO_FILENAME_PATTERN, re.VERBOSE)


MISEQ_FILENAME_PATTERN = r"""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<library>PE|MP)_
    (?P<size>\d*bp)_
    SEP_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_cell_id>\w{5})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>\S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
"""
miseq_filename_re = re.compile(MISEQ_FILENAME_PATTERN, re.VERBOSE)


HISEQ_FILENAME_PATTERN = r"""
    (?P<id>\d{4,6})_
    (?P<library>PE|MP)_
    (?P<size>\d*bp)_
    SEP_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_cell_id>\w{9})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
"""
hiseq_filename_re = re.compile(HISEQ_FILENAME_PATTERN, re.VERBOSE)


METABOLOMICS_LCMS_GCMS_FILENAME_PATTERN = r"""
    (?P<id>\d{4,6})_
    (SEP|Sep)_
    (?P<vendor>MA)_
    (?P<platform>LC-MS|LCMS|GCMS|GC-MS)_
    (?P<mastr_ms_id>[A-Z0-9-]+)_
    (?P<machine_data>[^\.]+)\.
    (tar\.gz|qgd|d\.tar\.gz)
"""
metabolomics_lcms_gcms_filename_re = re.compile(METABOLOMICS_LCMS_GCMS_FILENAME_PATTERN, re.VERBOSE)


PROTEOMICS_MS1QUANTIFICATION_FILENAME_PATTERN = r"""
    (?P<id>\d{4,6})_
    SEP_
    (?P<vendor>MBPF)_
    MS_
    (?P<machine_data>[^\.]+).
    raw
"""
proteomics_ms1quantification_filename_re = re.compile(PROTEOMICS_MS1QUANTIFICATION_FILENAME_PATTERN, re.VERBOSE)


PROTEOMICS_SWATHMS_1D_IDA_FILENAME_PATTERN = r"""
    (?P<id>\d{4,6})_
    SEP_
    (?P<vendor>APAF)_
    MS_1D_IDA_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_1d_ida_filename_re = re.compile(PROTEOMICS_SWATHMS_1D_IDA_FILENAME_PATTERN, re.VERBOSE)


PROTEOMICS_SWATHMS_2D_IDA_FILENAME_PATTERN = r"""
    (?P<id>P\d{4,6})_
    (?P<taxon>[A-Za-z]+)_
    SEP_
    (?P<vendor>APAF)_
    MS_2D_IDA_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_2d_ida_filename_re = re.compile(PROTEOMICS_SWATHMS_2D_IDA_FILENAME_PATTERN, re.VERBOSE)


PROTEOMICS_SWATHMS_SWATH_RAW_FILENAME_PATTERN = r"""
    (?P<id>\d{4,6})_
    SEP_
    (?P<vendor>APAF)_
    MS_SWATH_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_swath_raw_filename_re = re.compile(PROTEOMICS_SWATHMS_SWATH_RAW_FILENAME_PATTERN, re.VERBOSE)

PROTEOMICS_2DLIBRARY_FILENAME_PATTERN = r"""
    (?P<pool_id>[^_]+)_
    (?P<pool_name>Pooled\ sera|RPMI|Serum|PooledSera|RPMIGalactose|RPMIGlucose)_
    (?P<fraction>Fraction\d+)_
    SEP_
    (?P<vendor>MBPF)_
    MS_
    (?P<machine_data>[^\.]+).
    raw$
"""
proteomics_2dlibrary_filename_re = re.compile(PROTEOMICS_2DLIBRARY_FILENAME_PATTERN, re.VERBOSE)


PROTEOMICS_SWATHMS_LIB_FILENAME_PATTERN = r"""
    (?P<id>P\d{4,6})_
    (?P<taxon>\w+)_
    SEP_
    (?P<vendor>APAF)_
    (?P<type>MS_Lib_|Lib_extended|MS_Lib)
    (?P<machine_data>)
"""
proteomics_swathms_lib_filename_re = re.compile(PROTEOMICS_SWATHMS_LIB_FILENAME_PATTERN, re.VERBOSE)


PROTEOMICS_SWATHMS_MSPEAK_FILENAME_PATTERN = r"""
    (?P<id>P\d{4,6})_
    (?P<taxon>[A-Za-z]+)_
    SEP_
    (?P<vendor>APAF)_
    MS_SWATH_Peaks_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_mspeak_filename_re = re.compile(PROTEOMICS_SWATHMS_MSPEAK_FILENAME_PATTERN, re.VERBOSE)


PROTEOMICS_SWATHMS_MSRESULT_FILENAME_PATTERN = r"""
    (?P<id>P\d{4,6})_
    (?P<taxon>[A-Za-z]+)_
    SEP_
    (?P<vendor>APAF)_
    MS_SWATH_Result_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_msresult_filename_re = re.compile(PROTEOMICS_SWATHMS_MSRESULT_FILENAME_PATTERN, re.VERBOSE)
