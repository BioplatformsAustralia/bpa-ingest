# -*- coding: utf-8 -*-

import re
from ...libs.md5lines import md5lines
from ...util import make_logger


logger = make_logger(__name__)

PACBIO_FILENAME_PATTERN = """
    (?P<id>\d{4,6})_
    SEP_
    (?P<vendor>AGRF|UNSW)_
    PAC_
    (?P<run_id>m\d{6}_\d{6})_
    (?P<machine_data>\S*)_
    (?P<data_type>\S*)
"""
pacbio_filename_re = re.compile(PACBIO_FILENAME_PATTERN, re.VERBOSE)


def test_pacbio():
    filenames = [
        '25705_SEP_UNSW_PAC_m160304_174004_42272_c100950162550000001823211206101602_s1_p0.1.bax.h5.gz',
        '25705_SEP_UNSW_PAC_m160304_174004_42272_c100950162550000001823211206101602_s1_p0.1.subreads.fasta.gz',
        '25705_SEP_UNSW_PAC_m160304_174004_42272_c100950162550000001823211206101602_s1_p0.1.subreads.fastq.gz',
    ]
    for filename in filenames:
        assert(pacbio_filename_re.match(filename) is not None)


MISEQ_FILENAME_PATTERN = """
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


def test_miseq():
    filenames = [
        '25705_1_PE_700bp_SEP_UNSW_APAFC_TAGCGCTC-GAGCCTTA_S1_L001_I1.fastq.gz',
        '25705_1_PE_700bp_SEP_UNSW_APAFC_TAGCGCTC-GAGCCTTA_S1_L001_I2.fastq.gz',
    ]
    for filename in filenames:
        assert(miseq_filename_re.match(filename) is not None)

HISEQ_FILENAME_PATTERN = """
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


def test_hiseq():
    filenames = [
        '25874_PE_230bp_SEP_AGRF_CA3FUANXX_TAATGCGC-TAATCTTA_L001_R1.fastq.gz',
        '25884_PE_230bp_SEP_AGRF_CA3FUANXX_GAATTCGT-TAATCTTA_L001_R1.fastq.gz'
    ]

    for filename in filenames:
        assert(hiseq_filename_re.match(filename) is not None)


METABOLOMICS_DEEPLCMS_FILENAME_PATTERN = """
    (?P<id>\d{4,6})_
    SEP_
    (?P<vendor>MA)_
    (?P<platform>LC-MS|GC-MS)_
    (?P<mastr_ms_id>[A-Z0-9-]+)_
    (?P<machine_data>[^\.]+).
    tar.gz
"""
metabolomics_deepclms_filename_re = re.compile(METABOLOMICS_DEEPLCMS_FILENAME_PATTERN, re.VERBOSE)


def test_metabolomics_deeplcms():
    filenames = [
        '25835_SEP_MA_LC-MS_SA2760-1-813-28029_Bio21-LC-QTOF-001.tar.gz'
    ]

    for filename in filenames:
        assert(metabolomics_deepclms_filename_re.match(filename) is not None)


PROTEOMICS_DEEPLCMS_FILENAME_PATTERN = """
    (?P<id>\d{4,6})_
    SEP_
    (?P<vendor>MBPF)_
    MS_
    (?P<machine_data>[^\.]+).
    raw
"""
proteomics_deepclms_filename_re = re.compile(PROTEOMICS_DEEPLCMS_FILENAME_PATTERN, re.VERBOSE)


def test_proteomics_deeplcms():
    filenames = [
        '26089_SEP_MBPF_MS_QEPlus1.raw'
    ]

    for filename in filenames:
        assert(proteomics_deepclms_filename_re.match(filename) is not None)


PROTEOMICS_SWATHMS_1D_IDA_FILENAME_PATTERN = """
    (?P<id>\d{4,6})_
    SEP_
    (?P<vendor>APAF)_
    MS_1D_IDA_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_1d_ida_filename_re = re.compile(PROTEOMICS_SWATHMS_1D_IDA_FILENAME_PATTERN, re.VERBOSE)


def test_proteomics_swathms_1d_ida():
    filenames = [
        '25805_SEP_APAF_MS_1D_IDA_P19471_161006.wiff',
        '25805_SEP_APAF_MS_1D_IDA_P19471_161006.wiff.scan'
    ]

    for filename in filenames:
        assert(proteomics_swathms_1d_ida_filename_re.match(filename) is not None)


PROTEOMICS_SWATHMS_2D_IDA_FILENAME_PATTERN = """
    (?P<id>P\d{4,6})_
    (?P<taxon>[A-Za-z]+)_
    SEP_
    (?P<vendor>APAF)_
    MS_2D_IDA_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_2d_ida_filename_re = re.compile(PROTEOMICS_SWATHMS_2D_IDA_FILENAME_PATTERN, re.VERBOSE)


def test_proteomics_swathms_2d_ida():
    filenames = [
        'P19471_Kleb_SEP_APAF_MS_2D_IDA_161018_F01.wiff',
        'P19471_Kleb_SEP_APAF_MS_2D_IDA_161018_F01.wiff.scan'
        'P19471_Staph_SEP_APAF_MS_2D_IDA_161019_F07.wiff',
        'P19471_Staph_SEP_APAF_MS_2D_IDA_161019_F07.wiff.scan',
    ]

    for filename in filenames:
        assert(proteomics_swathms_2d_ida_filename_re.match(filename) is not None)


PROTEOMICS_SWATHMS_SWATH_RAW_FILENAME_PATTERN = """
    (?P<id>\d{4,6})_
    SEP_
    (?P<vendor>APAF)_
    MS_SWATH_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_swath_raw_filename_re = re.compile(PROTEOMICS_SWATHMS_SWATH_RAW_FILENAME_PATTERN, re.VERBOSE)


def test_proteomics_swathms_swath_raw():
    filenames = [
        '25805_SEP_APAF_MS_SWATH_P19471_161007.wiff',
        '25805_SEP_APAF_MS_SWATH_P19471_161007.wiff.scan',
    ]

    for filename in filenames:
        assert(proteomics_swathms_swath_raw_filename_re.match(filename) is not None)


PROTEOMICS_SWATHMS_MSLIB_FILENAME_PATTERN = """
    (?P<id>P\d{4,6})_
    (?P<taxon>[A-Za-z]+)_
    SEP_
    (?P<vendor>APAF)_
    MS_Lib_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_mslib_filename_re = re.compile(PROTEOMICS_SWATHMS_MSLIB_FILENAME_PATTERN, re.VERBOSE)


def test_proteomics_swathms_mslib():
    filenames = [
        'P19471_Kleb_SEP_APAF_MS_Lib_V1.txt',
        'P19471_Staph_SEP_APAF_MS_Lib_V1.txt',
    ]

    for filename in filenames:
        assert(proteomics_swathms_mslib_filename_re.match(filename) is not None)


PROTEOMICS_SWATHMS_MSPEAK_FILENAME_PATTERN = """
    (?P<id>P\d{4,6})_
    (?P<taxon>[A-Za-z]+)_
    SEP_
    (?P<vendor>APAF)_
    MS_SWATH_Peaks_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_mspeak_filename_re = re.compile(PROTEOMICS_SWATHMS_MSPEAK_FILENAME_PATTERN, re.VERBOSE)


def test_proteomics_swathms_mspeak():
    filenames = [
        'P19471_Kleb_SEP_APAF_MS_SWATH_Peaks_ExtendedLib_V1.xlsx',
        'P19471_Kleb_SEP_APAF_MS_SWATH_Peaks_LocalLib_V1.xlsx',
        'P19471_Staph_SEP_APAF_MS_SWATH_Peaks_extendedLib_V1.xlsx',
        'P19471_Staph_SEP_APAF_MS_SWATH_Peaks_LocalLib_V1.xlsx',
    ]

    for filename in filenames:
        assert(proteomics_swathms_mspeak_filename_re.match(filename) is not None)


PROTEOMICS_SWATHMS_MSRESULT_FILENAME_PATTERN = """
    (?P<id>P\d{4,6})_
    (?P<taxon>[A-Za-z]+)_
    SEP_
    (?P<vendor>APAF)_
    MS_SWATH_Result_
    (?P<machine_data>[^\.]+).
    (?P<type>.*)
"""
proteomics_swathms_msresult_filename_re = re.compile(PROTEOMICS_SWATHMS_MSRESULT_FILENAME_PATTERN, re.VERBOSE)


def test_proteomics_swathms_msresult():
    # FIXME -- no real data received yet, so can't double-check yet
    filenames = [
        'P19471_Kleb_SEP_APAF_MS_SWATH_Result_ExtendedLib_V1.xlsx',
        'P19471_Kleb_SEP_APAF_MS_SWATH_Result_LocalLib_V1.xlsx',
        'P19471_Staph_SEP_APAF_MS_SWATH_Result_extendedLib_V1.xlsx',
        'P19471_Staph_SEP_APAF_MS_SWATH_Result_LocalLib_V1.xlsx',
    ]

    for filename in filenames:
        assert(proteomics_swathms_msresult_filename_re.match(filename) is not None)


class MD5ParsedLine(object):
    def __init__(self, pattern, md5, path):
        self.pattern = pattern
        self._ok = False
        self.md5 = md5
        self.md5data = None
        self.filename = path
        matched = self.pattern.match(self.filename)
        if not matched:
            raise Exception("unable to match MD5 filename: `%s'" % (self.filename))
        self.md5data = matched.groupdict()

    def get(self, k):
        return self.md5data[k]

    def __str__(self):
        return "<parsed_md5> {} {} {}".format(self.filename, self.md5, self.md5data)


def parse_md5_file(pattern, md5_file):
    data = []
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            if path.endswith('.xlsx'):
                continue
            parsed_line = MD5ParsedLine(pattern, md5, path)
            data.append(parsed_line)
    return data
