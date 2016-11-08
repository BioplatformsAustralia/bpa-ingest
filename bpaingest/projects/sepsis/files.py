# -*- coding: utf-8 -*-

import re
from ...libs.md5lines import md5lines

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
        '25867_PE_350bp_ARP_AGRF_CA3FUANXX_CTGAAGCT-TAATCTTA_L001_R1.fastq.gz',
        '25867_PE_350bp_ARP_AGRF_CA3FUANXX_CTGAAGCT-TAATCTTA_L001_R2.fastq.gz',
    ]
    for filename in filenames:
        assert(hiseq_filename_re.match(filename) is not None)


class MD5ParsedLine(object):
    def __init__(self, pattern, md5, path):
        self.pattern = pattern
        self._ok = False
        self.md5 = md5
        self.md5data = None
        self.filename = path
        matched = self.pattern.match(self.filename)
        if matched:
            self.md5data = matched.groupdict()
            self._ok = True

    def is_ok(self):
        return self._ok

    def get(self, k):
        return self.md5data[k]

    def __str__(self):
        return "<parsed_md5> {} {} {}".format(self.filename, self.md5, self.md5data)


def parse_md5_file(pattern, md5_file):
    """ Parse md5 file """
    data = []
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            parsed_line = MD5ParsedLine(pattern, md5, path)
            if parsed_line.is_ok():
                data.append(parsed_line)
    return data
