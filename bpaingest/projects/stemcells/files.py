from ...libs.md5lines import md5lines

import re


transcriptome_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    (?P<project>\w+)_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>\w{9})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


def test_transcriptome():
    filenames = [
        '24708_PE_200bp_STEMCELLS_AGRF_HMHNFBCXX_CGATGT_L002_R2.fastq.gz',
        '29586_PE_200bp_STEMCELLS_AGRF_CAGCTANXX_CGATGT_L001_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(transcriptome_filename_re.match(filename) is not None)


# FIXME: we need the full convention from BPA / MA
metabolomics_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    SC_
    (?P<vendor>MA)_
    .*
    (\.tar\.gz|\.mzML)$
""", re.VERBOSE)


def test_metabolomics():
    filenames = [
        '24721_SC_MA_GCMS_PosC-1-857-29036_Bio21-GCMS-001.tar.gz',
        '24729_SC_MA_GCMS_NegC-4-857-29046_Bio21-GCMS-001.mzML',
    ]
    for filename in filenames:
        assert(metabolomics_filename_re.match(filename) is not None)


singlecell_filename_re = re.compile("""
    (?P<id>\d{4,6}-\d{4,6})_
    (?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    (?P<project>\w+)_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>\w{9})_
    (?P<index>[G|A|T|C|-]*|NoIndex)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


def test_singlecell():
    filenames = [
        '25116-28799_PE_400bp_Stemcells_UNSW_HVC2VBGXY_NoIndex_L002_R1.fastq.gz',
        '25116-28799_PE_400bp_Stemcells_UNSW_HVC2VBGXY_NoIndex_L004_R2.fastq.gz',
    ]
    for filename in filenames:
        assert(singlecell_filename_re.match(filename) is not None)


smallrna_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<insert_size>[\d-]+nt)_
    smRNA_
    (?P<project>\w+)_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>\w{9})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


def test_smallrna():
    filenames = [
        '24695_15-50nt_smRNA_STEMCELLS_AGRF_H5KHCADXY_TGACCA_L001_R1.fastq.gz',
        '29572_15-35nt_smRNA_STEMCELLS_AGRF_CA7VCANXX_AGTTCC_L008_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(smallrna_filename_re.match(filename) is not None)


def parse_md5_file(md5_file, regexp):
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            m = regexp.match(path)
            if not m:
                raise Exception("no match for {}".format(path))
            yield path, md5, m.groupdict()
