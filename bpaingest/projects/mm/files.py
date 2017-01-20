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

metatranscriptome_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<extraction>\d+)_
    (?P<unknown>\w{3})_
    (?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    MM_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>\w{9})_
    (?P<index>[G|A|T|C|-]*|NoIndex)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


def test_metatranscriptome():
    filenames = [
        '34957_1_wor_PE_200bp_MM_AGRF_CA5YNANXX_CAGATC_L003_R1.fastq.gz',
        '34955_1_wir_PE_200bp_MM_AGRF_CA5YNANXX_TTAGGC_L003_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(metatranscriptome_filename_re.match(filename) is not None)


def parse_md5_file(md5_file, regexp):
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            m = regexp.match(path)
            if not m:
                raise Exception("no match for {}".format(path))
            yield path, md5, m.groupdict()
