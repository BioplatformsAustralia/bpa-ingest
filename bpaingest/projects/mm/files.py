from ...libs.md5lines import md5lines

import re


amplicon_control_filename_re = re.compile("""
    ^(?P<control_type>Arc_mock_community|Bac_mock_community|Fungal_mock_community|Soil_DNA|STAN)_
    (?P<extra_descriptor>).*
    (?P<vendor>AGRF|UNSW)_
    (?P<index>[G|A|T|C|-]*)_
    (?P<flow_id>\w{5})_
    (?P<runsamplenum>\S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


amplicon_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<amplicon>16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<index>[G|A|T|C|-]*)_
    (?P<flow_id>\w{5})_
    (?P<runsamplenum>\S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


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


metagenomics_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    MM_
    (?P<vendor>AGRF|UNSW)_
    (?P<flowcell>\w{9})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>R[1|2])\.fastq\.gz
""", re.VERBOSE)


metagenomics_filename_v2_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    MM_
    (?P<vendor>AGRF|UNSW)_
    (?P<flowcell>\w{9})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>S\d+)_
    (?P<lane>L\d{3})_
    (?P<read>R[1|2])_
    001\.fastq\.gz
""", re.VERBOSE)


def parse_md5_file(md5_file, regexps):
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            matches = [_f for _f in [t.match(path) for t in regexps] if _f]
            if matches:
                yield path, md5, matches[0].groupdict()
            else:
                yield path, md5, None
