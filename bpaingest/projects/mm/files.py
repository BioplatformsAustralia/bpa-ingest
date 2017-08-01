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


def test_amplicon_control():
    filenames = [
        'Arc_mock_community_1_A16S_UNSW_CGATCAGT-CCTAGAGT_ARVTL_S105_L001_R2.fastq.gz',
        'Bac_mock_community_16S_UNSW_GGATCGCA-CTAGTATG_AUWLK_S124_L001_I2.fastq.gz',
        'Fungal_mock_community_18S_UNSW_CGAGGCTG-AAGGCTAT_APK6N_S105_L001_I2.fastq.gz',
        'Soil_DNA_16S_UNSW_CAGCTAGA-GATAGCGT_AYBVB_S110_L001_I1.fastq.gz',
        'STAN_16S_UNSW_TATCAGGTGTGC_AL1HY_S97_L001_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(amplicon_control_filename_re.match(filename) is not None)


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


def test_amplicon():
    filenames = [
        '21878_1_A16S_UNSW_GGACTCCT-TATCCTCT_AP3JE_S17_L001_R1.fastq.gz',
        '21644_1_16S_UNSW_GAACTAGTCACC_AFGB7_S61_L001_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(amplicon_filename_re.match(filename) is not None)


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


def test_metagenomics():
    filenames = [
        '21744_1_PE_700bp_MM_UNSW_HM7K2BCXX_AAGAGGCA-AAGGAGTA_L001_R1.fastq.gz',
        '34318_1_PE_680bp_MM_AGRF_H3KWTBCXY_CTCTCTAC-ACTGCATA_L002_R1.fastq.gz',
        '21730_1_PE_700bp_MM_UNSW_HL7NGBCXX_GTAGAGGA-ACTGCATA_L002_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(metagenomics_filename_re.match(filename) is not None)


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


def test_metagenomics_v2():
    filenames = [
        '34734_1_PE_700bp_MM_UNSW_HMMJFBCXY_TAAGGCGA-CTCTCTAT_S5_L001_R2_001.fastq.gz',
        '36064_1_PE_700bp_MM_UNSW_HM35MBCXY_ACTGAGCG-GAGCCTTA_S13_L002_R1_001.fastq.gz',
    ]
    for filename in filenames:
        assert(metagenomics_filename_v2_re.match(filename) is not None)


def parse_md5_file(md5_file, regexps):
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            matches = [_f for _f in [t.match(path) for t in regexps] if _f]
            if matches:
                yield path, md5, matches[0].groupdict()
            else:
                yield path, md5, None
