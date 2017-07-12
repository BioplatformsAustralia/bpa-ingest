from ...libs.md5lines import md5lines
from ...util import make_logger
import re


logger = make_logger(__name__)


amplicon_control_tech_vendor_filename_re = re.compile("""
    ^(?P<control_type>Arc_mock_community|Arch_mock_community|Bac_mock_community|Fungal_mock_community|Fungal-mock-community|Fungal__mock_Community|Soil_DNA|Soil-DNA||STAN|NEG1|NEG2|Neg|Neg1|Neg2|NEG_1|NEG_2|Undetermined)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<index>[G|A|T|C|-]{8,12}(_[G|A|T|C|-]{8})?)_
    (?P<flow_id>[A-Z0-9]{5})_
    .*\.fastq\.gz$
""", re.VERBOSE)


def test_amplicon_control():
    filenames = [
        'Arch_mock_community_A16S_UNSW_ATCTCAGG_AAGGCTAT_AHFYA_S72_L001_I2.fastq.gz',
        'Arc_mock_community_A16S_UNSW_ATCTCAGG_AAGGCTAT_AE4DM_S72_L001_R2.fastq.gz',
        'Arc_mock_community_A16S_UNSW_ATCTCAGG_AH3G1_S72_L001_R2.fastq.gz',
        'Arc_mock_community_A16S_UNSW_GTAGAGGA_AAGGAGTA_AC9FT_S84_L001_I2.fastq.gz',
        'Fungal_mock_community_18S_AGRF_ACGTAGCATTTC_A89CY_S96_L001_R1.fastq.gz',
        'Fungal-mock-community_ITS_AGRF_CCAAGTCTTACA_AN5TK_AN5TK_CCAAGTCTTACA_L001_R1.fastq.gz',
        'NEG_1_16S_AGRF_GGAGACAAGGGA_A5K1H_S1_L001_I1.fastq.gz',
        'Soil_DNA_16S_AGRF_CCACCTACTCCA_A815N_S94_L001_I1.fastq.gz',
        'Soil_DNA_18S_AGRF_TGTGCTGTGTAG_ANC6T_ANC6T_TGTGCTGTGTAG_L001_R1.fastq.gz',
        'Soil_DNA_A16S_AGRF_GTAGAGGA_GTAAGGAG_AEMV2_S60_L001_I1.fastq.gz',
        'Soil_DNA_A16S_UNSW_ATCTCAGG_AH62P_S84_L001_I1.fastq.gz',
        'Soil_DNA_A16S_UNSW_GTAGAGGA_ACTGCATA_AC9FT_S72_L001_R2.fastq.gz',
        'Soil-DNA_ITS_AGRF_ACACTAGATCCG_AN5TK_AN5TK_ACACTAGATCCG_L001_R2.fastq.gz',
    ]
    for filename in filenames:
        assert(amplicon_control_tech_vendor_filename_re.match(filename) is not None)


amplicon_control_tech_vendor_flow_filename_re = re.compile("""
    ^(?P<control_type>Arc_mock_community|Arch_mock_community|Bac_mock_community|Fungal_mock_community|Fungal-mock-community|Fungal__mock_Community|Soil_DNA|Soil-DNA||STAN|NEG1|NEG2|Neg|Neg1|Neg2|NEG_1|NEG_2|Undetermined)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>[A-Z0-9]{5})_
    .*\.fastq\.gz$
""", re.VERBOSE)


def test_amplicon_control2():
    filenames = [
        'Bac_mock_community_16S_AGRF_B3BDY_AGATGTTCTGCT_L001_R2.fastq.gz',
        'Fungal__mock_Community_ITS_AGRF_B39G7_ATGGACCGAACC_L001_R2.fastq.gz',
        'Soil_DNA_16S_AGRF_B3C7L_CAAGCATGCCTA_L001_R1.fastq.gz',
        'NEG1_16S_AGRF_B3BDY_GAATAGAGCCAA_L001_R1.fastq.gz',
        'Neg1_16S_AGRF_B3L2P_CAGCGGTGACAT_L001_R1.fastq.gz',
        'NEG_2_18S_AGRF_B3C5P_CCATTCGCCCAT_L001_R2.fastq.gz',
        'NEG2_16S_AGRF_B3BDY_GTACGTGGGATC_L001_R2.fastq.gz',
        'Neg2_ITS_AGRF_B3C4H_AGAGCCTACGTT_L001_R2.fastq.gz',
        'Undetermined_16S_AGRF_A5K1H_S0_L001_R2.fastq.gz',
    ]
    for filename in filenames:
        assert(amplicon_control_tech_vendor_flow_filename_re.match(filename) is not None)


amplicon_control_regexps = [amplicon_control_tech_vendor_filename_re, amplicon_control_tech_vendor_flow_filename_re]

amplicon_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<index>[G|A|T|C|-]*)_
    (?P<flow_id>\w{5})_
    (?P<runsamplenum>\S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


def test_amplicon():
    filenames = [
        '15984_1_ITS_UNSW_ACTATTGTCACG_AGEDA_S71_L001_R2.fastq.gz',
        '9504_1_16S_AGRF_AATGCCTCAACT_A5K1H_S59_L001_R2.fastq.gz',
        '8101_1_ITS_UNSW_TCGTCGATAATC_A64JJ_S3_L001_I1.fastq.gz',
    ]
    for filename in filenames:
        assert(amplicon_filename_re.match(filename) is not None)


amplicon_filename_flow_index_swapped_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>\w{5})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


def test_amplicon_flow_index_swapped():
    filenames = [
        '19621_1_18S_AGRF_B3C5P_CCTTAAGTCAGT_L001_R1.fastq.gz',
        '19569_1_18S_AGRF_B3C5P_CTCCTGAAAGTT_L001_R1.fastq.gz'
    ]
    for filename in filenames:
        assert(amplicon_filename_flow_index_swapped_re.match(filename) is not None)


amplicon_index2_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<index>[G|A|T|C|-]*)_
    (?P<flow_id>\w{5})_
    (?P<flow_id2>\w{5})_
    (?P<index2>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


def test_amplicon_index2():
    filenames = [
        '19418_1_ITS_AGRF_GTCCGAAACACT_ANVM7_ANVM7_GTCCGAAACACT_L001_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(amplicon_index2_filename_re.match(filename) is not None)


amplicon_index3_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<index>[G|A|T|C|-]*)_
    (?P<index2>[G|A|T|C|-]*)_
    (?P<flow_id>\w{5})_
    (?P<runsamplenum>\S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


def test_amplicon_index3():
    filenames = [
        '13392_1_A16S_UNSW_TAGGCATG_GTAAGGAG_ACG8D_S30_L001_I1.fastq.gz',
    ]
    for filename in filenames:
        assert(amplicon_index3_filename_re.match(filename) is not None)


amplicon_regexps = [amplicon_filename_re, amplicon_index2_filename_re, amplicon_filename_flow_index_swapped_re, amplicon_index3_filename_re]


metagenomics_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    BASE_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>\w{9})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>R[1|2])\.fastq\.gz
""", re.VERBOSE)


def test_metagenomics():
    filenames = [
        '10718_2_PE_550bp_BASE_AGRF_HFLF3BCXX_ATTACTCG-CCTATCCT_L002_R2.fastq.gz',
    ]
    for filename in filenames:
        assert(metagenomics_filename_re.match(filename) is not None)


metagenomics_run_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    BASE_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>\w{9})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>R[1|2])_
    (?P<run>\d{3})\.fastq\.gz
""", re.VERBOSE)


def test_metagenomics_run():
    filenames = [
        '12450_1_PE_550bp_BASE_UNSW_HCLVFBCXX_ATTCAGAA-CCTATCCT_L001_R2_001.fastq.gz',
    ]
    for filename in filenames:
        assert(metagenomics_run_filename_re.match(filename) is not None)


metagenomics_regexps = [metagenomics_filename_re, metagenomics_run_filename_re]


site_image_filename_re = re.compile("""
    (?P<id1>\d{4,6})-
    (?P<id2>\d{4,6}).jpg
""", re.VERBOSE)


def test_site_image():
    filenames = [
        '7075-7076.jpg',
        '19233-19234.jpg'
    ]
    for filename in filenames:
        assert(site_image_filename_re.match(filename) is not None)


def parse_md5_file(md5_file, regexps):
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            # skip AGRF checksum program
            if path == 'TestFiles.exe':
                continue
            matches = filter(None, (regexp.match(path.split('/')[-1]) for regexp in regexps))
            m = None
            if matches:
                m = matches[0]
            if m:
                yield path, md5, m.groupdict()
            else:
                yield path, md5, None
