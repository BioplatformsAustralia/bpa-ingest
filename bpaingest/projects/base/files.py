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


amplicon_control_tech_vendor_flow_filename_re = re.compile("""
    ^(?P<control_type>Arc_mock_community|Arch_mock_community|Bac_mock_community|Fungal_mock_community|Fungal-mock-community|Fungal__mock_Community|Soil_DNA|Soil-DNA||STAN|NEG1|NEG2|Neg|Neg1|Neg2|NEG_1|NEG_2|Undetermined)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>[A-Z0-9]{5})_
    .*\.fastq\.gz$
""", re.VERBOSE)


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


metagenomics_regexps = [metagenomics_filename_re, metagenomics_run_filename_re]


site_image_filename_re = re.compile("""
    (?P<id1>\d{4,6})-
    (?P<id2>\d{4,6}).jpg
""", re.VERBOSE)
