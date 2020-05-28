from ...util import make_logger
from ...libs.md5lines import md5lines
import re


logger = make_logger(__name__)


base_amplicon_control_tech_vendor_filename_re = re.compile(
    r"""
    ^(?P<control_type>Arc_mock_community|Arch_mock_community|Bac_mock_community|Fungal_mock_community|Fungal-mock-community|Fungal__mock_Community|Soil_DNA|Soil-DNA||STAN|NEG1|NEG2|Neg|Neg1|Neg2|NEG_1|NEG_2|Undetermined)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<index>[G|A|T|C|-]{8,12}(_[G|A|T|C|-]{8})?)_
    (?P<flow_id>[A-Z0-9]{5})_
    .*\.fastq\.gz$
""",
    re.VERBOSE,
)


base_amplicon_control_tech_vendor_flow_filename_re = re.compile(
    r"""
    ^(?P<control_type>Arc_mock_community|Arch_mock_community|Bac_mock_community|Fungal_mock_community|Fungal-mock-community|Fungal__mock_Community|Soil_DNA|Soil-DNA||STAN|NEG1|NEG2|Neg|Neg1|Neg2|NEG_1|NEG_2|Undetermined)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>[A-Z0-9]{5})_
    .*\.fastq\.gz$
""",
    re.VERBOSE,
)


base_amplicon_control_regexps = [
    base_amplicon_control_tech_vendor_filename_re,
    base_amplicon_control_tech_vendor_flow_filename_re,
]

base_amplicon_filename_re = re.compile(
    r"""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<index>[G|A|T|C|-]*)_
    (?P<flow_id>\w{5})_
    (?P<runsamplenum>\S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""",
    re.VERBOSE,
)


base_amplicon_filename_flow_index_swapped_re = re.compile(
    r"""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<amplicon>ITS|16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>\w{5})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""",
    re.VERBOSE,
)


base_amplicon_index2_filename_re = re.compile(
    r"""
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
""",
    re.VERBOSE,
)


base_amplicon_index3_filename_re = re.compile(
    r"""
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
""",
    re.VERBOSE,
)


base_amplicon_regexps = [
    base_amplicon_filename_re,
    base_amplicon_index2_filename_re,
    base_amplicon_filename_flow_index_swapped_re,
    base_amplicon_index3_filename_re,
]


base_metagenomics_filename_re = re.compile(
    r"""
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
""",
    re.VERBOSE,
)


base_metagenomics_run_filename_re = re.compile(
    r"""
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
""",
    re.VERBOSE,
)


base_metagenomics_regexps = [
    base_metagenomics_filename_re,
    base_metagenomics_run_filename_re,
]


base_site_image_filename_re = re.compile(
    r"""
    (?P<id1>\d{4,6})-
    (?P<id2>\d{4,6}).jpg
""",
    re.VERBOSE,
)


mm_amplicon_control_filename_re = re.compile(
    r"""
    ^(?P<control_type>Arc_mock_community|Bac_mock_community|Fungal_mock_community|Soil_DNA|STAN)_
    (?P<extra_descriptor>).*
    (?P<vendor>AGRF|UNSW)_
    (?P<index>[G|A|T|C|-]*)_
    (?P<flow_id>\w{5})_
    (?P<runsamplenum>\S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""",
    re.VERBOSE,
)


mm_amplicon_filename_re = re.compile(
    r"""
    (?P<id>\d{4,6})_
    (?P<extraction>\d)_
    (?P<amplicon>16S|18S|A16S)_
    (?P<vendor>AGRF|UNSW)_
    (?P<index>[G|A|T|C|-]*|UNKNOWN)_
    (?P<flow_id>\w{5})_
    (?P<runsamplenum>\S\d*|UNKNOWN)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""",
    re.VERBOSE,
)


mm_transcriptome_filename_re = re.compile(
    r"""
    (?P<id>\d{4,6})_
    (?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    (?P<project>\w+)_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>\w{9})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""",
    re.VERBOSE,
)


mm_metatranscriptome_filename_re = re.compile(
    r"""
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
""",
    re.VERBOSE,
)


mm_metatranscriptome_filename2_re = re.compile(
    r"""
    (?P<id>\d{4,6})_
    (?P<extraction>\d+)_
    (?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    MM_
    (?P<vendor>AGRF|UNSW)_
    (?P<flow_id>\w{9})_
    (?P<index>[G|A|T|C|-]*|NoIndex)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""",
    re.VERBOSE,
)


mm_metagenomics_filename_re = re.compile(
    r"""
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
""",
    re.VERBOSE,
)


mm_metagenomics_filename_v2_re = re.compile(
    r"""
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
""",
    re.VERBOSE,
)


def parse_md5_file(md5_file, regexps):
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            matches = [_f for _f in [t.match(path) for t in regexps] if _f]
            if matches:
                yield path, md5, matches[0].groupdict()
            else:
                yield path, md5, None


amd_metagenomics_novaseq_re = re.compile(
    r"""
    (?P<id>\d{4,6})_
    MGE_
    (?P<flowcell>\w{9})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>S\d+)_
    (?P<lane>L\d{3})_
    (?P<read>R[1|2])_001\.fastq\.gz
""",
    re.VERBOSE,
)


amd_metagenomics_novaseq_control_re = re.compile(
    r"""
    SOIL_DNA_MGE_
    (?P<flowcell>\w{9})-
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>S\d+)_
    (?P<lane>L\d{3})_
    (?P<read>R[1|2])_001\.fastq\.gz
""",
    re.VERBOSE,
)


amd_amplicon_filename_re = re.compile(
    r"""
    (?P<id>\d{4,6})_
    (?P<amplicon>16S|18S|A16S)_
    (?P<flow_id>\w{5})_
    (?P<index>[G|A|T|C|-]*|UNKNOWN)_
    (?P<runsamplenum>\S\d*|UNKNOWN)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""",
    re.VERBOSE,
)


amd_amplicon_control_filename_re = re.compile(
    r"""
    ^(?P<control_type>Arc_mock_community|Bac_mock_community|Fungal_mock_community|Soil_DNA|STAN|.*MOCK|No_Template_Control)_
    (?P<extra_descriptor>).*
    (?P<amplicon>16S|18S|A16S)_
    (?P<flow_id>\w{5})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>\S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""",
    re.VERBOSE,
)
