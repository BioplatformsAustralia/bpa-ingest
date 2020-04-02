import re

transcriptome_filename_re = re.compile(r"""
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

# FIXME: we need the full convention from BPA / MA
metabolomics_filename_re = re.compile(r"""
    (?P<id>\d{4,6})_
    SC_
    (?P<vendor>MA)_
    (?P<analytical_platform>GCMS|LCMS)_
    .*
    (\.tar\.gz|\.mzML)$
""", re.VERBOSE)

proteomics_filename_re = re.compile(r"""
    (?P<id>\d{4,6})_
    SC_
    (?P<vendor>APAF|MBPF|QIMR)_
    .*
    (\.wiff|\.wiff\.scan|\.txt|\.raw)$
""", re.VERBOSE)

proteomics_filename2_re = re.compile(r"""
 (P\d{2}_\d{4}_Exp\d_|)(?P<id>\d{4,6})_([a-zA-Z]+?_[a-zA-z]+?_|)(F\d{1,2}_|)
    SC_
    (?P<vendor>APAF|MBPF|QIMR)_
    .*
    (\.wiff|\.wiff\.scan|\.txt|\.raw)$
""", re.VERBOSE)

proteomics_pool_filename_re = re.compile(r"""
    (?P<pool_id>P\d+_\d+_Exp\d+_Pool\d+)_
    .*
    (\.raw)$
""", re.VERBOSE)

proteomics_pool_filename2_re = re.compile(r"""
    (P\d{2}_\d{4}_Exp\d_|)(?P<pool_id>P\d+_\d+_Exp\d+_Pool\d+)_
    .*
    (\.raw)$
""", re.VERBOSE)

proteomics_analysed_filename_re = re.compile(r"""
    (?P<zip_file_name>.*)
    (\.zip)$
""", re.VERBOSE)

singlecell_filename_re = re.compile(r"""
    (?P<id>\d{4,6}-\d{4,6})_
    (?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    (?P<project>\w+)_
    (?P<vendor>WEHI|UNSW)_
    (?P<flow_id>\w{9})_
    (?P<index>[G|A|T|C|-]*|NoIndex)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)

singlecell_filename2_re = re.compile(r"""
    (?P<id>\d{4,6}[-_]\d{4,6})
    _?(?P<library>PE|MP)_
    (?P<insert_size>\d*bp)_
    (?P<project>\w+)_
    (?P<vendor>WEHI|UNSW)_
    (?P<flow_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*|NoIndex)_
    (?P<read>[R|I][1|2])_
    (?P<lane>L?\d{3})
    \.fastq\.gz
""", re.VERBOSE)

SINGLECELL_RAW_XLSX_FILENAME_PATTERN = r"""
    .*
    (?P<vendor>WEHI|UNSW)_
    (?P<flow_id>\w{9,10})_
    metadata.xlsx$
"""

singlecell_raw_xlsx_filename_re = re.compile(
    SINGLECELL_RAW_XLSX_FILENAME_PATTERN, re.VERBOSE)

singlecell_index_info_filename_re = re.compile(r"""
    Stemcells_
    (?P<vendor>WEHI|UNSW)_
    (?P<flow_id>\w{9})_
    index_info_
    BPA(?P<id>\d{4,6}-\d{4,6})\.xlsx$
""", re.VERBOSE)

smallrna_filename_re = re.compile(r"""
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

xlsx_filename_re = re.compile(r'^.*\.xlsx')
pdf_filename_re = re.compile(r'^.*\.pdf')


def proteomics_raw_extract_pool_id(v):
    if v is None:
        return
    m = proteomics_pool_filename_re.match(v)
    if m is None:
        return
    return m.groupdict()['pool_id']
