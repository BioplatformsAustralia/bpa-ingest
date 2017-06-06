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
    (?P<analytical_platform>GCMS|LCMS)_
    .*
    (\.tar\.gz|\.mzML)$
""", re.VERBOSE)


def test_metabolomics():
    filenames = [
        '24721_SC_MA_GCMS_PosC-1-857-29036_Bio21-GCMS-001.tar.gz',
        '24729_SC_MA_GCMS_NegC-4-857-29046_Bio21-GCMS-001.mzML',
        '24721_SC_MA_LCMS_Pos-1-859-29065_Bio21-LC-QTOF-6545.tar.gz',
        '24721_SC_MA_LCMS_Pos-1-859-29065_Bio21-LC-QTOF-6545.mzML',
    ]
    for filename in filenames:
        assert(metabolomics_filename_re.match(filename) is not None)


proteomics_filename_re = re.compile("""
    (?P<id>\d{4,6})_
    SC_
    (?P<vendor>APAF|MBPF)_
    .*
    (\.wiff|\.wiff\.scan|\.txt|\.raw)$
""", re.VERBOSE)


def test_proteomics():
    filenames = [
        '29614_SC_APAF_MS_1D_IDA_161102_P19598_025e6_01.wiff',
        '29614_SC_APAF_MS_1D_IDA_161102_P19598_025e6_01.wiff.scan',
        '29614_SC_APAF_MS_1D_IDA_161102_P19598_05e6_01_DistinctPeptideSummary.txt',
        '29614_SC_APAF_MS_1D_IDA_161102_P19598_05e6_01_ProteinSummary.txt',
        '29613_SC_APAF_MS_2D_IDA_161102_P19598_1e6_All_DistinctPeptideSummary.txt',
        '29707_SC_MBPF_MS_DIA1_P16_0064_Exp2_Fusion.raw',
    ]
    for filename in filenames:
        assert(proteomics_filename_re.match(filename) is not None)


proteomics_pool_filename_re = re.compile("""
    (?P<pool_id>P\d+_\d+_Exp\d+_Pool\d+)_
    .*
    (\.raw)$
""", re.VERBOSE)


def test_proteomics_pool():
    filenames = [
        'P16_0064_Exp2_Pool2_F5_SC_MBPF_MS_2D_DDA_Fusion.raw'
    ]
    for filename in filenames:
        assert(proteomics_pool_filename_re.match(filename) is not None)


proteomics_analysed_filename_re = re.compile("""
    (?P<zip_file_name>.*)
    (\.zip)$
""", re.VERBOSE)


def test_proteomics_analysed():
    filenames = [
        'P16_0064_Exp1_SC_MBPF_MS_Analysed_20161213.zip',
    ]
    for filename in filenames:
        assert(proteomics_analysed_filename_re.match(filename) is not None)


singlecell_filename_re = re.compile("""
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


def test_singlecell():
    filenames = [
        '25116-28799_PE_400bp_Stemcells_UNSW_HVC2VBGXY_NoIndex_L002_R1.fastq.gz',
        '25116-28799_PE_400bp_Stemcells_UNSW_HVC2VBGXY_NoIndex_L004_R2.fastq.gz',
        '24732-25115_PE_550bp_Stemcells_WEHI_HHMYYBGXY_NoIndex_L001_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(singlecell_filename_re.match(filename) is not None)


singlecell_index_info_filename_re = re.compile("""
    Stemcells_
    (?P<vendor>WEHI|UNSW)_
    (?P<flow_id>\w{9})_
    index_info_
    BPA(?P<id>\d{4,6}-\d{4,6})\.xlsx$
""", re.VERBOSE)


def test_singlecell_index_info():
    filenames = [
        'Stemcells_UNSW_HVC2VBGXY_index_info_BPA25116-28799.xlsx',
        'Stemcells_WEHI_HHMYYBGXY_index_info_BPA24732-25115.xlsx',
    ]
    for filename in filenames:
        assert(singlecell_index_info_filename_re.match(filename) is not None)


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

xlsx_filename_re = re.compile(r'^.*\.xlsx')
pdf_filename_re = re.compile(r'^.*\.pdf')


def parse_md5_file(md5_file, regexps):
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            matches = filter(None, (regexp.match(path.split('/')[-1]) for regexp in regexps))
            m = None
            if matches:
                m = matches[0]
            if m:
                yield path, md5, m.groupdict()
            else:
                if path.endswith('_metadata.xlsx'):
                    continue
                if path.endswith('_Report.pdf'):
                    continue
                yield path, md5, None
