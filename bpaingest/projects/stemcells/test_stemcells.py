from .files import (
    transcriptome_filename_re,
    metabolomics_filename_re,
    proteomics_filename_re,
    proteomics_filename2_re,
    proteomics_pool_filename2_re,
    proteomics_analysed_filename_re,
    singlecell_filename_re,
    singlecell_filename2_re,
    singlecell_raw_xlsx_filename_re,
    singlecell_index_info_filename_re,
    smallrna_filename_re)


def test_transcriptome():
    filenames = [
        '24708_PE_200bp_STEMCELLS_AGRF_HMHNFBCXX_CGATGT_L002_R2.fastq.gz',
        '29586_PE_200bp_STEMCELLS_AGRF_CAGCTANXX_CGATGT_L001_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(transcriptome_filename_re.match(filename) is not None)


def test_metabolomics():
    filenames = [
        '24721_SC_MA_GCMS_PosC-1-857-29036_Bio21-GCMS-001.tar.gz',
        '24729_SC_MA_GCMS_NegC-4-857-29046_Bio21-GCMS-001.mzML',
        '24721_SC_MA_LCMS_Pos-1-859-29065_Bio21-LC-QTOF-6545.tar.gz',
        '24721_SC_MA_LCMS_Pos-1-859-29065_Bio21-LC-QTOF-6545.mzML',
    ]
    for filename in filenames:
        assert(metabolomics_filename_re.match(filename) is not None)


def test_proteomics():
    filenames = [
        '29614_SC_APAF_MS_1D_IDA_161102_P19598_025e6_01.wiff',
        '29614_SC_APAF_MS_1D_IDA_161102_P19598_025e6_01.wiff.scan',
        '29614_SC_APAF_MS_1D_IDA_161102_P19598_05e6_01_DistinctPeptideSummary.txt',
        '29614_SC_APAF_MS_1D_IDA_161102_P19598_05e6_01_ProteinSummary.txt',
        '29613_SC_APAF_MS_2D_IDA_161102_P19598_1e6_All_DistinctPeptideSummary.txt',
        '29707_SC_MBPF_MS_DIA1_P16_0064_Exp2_Fusion.raw',
        '24717_SC_QIMR_20160414_VelosPro_BPAstem_Positive_2_5ug_300m_T1_R1.raw',
        '33223_SC_MBPF_DIA_Phos_P16_0064_Exp7_QEPlus.htrms'
    ]
    for filename in filenames:
        assert(proteomics_filename_re.match(filename) is not None)


def test_proteomics2():
    filenames = [
        'P16_0064_Exp5_68667_F1_SC_MBPF_MS_2D_DDA_QEPlus.raw', 'P16_0064_Exp5_68667_F2_SC_MBPF_MS_2D_DDA_QEPlus.raw',
        'P16_0064_Exp7_52075_Human_Phos_F10_SC_MBPF_MS_2D_DDA_QEPlus.raw'
    ]
    for filename in filenames:
        assert (proteomics_filename2_re.match(filename) is not None)


def test_proteomics_pool2():
    filenames = [
        'P16_0064_Exp2_Pool1_F1_SC_MBPF_MS_2D_DDA_Fusion.raw', 'P16_0064_Exp2_Pool2_F1_SC_MBPF_MS_2D_DDA_Fusion.raw',
        'P16_0064_Exp2_Pool1_F1_SC_MBPF_MS_2D_DDA_Fusion.raw'
    ]
    for filename in filenames:
        assert (proteomics_pool_filename2_re.match(filename) is not None)


def test_proteomics_analysed():
    filenames = [
        'P16_0064_Exp1_SC_MBPF_MS_Analysed_20161213.zip',
    ]
    for filename in filenames:
        assert(proteomics_analysed_filename_re.match(filename) is not None)


def test_singlecell():
    filenames = [
        '25116-28799_PE_400bp_Stemcells_UNSW_HVC2VBGXY_NoIndex_L002_R1.fastq.gz',
        '25116-28799_PE_400bp_Stemcells_UNSW_HVC2VBGXY_NoIndex_L004_R2.fastq.gz',
        '24732-25115_PE_550bp_Stemcells_WEHI_HHMYYBGXY_NoIndex_L001_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(singlecell_filename_re.match(filename) is not None)


def test_singlecell2():
    filenames = [
        '32395_32778_PE_471bp_Stemcells_UNSW_HLVK7BGX5_NoIndex_R1_001.fastq.gz',
        '32395_32778PE_471bp_Stemcells_UNSW_HW5L5BGX5_NoIndex_R1_001.fastq.gz'
    ]
    for filename in filenames:
        assert (singlecell_filename2_re.match(filename) is not None)


def test_singlecell_raw_xlsx():
    filenames = ['Stemcells_UNSW_HK7LHBGX5_metadata.xlsx', 'Stemcells_UNSW_HLVK7HBGX5_metadata.xlsx']
    for filename in filenames:
        assert (singlecell_raw_xlsx_filename_re.match(filename) is not None)


def test_singlecell_index_info():
    filenames = [
        'Stemcells_UNSW_HVC2VBGXY_index_info_BPA25116-28799.xlsx',
        'Stemcells_WEHI_HHMYYBGXY_index_info_BPA24732-25115.xlsx',
    ]
    for filename in filenames:
        assert(singlecell_index_info_filename_re.match(filename) is not None)


def test_smallrna():
    filenames = [
        '24695_15-50nt_smRNA_STEMCELLS_AGRF_H5KHCADXY_TGACCA_L001_R1.fastq.gz',
        '29572_15-35nt_smRNA_STEMCELLS_AGRF_CA7VCANXX_AGTTCC_L008_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(smallrna_filename_re.match(filename) is not None)
