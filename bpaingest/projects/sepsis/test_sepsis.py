from .files import (
    pacbio_filename_re,
    miseq_filename_re,
    hiseq_filename_re,
    metabolomics_lcms_gcms_filename_re,
    proteomics_ms1quantification_filename_re,
    proteomics_swathms_1d_ida_filename_re,
    proteomics_swathms_2d_ida_filename_re,
    proteomics_swathms_swath_raw_filename_re,
    proteomics_swathms_lib_filename_re,
    proteomics_swathms_mspeak_filename_re,
    proteomics_swathms_msresult_filename_re,
    proteomics_2dlibrary_filename_re)


def test_pacbio():
    filenames = [
        '25705_SEP_UNSW_PAC_m160304_174004_42272_c100950162550000001823211206101602_s1_p0.1.bax.h5.gz',
        '25705_SEP_UNSW_PAC_m160304_174004_42272_c100950162550000001823211206101602_s1_p0.1.subreads.fasta.gz',
        '25705_SEP_UNSW_PAC_m160304_174004_42272_c100950162550000001823211206101602_s1_p0.1.subreads.fastq.gz',
    ]
    for filename in filenames:
        assert(pacbio_filename_re.match(filename) is not None)


def test_miseq():
    filenames = [
        '25705_1_PE_700bp_SEP_UNSW_APAFC_TAGCGCTC-GAGCCTTA_S1_L001_I1.fastq.gz',
        '25705_1_PE_700bp_SEP_UNSW_APAFC_TAGCGCTC-GAGCCTTA_S1_L001_I2.fastq.gz',
    ]
    for filename in filenames:
        assert(miseq_filename_re.match(filename) is not None)


def test_hiseq():
    filenames = [
        '25874_PE_230bp_SEP_AGRF_CA3FUANXX_TAATGCGC-TAATCTTA_L001_R1.fastq.gz',
        '25884_PE_230bp_SEP_AGRF_CA3FUANXX_GAATTCGT-TAATCTTA_L001_R1.fastq.gz'
    ]

    for filename in filenames:
        assert(hiseq_filename_re.match(filename) is not None)


def test_metabolomics_lcms_gcms():
    filenames = [
        '25835_SEP_MA_LC-MS_SA2760-1-813-28029_Bio21-LC-QTOF-001.tar.gz',
        '50534_SEP_MA_LCMS_2900M-5-906-30612_Bio21-LC-QTOF-6545.tar.gz',
        '50557_SEP_MA_GCMS_2986M-4-907-30689_Bio21-GC-QQQ-Shimadzu.qgd',
        '51193_Sep_MA_GCMS_5448_6_594_34684_BIO21_GCQQQ_Shimadzu.qgd',
    ]

    for filename in filenames:
        assert(metabolomics_lcms_gcms_filename_re.match(filename) is not None)


def test_proteomics_ms1quantification():
    filenames = [
        '26089_SEP_MBPF_MS_QEPlus1.raw'
    ]

    for filename in filenames:
        assert(proteomics_ms1quantification_filename_re.match(filename) is not None)


def test_proteomics_swathms_1d_ida():
    filenames = [
        '25805_SEP_APAF_MS_1D_IDA_P19471_161006.wiff',
        '25805_SEP_APAF_MS_1D_IDA_P19471_161006.wiff.scan'
    ]

    for filename in filenames:
        assert(proteomics_swathms_1d_ida_filename_re.match(filename) is not None)


def test_proteomics_swathms_2d_ida():
    filenames = [
        'P19471_Kleb_SEP_APAF_MS_2D_IDA_161018_F01.wiff',
        'P19471_Kleb_SEP_APAF_MS_2D_IDA_161018_F01.wiff.scan'
        'P19471_Staph_SEP_APAF_MS_2D_IDA_161019_F07.wiff',
        'P19471_Staph_SEP_APAF_MS_2D_IDA_161019_F07.wiff.scan',
    ]

    for filename in filenames:
        assert(proteomics_swathms_2d_ida_filename_re.match(filename) is not None)


def test_proteomics_swathms_swath_raw():
    filenames = [
        '25805_SEP_APAF_MS_SWATH_P19471_161007.wiff',
        '25805_SEP_APAF_MS_SWATH_P19471_161007.wiff.scan',
    ]

    for filename in filenames:
        assert(proteomics_swathms_swath_raw_filename_re.match(filename) is not None)


def test_proteomics_swathms_lib():
    filenames = [
        'P19471_Kleb_SEP_APAF_MS_Lib_V1.txt',
        'P19471_Staph_SEP_APAF_MS_Lib_V1.txt',
        'P19471_Staph_SEP_APAF_Lib_extended_V1.txt',
        'P20249_Staph2900_SEP_APAF_MS_Lib.txt'
    ]

    for filename in filenames:
        assert(proteomics_swathms_lib_filename_re.match(filename) is not None)


def test_proteomics_swathms_mspeak():
    filenames = [
        'P19471_Kleb_SEP_APAF_MS_SWATH_Peaks_ExtendedLib_V1.xlsx',
        'P19471_Kleb_SEP_APAF_MS_SWATH_Peaks_LocalLib_V1.xlsx',
        'P19471_Staph_SEP_APAF_MS_SWATH_Peaks_extendedLib_V1.xlsx',
        'P19471_Staph_SEP_APAF_MS_SWATH_Peaks_LocalLib_V1.xlsx',
    ]

    for filename in filenames:
        assert(proteomics_swathms_mspeak_filename_re.match(filename) is not None)


def test_proteomics_swathms_msresult():
    # FIXME -- no real data received yet, so can't double-check yet
    filenames = [
        'P19471_Kleb_SEP_APAF_MS_SWATH_Result_ExtendedLib_V1.xlsx',
        'P19471_Kleb_SEP_APAF_MS_SWATH_Result_LocalLib_V1.xlsx',
        'P19471_Staph_SEP_APAF_MS_SWATH_Result_extendedLib_V1.xlsx',
        'P19471_Staph_SEP_APAF_MS_SWATH_Result_LocalLib_V1.xlsx',
    ]

    for filename in filenames:
        assert(proteomics_swathms_msresult_filename_re.match(filename) is not None)


def test_proteomics_2dlibrary():
    filenames = [
        'BPH2760_Pooled sera_Fraction7_SEP_MBPF_MS_QEPlus1_P16_0062_Exp22.raw',
        'BPH2760_RPMI_Fraction2_SEP_MBPF_MS_QEPlus1_P16_0062_Exp22.raw',
        'B36_Serum_Fraction1_SEP_MBPF_MS_QEPlus1_P16_0062_Exp23.raw',
        'MS14387_RPMI_Fraction3_SEP_MBPF_MS_QEPlus1_P16_0062_Exp23.raw',
        'Sp180-15_PooledSera_Fraction9_SEP_MBPF_MS_QEPlus2_P16_0062_Exp25.raw',
        'Sp4559_RPMIGalactose_Fraction0_SEP_MBPF_MS_QEPlus2_P16_0062_Exp25.raw',
        'Sp947_RPMIGlucose_Fraction11_SEP_MBPF_MS_QEPlus2_P16_0062_Exp25.raw',
    ]
    for filename in filenames:
        assert(proteomics_2dlibrary_filename_re.match(filename) is not None)
