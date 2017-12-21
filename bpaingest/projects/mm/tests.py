from .files import (
    amplicon_control_filename_re,
    amplicon_filename_re,
    transcriptome_filename_re,
    metatranscriptome_filename_re,
    metatranscriptome_filename2_re,
    metagenomics_filename_re,
    metagenomics_filename_v2_re)


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


def test_amplicon():
    filenames = [
        '21878_1_A16S_UNSW_GGACTCCT-TATCCTCT_AP3JE_S17_L001_R1.fastq.gz',
        '21644_1_16S_UNSW_GAACTAGTCACC_AFGB7_S61_L001_R1.fastq.gz',
        '27491_1_16S_UNSW_UNKNOWN_AHG7M_UNKNOWN_L001_R2.fastq.gz',
        '27160_1_16S_UNSW_UNKNOWN_AH55W_UNKNOWN_L001_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(amplicon_filename_re.match(filename) is not None)


def test_transcriptome():
    filenames = [
        '24708_PE_200bp_STEMCELLS_AGRF_HMHNFBCXX_CGATGT_L002_R2.fastq.gz',
        '29586_PE_200bp_STEMCELLS_AGRF_CAGCTANXX_CGATGT_L001_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(transcriptome_filename_re.match(filename) is not None)


def test_metatranscriptome():
    filenames = [
        '34957_1_wor_PE_200bp_MM_AGRF_CA5YNANXX_CAGATC_L003_R1.fastq.gz',
        '34955_1_wir_PE_200bp_MM_AGRF_CA5YNANXX_TTAGGC_L003_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(metatranscriptome_filename_re.match(filename) is not None)


def test_metatranscriptome2():
    filenames = [
        '57510_1_PE_210bp_MM_AGRF_CC26CANXX_CGATGT_L003_R1.fastq.gz',
        '57512_1_PE_210bp_MM_AGRF_CC26CANXX_CAGATC_L002_R2.fastq.gz',
        '57518_1_PE_210bp_MM_AGRF_CC26CANXX_GCCAAT_L003_R2.fastq.gz',
    ]
    for filename in filenames:
        assert(metatranscriptome_filename2_re.match(filename) is not None)


def test_metagenomics():
    filenames = [
        '21744_1_PE_700bp_MM_UNSW_HM7K2BCXX_AAGAGGCA-AAGGAGTA_L001_R1.fastq.gz',
        '34318_1_PE_680bp_MM_AGRF_H3KWTBCXY_CTCTCTAC-ACTGCATA_L002_R1.fastq.gz',
        '21730_1_PE_700bp_MM_UNSW_HL7NGBCXX_GTAGAGGA-ACTGCATA_L002_R1.fastq.gz',
    ]
    for filename in filenames:
        assert(metagenomics_filename_re.match(filename) is not None)


def test_metagenomics_v2():
    filenames = [
        '34734_1_PE_700bp_MM_UNSW_HMMJFBCXY_TAAGGCGA-CTCTCTAT_S5_L001_R2_001.fastq.gz',
        '36064_1_PE_700bp_MM_UNSW_HM35MBCXY_ACTGAGCG-GAGCCTTA_S13_L002_R1_001.fastq.gz',
    ]
    for filename in filenames:
        assert(metagenomics_filename_v2_re.match(filename) is not None)
