from .files import (
    amd_metagenomics_analysed_re,
    amd_metagenomics_novaseq_re,
    amd_metagenomics_novaseq_control_re,
    amd_amplicon_filename_re,
    amd_amplicon_control_filename_re,
    base_amplicon_control_tech_vendor_filename_re,
    base_amplicon_control_tech_vendor_flow_filename_re,
    base_amplicon_filename_flow_index_swapped_re,
    base_amplicon_filename_re,
    base_amplicon_index2_filename_re,
    base_amplicon_index3_filename_re,
    base_metagenomics_filename_re,
    base_metagenomics_run_filename_re,
    base_site_image_filename_re,
    mm_amplicon_control_filename_re,
    mm_amplicon_filename_re,
    mm_metagenomics_filename_re,
    mm_metagenomics_filename_v2_re,
    mm_metatranscriptome_filename2_re,
    mm_metatranscriptome_filename_re,
    mm_transcriptome_filename_re,
)


def test_base_amplicon_control():
    filenames = [
        "Arch_mock_community_A16S_UNSW_ATCTCAGG_AAGGCTAT_AHFYA_S72_L001_I2.fastq.gz",
        "Arc_mock_community_A16S_UNSW_ATCTCAGG_AAGGCTAT_AE4DM_S72_L001_R2.fastq.gz",
        "Arc_mock_community_A16S_UNSW_ATCTCAGG_AH3G1_S72_L001_R2.fastq.gz",
        "Arc_mock_community_A16S_UNSW_GTAGAGGA_AAGGAGTA_AC9FT_S84_L001_I2.fastq.gz",
        "Fungal_mock_community_18S_AGRF_ACGTAGCATTTC_A89CY_S96_L001_R1.fastq.gz",
        "Fungal-mock-community_ITS_AGRF_CCAAGTCTTACA_AN5TK_AN5TK_CCAAGTCTTACA_L001_R1.fastq.gz",
        "NEG_1_16S_AGRF_GGAGACAAGGGA_A5K1H_S1_L001_I1.fastq.gz",
        "Soil_DNA_16S_AGRF_CCACCTACTCCA_A815N_S94_L001_I1.fastq.gz",
        "Soil_DNA_18S_AGRF_TGTGCTGTGTAG_ANC6T_ANC6T_TGTGCTGTGTAG_L001_R1.fastq.gz",
        "Soil_DNA_A16S_AGRF_GTAGAGGA_GTAAGGAG_AEMV2_S60_L001_I1.fastq.gz",
        "Soil_DNA_A16S_UNSW_ATCTCAGG_AH62P_S84_L001_I1.fastq.gz",
        "Soil_DNA_A16S_UNSW_GTAGAGGA_ACTGCATA_AC9FT_S72_L001_R2.fastq.gz",
        "Soil-DNA_ITS_AGRF_ACACTAGATCCG_AN5TK_AN5TK_ACACTAGATCCG_L001_R2.fastq.gz",
    ]
    for filename in filenames:
        assert base_amplicon_control_tech_vendor_filename_re.match(filename) is not None


def test_base_amplicon_control2():
    filenames = [
        "Bac_mock_community_16S_AGRF_B3BDY_AGATGTTCTGCT_L001_R2.fastq.gz",
        "Fungal__mock_Community_ITS_AGRF_B39G7_ATGGACCGAACC_L001_R2.fastq.gz",
        "Soil_DNA_16S_AGRF_B3C7L_CAAGCATGCCTA_L001_R1.fastq.gz",
        "NEG1_16S_AGRF_B3BDY_GAATAGAGCCAA_L001_R1.fastq.gz",
        "Neg1_16S_AGRF_B3L2P_CAGCGGTGACAT_L001_R1.fastq.gz",
        "NEG_2_18S_AGRF_B3C5P_CCATTCGCCCAT_L001_R2.fastq.gz",
        "NEG2_16S_AGRF_B3BDY_GTACGTGGGATC_L001_R2.fastq.gz",
        "Neg2_ITS_AGRF_B3C4H_AGAGCCTACGTT_L001_R2.fastq.gz",
        "Undetermined_16S_AGRF_A5K1H_S0_L001_R2.fastq.gz",
    ]
    for filename in filenames:
        assert (
            base_amplicon_control_tech_vendor_flow_filename_re.match(filename)
            is not None
        )


def test_base_amplicon():
    filenames = [
        "15984_1_ITS_UNSW_ACTATTGTCACG_AGEDA_S71_L001_R2.fastq.gz",
        "9504_1_16S_AGRF_AATGCCTCAACT_A5K1H_S59_L001_R2.fastq.gz",
        "8101_1_ITS_UNSW_TCGTCGATAATC_A64JJ_S3_L001_I1.fastq.gz",
        "39254_1_ITS_UNSW_CTCGAGAGGCTCTAGT_BC267_S73_L001_I2.fastq.gz",
        "42198_1_A16S_UNSW_TACGCTGC-TATCCTCT_B8RGF_S102_L001_R2.fastq.gz",
        "7033_1_18S_UNSW_TCTTCCGCTACT_A6BRJ_S23_L001_R1.fastq.gz",
    ]
    for filename in filenames:
        assert base_amplicon_filename_re.match(filename) is not None


def test_base_amplicon_flow_index_swapped():
    filenames = [
        "19621_1_18S_AGRF_B3C5P_CCTTAAGTCAGT_L001_R1.fastq.gz",
        "19569_1_18S_AGRF_B3C5P_CTCCTGAAAGTT_L001_R1.fastq.gz",
    ]
    for filename in filenames:
        assert base_amplicon_filename_flow_index_swapped_re.match(filename) is not None


def test_base_amplicon_index2():
    filenames = [
        "19418_1_ITS_AGRF_GTCCGAAACACT_ANVM7_ANVM7_GTCCGAAACACT_L001_R1.fastq.gz"
    ]
    for filename in filenames:
        assert base_amplicon_index2_filename_re.match(filename) is not None


def test_base_amplicon_index3():
    filenames = ["13392_1_A16S_UNSW_TAGGCATG_GTAAGGAG_ACG8D_S30_L001_I1.fastq.gz"]
    for filename in filenames:
        assert base_amplicon_index3_filename_re.match(filename) is not None


def test_base_metagenomics():
    filenames = [
        "10718_2_PE_550bp_BASE_AGRF_HFLF3BCXX_ATTACTCG-CCTATCCT_L002_R2.fastq.gz",
    ]
    for filename in filenames:
        assert base_metagenomics_filename_re.match(filename) is not None


def test_base_metagenomics_run():
    filenames = [
        "12450_1_PE_550bp_BASE_UNSW_HCLVFBCXX_ATTCAGAA-CCTATCCT_L001_R2_001.fastq.gz",
    ]
    for filename in filenames:
        assert base_metagenomics_run_filename_re.match(filename) is not None


def test_base_site_image():
    filenames = ["7075-7076.jpg", "19233-19234.jpg"]
    for filename in filenames:
        assert base_site_image_filename_re.match(filename) is not None


def test_mm_amplicon_control():
    filenames = [
        "Arc_mock_community_1_A16S_UNSW_CGATCAGT-CCTAGAGT_ARVTL_S105_L001_R2.fastq.gz",
        "Bac_mock_community_16S_UNSW_GGATCGCA-CTAGTATG_AUWLK_S124_L001_I2.fastq.gz",
        "Fungal_mock_community_18S_UNSW_CGAGGCTG-AAGGCTAT_APK6N_S105_L001_I2.fastq.gz",
        "Soil_DNA_16S_UNSW_CAGCTAGA-GATAGCGT_AYBVB_S110_L001_I1.fastq.gz",
        "STAN_16S_UNSW_TATCAGGTGTGC_AL1HY_S97_L001_R1.fastq.gz",
        "Bac_mock_community6S_UNSW_GACTCTTG-ACGACGTG_BJT3V_S60_L001_I1.fastq.gz",
        "Soil_DNA6S_UNSW_GACTCTTG-GTCTAGTG_BJT3V_S75_L001_I1.fastq.gz",
    ]
    for filename in filenames:
        assert mm_amplicon_control_filename_re.match(filename) is not None


def test_mm_amplicon():
    filenames = [
        "21878_1_A16S_UNSW_GGACTCCT-TATCCTCT_AP3JE_S17_L001_R1.fastq.gz",
        "21644_1_16S_UNSW_GAACTAGTCACC_AFGB7_S61_L001_R1.fastq.gz",
        "27491_1_16S_UNSW_UNKNOWN_AHG7M_UNKNOWN_L001_R2.fastq.gz",
        "27160_1_16S_UNSW_UNKNOWN_AH55W_UNKNOWN_L001_R1.fastq.gz",
        "37249_18S_UNSW_GCTCATGA-ACTGCATA_BFY6H_S52_L001_I1.fastq.gz",
    ]
    for filename in filenames:
        assert mm_amplicon_filename_re.match(filename) is not None


def test_amd_amplicon_re():
    filenames = [
        "139713_16S_J6HNJ_CGATCCGT-CGATCTAC_S36_L001_I1.fastq.gz",
        "139714_A16_J8H8P_ACTCGCTA-CTCTCTAT_S4_L001_I1.fastq.gz",
        "139714_A16S_J8H8P_ACTCGCTA-CTCTCTAT_S4_L001_I1.fastq.gz",
        "138620_ITS_J9GNL_AATGTCCG-GCTCTAGT_S52_L001_R1.fastq.gz",
    ]
    for filename in filenames:
        assert amd_amplicon_filename_re.match(filename) is not None


def test_amd_amplicon_control_re():
    filenames = [
        "ATCC1002MOCK_16S_J6H7B_TCCGAATT-TCTACACT_S2_L001_R2.fastq.gz",
        "Arc_mock_community_A16_J8H8P_TCGACGTC-CTAAGCCT_S1_L001_I1.fastq.gz",
        "No_Template_Control_16S_J6H5P_ACGCCACG-TCTACACT_S3_L001_R2.fastq.gz",
        "No_Template_Control_A16_J8H8P_TCGACGTC-TCTCTCCG_S3_L001_I1.fastq.gz",
        "Soil_DNA_16S_J6HNK_ACGCCACG-GATAGCGT_S1_L001_I1.fastq.gz",
        "blank_16S_J655F_AAGAGATG-TCTACACT_S29_L001_I1.fastq.gz",
        "Fungal_mock_community_ITS_J9GNL_AATGTCCG-GACACTGA_S1_L001_R2.fastq.gz",
        "No_Template_Control_ITS_J9GNL_AATGTCCG-TAGTGTAG_S3_L001_R2.fastq.gz",
        "Soil_DNA_ITS_J9GNL_AATGTCCG-TGCGTACG_S2_L001_I1.fastq.gz",
        "NEG_16S_K9276_ATTCCTGT-ACGACGTG_S56_L001_I1.fastq.gz",
    ]
    for filename in filenames:
        assert amd_amplicon_control_filename_re.match(filename) is not None


def test_mm_transcriptome():
    filenames = [
        "24708_PE_200bp_STEMCELLS_AGRF_HMHNFBCXX_CGATGT_L002_R2.fastq.gz",
        "29586_PE_200bp_STEMCELLS_AGRF_CAGCTANXX_CGATGT_L001_R1.fastq.gz",
    ]
    for filename in filenames:
        assert mm_transcriptome_filename_re.match(filename) is not None


def test_metatranscriptome():
    filenames = [
        "34957_1_wor_PE_200bp_MM_AGRF_CA5YNANXX_CAGATC_L003_R1.fastq.gz",
        "34955_1_wir_PE_200bp_MM_AGRF_CA5YNANXX_TTAGGC_L003_R1.fastq.gz",
    ]
    for filename in filenames:
        assert mm_metatranscriptome_filename_re.match(filename) is not None


def test_mm_metatranscriptome2():
    filenames = [
        "57510_1_PE_210bp_MM_AGRF_CC26CANXX_CGATGT_L003_R1.fastq.gz",
        "57512_1_PE_210bp_MM_AGRF_CC26CANXX_CAGATC_L002_R2.fastq.gz",
        "57518_1_PE_210bp_MM_AGRF_CC26CANXX_GCCAAT_L003_R2.fastq.gz",
    ]
    for filename in filenames:
        assert mm_metatranscriptome_filename2_re.match(filename) is not None


def test_mm_metagenomics():
    filenames = [
        "21744_1_PE_700bp_MM_UNSW_HM7K2BCXX_AAGAGGCA-AAGGAGTA_L001_R1.fastq.gz",
        "34318_1_PE_680bp_MM_AGRF_H3KWTBCXY_CTCTCTAC-ACTGCATA_L002_R1.fastq.gz",
        "21730_1_PE_700bp_MM_UNSW_HL7NGBCXX_GTAGAGGA-ACTGCATA_L002_R1.fastq.gz",
    ]
    for filename in filenames:
        assert mm_metagenomics_filename_re.match(filename) is not None


def test_mm_metagenomics_v2():
    filenames = [
        "34734_1_PE_700bp_MM_UNSW_HMMJFBCXY_TAAGGCGA-CTCTCTAT_S5_L001_R2_001.fastq.gz",
        "36064_1_PE_700bp_MM_UNSW_HM35MBCXY_ACTGAGCG-GAGCCTTA_S13_L002_R1_001.fastq.gz",
    ]
    for filename in filenames:
        assert mm_metagenomics_filename_v2_re.match(filename) is not None


def test_amd_metagenomics_analysed():
    filenames = [
        "21645_MGSD_CSIRO_bam.md5",
        "21645_MGSD_CSIRO_bins.zip",
        "21645_MGSD_CSIRO_combined_merged.fastq.gz",
        "21645_MGSD_CSIRO_combined_R1p.fastq.gz",
        "21645_MGSD_CSIRO_combined_R1R2u.fastq.gz",
        "21645_MGSD_CSIRO_combined_R2p.fastq.gz",
        "21645_MGSD_CSIRO_QCreads.md5",
        "21645_MGSD_CSIRO.sorted.bam",
        "21645_MGSD_CSIRO_SQM_01.fasta",
        "21645_MGSD_CSIRO_SQM_01.lon",
        "21645_MGSD_CSIRO_SQM_01.stats",
        "21645_MGSD_CSIRO_SQM_02.16S.txt",
        "21645_MGSD_CSIRO_SQM_02.maskedrna.fasta",
        "21645_MGSD_CSIRO_SQM_02.rnas",
        "21645_MGSD_CSIRO_SQM_02.trnas",
        "21645_MGSD_CSIRO_SQM_02.trnas.fasta",
        "21645_MGSD_CSIRO_SQM_03.faa",
        "21645_MGSD_CSIRO_SQM_03.fna",
        "21645_MGSD_CSIRO_SQM_03.gff",
        "21645_MGSD_CSIRO_SQM_06.fun3.tax.noidfilter.wranks",
        "21645_MGSD_CSIRO_SQM_06.fun3.tax.wranks",
        "21645_MGSD_CSIRO_SQM_07.fun3.cog",
        "21645_MGSD_CSIRO_SQM_07.fun3.kegg",
        "21645_MGSD_CSIRO_SQM_07.fun3.pfam",
        "21645_MGSD_CSIRO_SQM_10.contigcov",
        "21645_MGSD_CSIRO_SQM_10.mapcount",
        "21645_MGSD_CSIRO_SQM_10.mappingstat",
        "21645_MGSD_CSIRO_SQM_11.mcount",
        "21645_MGSD_CSIRO_SQM_12.cog.funcover",
        "21645_MGSD_CSIRO_SQM_12.kegg.funcover",
        "21645_MGSD_CSIRO_SQM_13.orftable",
        "21645_MGSD_CSIRO_SQM_18.DASTool.checkM",
        "21645_MGSD_CSIRO_SQM_19.bintable",
        "21645_MGSD_CSIRO_SQM_20.contigtable",
        "21645_MGSD_CSIRO_SQM_21.kegg.pathways",
        "21645_MGSD_CSIRO_SQM_21.metacyc.pathways",
        "21645_MGSD_CSIRO_SQM_22.stats",
        "21645_MGSD_CSIRO_SQM.md5",
        "21645_MGSD_CSIRO_SQMreads.md5",
        "21645_MGSD_CSIRO_sqm_reads.out.allreads",
        "21645_MGSD_CSIRO_sqm_reads.out.allreads.funcog",
        "21645_MGSD_CSIRO_sqm_reads.out.allreads.funkegg",
        "21645_MGSD_CSIRO_sqm_reads.out.allreads.mcount",
        "21645_MGSD_CSIRO_sqm_reads.out.mappingstat",
    ]
    for filename in filenames:
        assert amd_metagenomics_analysed_re.match(filename) is not None


def test_amd_metagenomics_novaseq():
    filenames = [
        "139811_MGE_HYTFVDSXX_AACGAGGCCG-ATACCTGGAT_S161_L001_R2_001.fastq.gz",
        "138600_MGE_UNSW_HG7NLDSX2_GAAGACTAGA-ACTAGAACTT_S38_L004_R1_001.fastq.gz",
    ]
    for filename in filenames:
        assert amd_metagenomics_novaseq_re.match(filename) is not None


def test_amd_metagenomics_novaseq_control():
    filenames = [
        "SOIL_DNA_MGE_HYTFVDSXX-TATCACTCTG-AACGTTACAT_S134_L002_R1_001.fastq.gz",
        "SOIL_DNA_MGE_HYTFVDSXX-TATCACTCTG-AACGTTACAT_S134_L002_R1_001.fastq.gz",
        "Soil_DNA_MGE_HTW7LDRXX_TTAACGGTGT-ACGGTCAGGA_S37_L002_R2_001.fastq.gz",
        "SOIL_MOCK_MGE_UNSW_HG7NLDSX2_TATCACTCTG-AACGTTACAT_S101_L004_R1_001.fastq.gz",
    ]
    for filename in filenames:
        assert amd_metagenomics_novaseq_control_re.match(filename) is not None
