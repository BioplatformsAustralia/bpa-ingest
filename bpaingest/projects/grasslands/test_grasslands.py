from .files import (
    illumina_shortread_rna_phylo_re,

)



def test_illumina_rna_and_phylo_shortread():
    filenames = [
        "369564_LibID371565_AG_BRF_HMMYTDRX3_TCAGCATC_S1_L001_R2_001.fastq.gz",
    ]
    for filename in filenames:
        assert illumina_shortread_rna_phylo_re.match(filename) is not None
