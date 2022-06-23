from ...util import make_logger
import re


logger = make_logger(__name__)


ILLUMINA_SHORTREAD_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    GAP_NGS_
    (?P<facility_id>(AGRF))_
    (?P<flow_cell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<runsamplenum>S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])_
    001
    \.fastq\.gz$
"""
illumina_shortread_re = re.compile(ILLUMINA_SHORTREAD_PATTERN, re.VERBOSE)

ILLUMINA__RNA_AND_PHYLO_SHORTREAD_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    LibID(?P<library_id>\d{4,6})_
    (GAP_
    (?P<facility_id>(BRF|UNSW))_)?
    (?P<flow_cell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]{8,12}([_-][G|A|T|C|-]{8,12})?)_
    (?P<runsamplenum>S?\d*)_?
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])
    (_001|)
    \.fastq\.gz$
"""

illumina_shortread_rna_phylo_re = re.compile(
    ILLUMINA__RNA_AND_PHYLO_SHORTREAD_PATTERN, re.VERBOSE
)

ONT_MINION_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    (?P<flow_cell_id>FA[KL]\d{5})_
    GAP_
    (?P<facility_id>(AGRF))_
    ONTMinion_
    (?P<archive_type>\w+)
    \.tar
"""
ont_minion_re = re.compile(ONT_MINION_PATTERN, re.VERBOSE)

ONT_PROMETHION_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    (?P<flow_cell_id>PA[DEFGH]\d{5})_
    GAP_
    (?P<facility_id>(AGRF|BRF))_
    (Run(?P<run_number>\d+)_)?
    ONTPromethION_
    (?P<archive_type>\w+)
    \.(tar|html|txt)
"""
ont_promethion_re = re.compile(ONT_PROMETHION_PATTERN, re.VERBOSE)

ONT_PROMETHION_PATTERN_2 = r"""
    (?P<sample_id>\d{4,6})_
    GAP_
    (?P<facility_id>(AGRF|BRF|UNSW))_
    (?P<flow_cell_id>PA[DEFGK]\d{5})_
    (Run(?P<run_number>\d+)_)?
    ONTPromethION_
    (\w+_)?
    (?P<archive_type>\w+)
    \.(tar|fastq.gz|blow5)
"""
ont_promethion_re_2 = re.compile(ONT_PROMETHION_PATTERN_2, re.VERBOSE)

GENOMICS_10X_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    .*
    \.tar
"""
genomics_10x_re = re.compile(GENOMICS_10X_PATTERN, re.VERBOSE)

DDRAD_FASTQ_FILENAME_PATTERN = r"""
    (?P<dataset_id>\d{4,6})_
    (?P<flowcell_id>\w{9})_
    (?P<index>[G|A|T|C|-]*|N)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
"""
ddrad_fastq_filename_re = re.compile(DDRAD_FASTQ_FILENAME_PATTERN, re.VERBOSE)

DDRAD_METADATA_SHEET_PATTERN = r"""
    GAP_
    NGS_
    (?P<flowcell_id>\w{9})_
    library_metadata_
    (?P<dataset_id>\d{4,6})
    \.xlsx
"""
ddrad_metadata_sheet_re = re.compile(DDRAD_METADATA_SHEET_PATTERN, re.VERBOSE)

PACBIO_HIFI_PATTERN = r"""
    (?P<sample_id>\d{4,6})_
    GAP_
    (?P<facility>(GWA))_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    (_ccs_statistics\.csv
      |_final\.consensusreadset\.xml
      |\.ccs\.bam
      |[\._]subreads\.bam
      |_HiFi_qc\.pdf
      |\.xlsx
      |.*\.pdf)
"""
pacbio_hifi_filename_re = re.compile(PACBIO_HIFI_PATTERN, re.VERBOSE)

PACBIO_HIFI_METADATA_SHEET_PATTERN = r"""
    GAP_
    (?P<facility>(GWA))_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    (_metadata\.xlsx)
"""
pacbio_hifi_metadata_sheet_re = re.compile(
    PACBIO_HIFI_METADATA_SHEET_PATTERN, re.VERBOSE
)
