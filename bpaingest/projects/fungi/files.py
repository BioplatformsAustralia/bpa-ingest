from ...util import make_logger
import re


logger = make_logger(__name__)

ILLUMINA_SHORTREAD_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility_id>(AGRF))_
    (?P<flow_cell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])
    \.fastq\.gz$
"""
illumina_shortread_re = re.compile(ILLUMINA_SHORTREAD_PATTERN, re.VERBOSE)

ILLUMINA_FASTQ_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility_id>(UNSW|AGRF))_
    (?P<flowcell_id>\w{9,10})_
    (?P<index>[G|A|T|C|-]*)_
    ((?P<runsamplenum>S\d*)_)?
    ((?P<lane>L\d{3})_)?
    (?P<read>[R|I][1|2])
    (_001)?
    \.fastq\.gz$
"""
illumina_fastq_re = re.compile(ILLUMINA_FASTQ_PATTERN, re.VERBOSE)

PACBIO_HIFI_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility>(AGRF)|C?(AGRF)\d+)_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    (_ccs_statistics\.csv
      |_final\.consensusreadset\.xml
      |\.ccs\.bam
      |[\._]subreads\.bam
      |[\._]HiFi_qc\.pdf
      |\.pdf)
"""
pacbio_hifi_filename_re = re.compile(PACBIO_HIFI_PATTERN, re.VERBOSE)

PACBIO_HIFI_METADATA_SHEET_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility>(AGRF)|C?(AGRF)\d+)_
    (PacBio_)?
    (?P<flowcell_id>\w{8})
    ([\._]metadata\.xlsx)
"""
pacbio_hifi_metadata_sheet_re = re.compile(
    PACBIO_HIFI_METADATA_SHEET_PATTERN, re.VERBOSE
)

DDRAD_FASTQ_FILENAME_PATTERN = r"""
    (?P<bpa_dataset_id>\d{4,6})_
    (?P<flowcell_id>\w{9})_
    (?P<index>[G|A|T|C|-]*|N)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])\.fastq\.gz
"""
ddrad_fastq_filename_re = re.compile(DDRAD_FASTQ_FILENAME_PATTERN, re.VERBOSE)

DDRAD_METADATA_SHEET_PATTERN = r"""
    FUN_
    NGS_
    (?P<flowcell_id>\w{9})_
    library_metadata_
    (?P<bpa_dataset_id>\d{4,6})
    \.xlsx
"""
ddrad_metadata_sheet_re = re.compile(DDRAD_METADATA_SHEET_PATTERN, re.VERBOSE)

METADATA_SHEET_PATTERN = r"""
    FUN_
    (?P<facility_id>(UNSW))_
    (?P<flowcell_id>\w{9,10})_
    metadata.xlsx
"""
metadata_sheet_re = re.compile(METADATA_SHEET_PATTERN, re.VERBOSE)

GENOME_ASSEMBLY_PATTERN = r"""
    (?P<bioplatforms_secondarydata_id>\d{4,6})_
    .*
    \.
    fasta
"""
genome_assembly_filename_re = re.compile(GENOME_ASSEMBLY_PATTERN, re.VERBOSE)

ILLUMINA_HIC_PATTERN = r"""
    (?P<library_id>\d{4,6})_
    FUN_
    (?P<facility_id>(BRF))_
    (?P<flowcell_id>\w{5,10})_
    ((?P<index>[G|A|T|C|-]*)_)?
    (?P<runsamplenum>S\d*)_
    (?P<lane>L\d{3})_
    (?P<read>[R|I][1|2])_
    001
    \.fastq\.gz$
"""
illumina_hic_re = re.compile(ILLUMINA_HIC_PATTERN, re.VERBOSE)
