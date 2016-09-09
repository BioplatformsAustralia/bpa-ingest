from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class SepsisMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/sepsis/tracking/'

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

    def get_group(self):
        # Markdown
        desc = """
The Antibiotic Resistant Pathogens Framework Initiative aims to develop a framework dataset that will enable identification of core targets common to antibiotic-resistant sepsis pathogens. The project aims to use an integrated multi-omics approach and brings together genomics, transcriptomics, bioinformatics, proteomics and metabolomics expertise across the Bioplatforms Australia network. Five clinical strains of Escherichia coli, Klebsiella bn pneumoniae, Streptococcus pneumoniae, Staphylococcus aureus and Streptococcus pyogenes selected by members of the consortium will form the core of the project.

For more information please visit: http://www.bioplatforms.com/antibiotic-resistant-pathogens/
        """
        return {
            'name': 'sepsis',
            'title': 'Antibiotic Resistant Pathogens',
            'display_name': 'Antibiotic Resistant Pathogens',
            'image_url': 'https://data.bioplatforms.com/sepsis.png',
            'description': desc
        }

    def get_packages(self):
        return []

    def get_resources(self):
        return []
