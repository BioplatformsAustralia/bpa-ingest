from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata
from ...bpa import BPA_ORGANIZATION_ID

logger = make_logger(__name__)


class MelanomaMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/melanoma/tracking/'
    auth = ('melanoma', 'melanoma')
    parent_organization = BPA_ORGANIZATION_ID

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

    def get_organization(self):
        # Markdown
        desc = """
The Melanoma Genomics Project aims to whole genome sequence approximately 500 melanoma patients.

The samples sequenced include brain, lymph, primary and metastatic tumours as well as cell line derived samples. The following coverage levels were generated for each sample:

 - 60x (or greater) for tumour samples
 - 40x for cell line samples
 - 40x for control blood samples

Partner organisations include:

 - Melanoma Institute Australia
 - John Curtin School of Medical Research, ANU
 - Berghoffer Queensland Institute of Medical Research
 - University of Sydney
 - Harry Perkins Institute of Medical Research
 - Peter MacCallum Cancer Centre
 - Ludwig Institute for Cancer Research
 - University of Queensland
 - Cancer Council NSW

Note, the sequence information in this repository is only part of the total number of genomes sequenced (the remainder is available upon request).

Due to ethics requirements this data is only available upon request to the collaborators by authenticated researchers. It is anticipated this data will also be made available through the International Cancer Genome Consortium.

For more information please visit:
http://www.bioplatforms.com/melanoma/
        """
        return {
            'name': 'bpa-melanoma',
            'title': 'Melanoma',
            'display_name': 'Melanoma',
            'image_url': 'https://data.bioplatforms.com/melanoma.png',
            'description': desc
        }

    def get_packages(self):
        return []

    def get_resources(self):
        return []
