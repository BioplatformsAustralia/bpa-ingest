barcode_desc = """
Of the estimated 10 million species that exist on our planet, only just over a million have so far been identified and described.

Using traditional taxonomy, it would take at least another 2000 years to identify Earth's remaining species. With DNA barcoding, we can vastly accelerate this rate of biodiversity discovery and conservation as well as generate significant scientific and economic benefits for the mining, fisheries and forestry industries.

The national collaborative project will focus on five key areas with immediate strategic value:

 - verifying timber origins to combat illegal timber trading
 - authenticating labelling and geographical origin of fish in the retail marketplace
 - mapping plant biodiversity in the Pilbara to help with mine site environmental impact assessment and restoration management
 - biodiversity discovery and impact assessment of invertebrates that inhabit underground aquifers utilised by mining and farming
 - generating barcodes for Australia's orchids to enhance conservation.

For more information please visit:
http://www.bioplatforms.com/dna-barcoding/
"""

base_desc = """
The Biome of Australia Soil Environments (BASE) is a collaborative project to create a public resource containing microbial genome information from a range of Australian soil environments.

Soil along with historical, physical and chemical contextual information (including photos) has been collected from 600+ diverse sites around Australia including Christmas Island and the Australian Antarctic Territory.

DNA is extracted from all samples using a standardised protocol (PowerSoil, MO BIO) and amplicon analysis undertaken using the Illumina MiSeq platform for bacterial 16S, fungal ITS and eukaryotic 18S targets.

OTU data has been generated through standardised pipelines for each target and is available through this repository.

For more information please visit: http://www.bioplatforms.com/soil-biodiversity
"""

gbr_desc = """
The Sea-quence Project is generating core genetic data for corals from the Great Barrier Reef and Red Sea to ultimately help guide reef management practices. The project aims to sequence the genomes of 10 coral species across 6 different coral types, 3-4 algal symbionts and generate a new suite of microbial symbiont sequence data. This project is an initiative of the ReFuGe 2020 Consortium. Membership of the consortium includes:

 - Great Barrier Reef Foundation
 - James Cook University
 - Australian Institute of Marine Science
 - University of Queensland
 - The Great Barrier Reef Marine Park Authority
 - King Abdullah University of Science and Technology (Saudi Arabia)
 - Australian National University
 - Bioplatforms Australia

For more information please visit: http://www.bioplatforms.com/great-barrier-reef/
"""

melanoma_desc = """
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

mm_desc = """
The Marine Microbes project will establish how Australia's marine microbial communities change over time in various locations and environments. The consortium of researchers will investigate the microbial communities of seawater, sediment, sponges and sea grass utilising the extensive capability of Australia's Integrated Marine Observing System (IMOS).

For more information please visit: http://www.bioplatforms.com/marine-microbes/
"""

sepsis_desc = """
The Antibiotic Resistant Pathogens Framework Initiative aims to develop a framework dataset that will enable identification of core targets common to antibiotic-resistant sepsis pathogens. The project aims to use an integrated multi-omics approach and brings together genomics, transcriptomics, bioinformatics, proteomics and metabolomics expertise across the Bioplatforms Australia network. Five clinical strains of Escherichia coli, Klebsiella bn pneumoniae, Streptococcus pneumoniae, Staphylococcus aureus and Streptococcus pyogenes selected by members of the consortium will form the core of the project.

For more information please visit: http://www.bioplatforms.com/antibiotic-resistant-pathogens/
"""

stemcell_desc = """
Stem cells allow us to study fundamental processes in tissue growth, development, aging and disease. The next waves of medicine will build on personalised therapies for drug treatments that require an understanding of drug-genetic-tissue interactions. Stem cell programs that can direct the differentiation of cells lead to the assembly of mini-organs in a dish that are already in use for disease screening in Australia, and around the world.

http://www.bioplatforms.com/stem-cells/
"""

wheat_cultivars_desc = """
This dataset contains genomic sequence information from 16 wheat varieties of importance to Australia selected and prioritised by the major stakeholders of the Australian grains research community based on availability of mapping populations and genetic diversity.

Wheat leaf and/or root tissue samples where sequenced to generate approximately 10x coverage of each variety's genome.

Partner organisations include:

 - Australian Centre for Plant Functional Genomics
 - CSIRO
 - Victorian Department of Environment and Primary Industries
 - Murdoch University
 - University of Queensland
 - Grains Research and Development Corporation

For more information please visit: http://www.bioplatforms.com/wheat-sequencing/
"""

wheat_path_genomes_desc = """
This dataset contains the genomic sequence from 10 fungal and 2 bacterial pathogen species. Among the pathogens sequenced are the causal agents of stripe rust, stem rust, tan spot, glume blotch, septoria leaf blotch, bare patch and crown rot/head blight. A total of 27 genomes will be made available.

The genomes were selected for analysis by a consortium of Australian wheat pathogen researchers from the following organisations:

 - Australian National University
 - CSIRO
 - Curtin University
 - Charles Sturt University
 - NSW Department of Primary industry
 - Grains Research and Development Corporation

For more information please visit: http://www.bioplatforms.com/wheat-defense/
"""

wheat_path_transcript_desc = """
This dataset contains transcript sequence data from 8 different fungal pathogen species of wheat.

The samples analysed cover various developmental stages of the pathogens and their interaction with the wheat host.

The pathogens included are the causal agents of stripe rust, stem rust, tan spot, glume blotch, bare patch and crown rot/head blight.

The data for generation was prioritised by a consortium of Australian wheat pathogen researchers from the following organisations:

 - Australian National University
 - CSIRO
 - Curtin University
 - Charles Sturt University
 - NSW Department of Primary industry

For more information please visit: http://www.bioplatforms.com/wheat-defense/
"""

BPA_ORGANIZATION_ID = 'bioplatforms-australia'

ORGANIZATIONS = [
    {
        'display_name': 'BioPlatforms Australia',
        'name': BPA_ORGANIZATION_ID,
        'image_display_url': 'https://data.bioplatforms.com/bpalogo.png',
        'image_url': 'https://data.bioplatforms.com/bpalogo.png',
        'title': 'BioPlatforms Australia',
        'approval_status': 'approved',
        'is_organization': True,
        'state': 'active',
        'type': 'organization',
        'description': 'Bioplatforms Australia enables innovation and collaboration through investments in world class infrastructure and expertise.'
    },
    {
        'name': 'bpa-barcode',
        'title': 'Barcode',
        'display_name': 'Barcode',
        'image_url': 'https://data.bioplatforms.com/barcode.png',
        'description': barcode_desc,
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-base',
        'title': 'BASE',
        'display_name': 'Biome of Australian Soil Environments',
        'image_url': 'https://data.bioplatforms.com/base.png',
        'description': base_desc,
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-great-barrier-reef',
        'title': 'Great Barrier Reef',
        'display_name': 'Great Barrier Reef',
        'image_url': 'https://data.bioplatforms.com/coral.png',
        'description': gbr_desc,
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-melanoma',
        'title': 'Melanoma',
        'display_name': 'Melanoma',
        'image_url': 'https://data.bioplatforms.com/melanoma.png',
        'description': melanoma_desc,
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-marine-microbes',
        'title': 'Marine Microbes',
        'display_name': 'Marine Microbes',
        'image_url': 'https://data.bioplatforms.com/marine_microbes.png',
        'description': mm_desc,
        'display_name': 'Marine Microbes',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-sepsis',
        'title': 'Antibiotic Resistant Pathogens',
        'display_name': 'Antibiotic Resistant Pathogens',
        'image_url': 'https://data.bioplatforms.com/sepsis.png',
        'description': sepsis_desc,
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-stemcells',
        'title': 'Stemcells',
        'display_name': 'Stemcells',
        'image_url': 'https://data.bioplatforms.com/stemcell.png',
        'description': stemcell_desc,
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-wheat-cultivars',
        'title': 'Wheat Cultivars',
        'display_name': 'Wheat Cultivars',
        'image_url': 'https://downloads.bioplatforms.com/static/wheat_cultivars/wheat.png',
        'description': wheat_cultivars_desc,
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-wheat-pathogens-genomes',
        'title': 'Wheat Pathogens Genomes',
        'display_name': 'Wheat Pathogens Genomes',
        'image_url': 'https://data.bioplatforms.com/stagonospora_nodorum.png',
        'description': wheat_path_genomes_desc,
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-wheat-pathogens-transcript',
        'title': 'Wheat Pathogens Transcript',
        'display_name': 'Wheat Pathogens Transcript',
        'image_url': 'https://data.bioplatforms.com/wheat.png',
        'description': wheat_path_transcript_desc,
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    }
]
