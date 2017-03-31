# _*_ coding: utf-8 _*_

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
    },
    {
        'name': 'bpa-barcode',
        'title': 'Barcode',
        'display_name': 'Barcode',
        'image_url': 'https://data.bioplatforms.com/barcode.png',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-base',
        'title': 'Biome of Australia Soil Environments',
        'display_name': 'Biome of Australia Soil Environments',
        'image_url': 'https://data.bioplatforms.com/base.png',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-great-barrier-reef',
        'title': 'Great Barrier Reef',
        'display_name': 'Great Barrier Reef',
        'image_url': 'https://data.bioplatforms.com/coral.png',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-melanoma',
        'title': 'Melanoma',
        'display_name': 'Melanoma',
        'image_url': 'https://data.bioplatforms.com/melanoma.png',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-marine-microbes',
        'title': 'Marine Microbes',
        'display_name': 'Marine Microbes',
        'image_url': 'https://data.bioplatforms.com/tricho.jpg',
        'display_name': 'Marine Microbes',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-sepsis',
        'title': 'Antibiotic Resistant Sepsis Pathogens',
        'display_name': 'Antibiotic Resistant Sepsis Pathogens',
        'image_url': 'https://data.bioplatforms.com/sepsis.png',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-stemcells',
        'title': 'Stemcells',
        'display_name': 'Stemcells',
        'image_url': 'https://data.bioplatforms.com/stemcell.png',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-wheat-cultivars',
        'title': 'Wheat Cultivars',
        'display_name': 'Wheat Cultivars',
        'image_url': 'https://data.bioplatforms.com/wheat.png',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-wheat-pathogens-genomes',
        'title': 'Wheat Pathogens Genomes',
        'display_name': 'Wheat Pathogens Genomes',
        'image_url': 'https://data.bioplatforms.com/stagonospora_nodorum.png',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-wheat-pathogens-transcript',
        'title': 'Wheat Pathogens Transcript',
        'display_name': 'Wheat Pathogens Transcript',
        'image_url': 'https://data.bioplatforms.com/wheat.png',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    },
    {
        'name': 'bpa-omg',
        'title': 'Oz Mammals Genome Initiative',
        'display_name': 'Oz Mammals Genome Initiative',
        'image_url': 'https://s3-ap-southeast-2.amazonaws.com/bpa-web-assets/Dasyurus_viverrinus_Taranna_AnnaMacDonald_CCBY.1200x1200.jpeg',
        'groups': [{'capacity': 'public', 'name': BPA_ORGANIZATION_ID}],
    }
]
