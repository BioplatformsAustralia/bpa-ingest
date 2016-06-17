from .ops import update_or_create

BPA_ID = 'bioplatforms-australia'


def create_bpa(ckan):
    # create or update the BPA organisation
    bpa = {
        u'display_name': u'BioPlatforms Australia',
        u'name': BPA_ID,
        u'image_display_url': u'http://www.bioplatforms.com/wp-content/uploads/BioplatformsAustralia.png',
        u'image_url': u'http://www.bioplatforms.com/wp-content/uploads/BioplatformsAustralia.png',
        u'title': u'BioPlatforms Australia',
        u'approval_status': u'approved',
        u'is_organization': True,
        u'state': u'active',
        u'type': u'organization',
        u'description': u'Bioplatforms Australia enables innovation and collaboration through investments in world class infrastructure and expertise.'
    }
    update_or_create(ckan, "organization", bpa)
