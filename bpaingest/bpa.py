from urlparse import urljoin

from .ops import make_organization, ckan_method

BPA_ORGANIZATION_ID = 'bioplatforms-australia'
MIRROR_BASE = 'https://downloads-qcif.bioplatforms.com/'


def bpa_mirror_url(path):
    return urljoin(MIRROR_BASE, path)


def create_bpa(ckan):
    # create or update the BPA organisation
    bpa = {
        u'display_name': u'BioPlatforms Australia',
        u'name': BPA_ORGANIZATION_ID,
        u'image_display_url': u'http://www.bioplatforms.com/wp-content/uploads/BioplatformsAustralia.png',
        u'image_url': u'http://www.bioplatforms.com/wp-content/uploads/BioplatformsAustralia.png',
        u'title': u'BioPlatforms Australia',
        u'approval_status': u'approved',
        u'is_organization': True,
        u'state': u'active',
        u'type': u'organization',
        u'description':
        u'Bioplatforms Australia enables innovation and collaboration through investments in world class infrastructure and expertise.'
    }
    make_organization(ckan, bpa)


def get_bpa(ckan):
    create_bpa(ckan)
    return ckan_method(ckan, 'organization', 'show')(id=BPA_ORGANIZATION_ID)
