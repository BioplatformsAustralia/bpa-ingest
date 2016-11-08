from urlparse import urljoin

MIRROR_BASE = 'https://downloads-qcif.bioplatforms.com/'


def bpa_mirror_url(path):
    return urljoin(MIRROR_BASE, path)
