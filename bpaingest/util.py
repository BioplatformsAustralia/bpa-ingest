import logging
import string
import os

import ckanapi
import csv
import requests

from collections import namedtuple


def bpa_id_to_ckan_name(bpa_id, suborg=None):
    r = 'bpa-'
    if suborg is not None:
        r += suborg + '-'
    r += bpa_id.replace('/', '_').replace('.', '_')
    return r


def prune_dict(d, keys):
    if d is None:
        return None
    return dict((k, v) for (k, v) in d.items() if k in keys)


def clean_tag_name(s):
    "reduce s to strings acceptable in a tag name"
    return ''.join(t for t in s if t in string.digits or t in string.ascii_letters or t in '-_.')


def make_registration_decorator():
    """
    returns a (decorator, list). any function decorated with
    the returned decorator will be appended to the list
    """
    registered = []

    def _register(fn):
        registered.append(fn)
        return fn

    return _register, registered


def make_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s [%(levelname)-7s] [%(threadName)s]  %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger


def make_ckan_api(args):
    ckan = ckanapi.RemoteCKAN(args.ckan_url, apikey=args.api_key)
    return ckan


CKAN_AUTH = {
    'login': 'CKAN_USERNAME',
    'password': 'CKAN_PASSWORD'
}


# http://stackoverflow.com/questions/38271351/download-resources-from-private-ckan-datasets
def authenticated_ckan_session(ckan):
    s = requests.Session()
    data = dict((k, os.environ.get(v)) for k, v in CKAN_AUTH.items())
    if any(t is None for t in data.values()):
        raise Exception('please set %s' % (', '.join(CKAN_AUTH.values())))
    url = ckan.address + '/login_generic'
    r = s.post(url, data=data)
    if 'field-login' in r.text:
        raise RuntimeError('Login failed.')
    return s


digit_words = {
    '0': 'zero',
    '1': 'one',
    '2': 'two',
    '3': 'three',
    '4': 'four',
    '5': 'five',
    '6': 'six',
    '7': 'seven',
    '8': 'eight',
    '9': 'nine',
}


def csv_to_named_tuple(typname, fname):
    def clean_name(s):
        s = s.lower().strip().replace('-', '_').replace(' ', '_')
        s = ''.join([t for t in s if t in string.ascii_letters or t in string.digits or t == '_'])
        if s[0] in string.digits:
            s = digit_words[s[0]] + s[1:]
        s = s.strip('_')
        return s
    with open(fname) as fd:
        r = csv.reader(fd)
        header = [clean_name(t) for t in next(r)]
        typ = namedtuple(typname, header)
        rows = []
        for row in r:
            rows.append(typ(*row))
        return header, rows
