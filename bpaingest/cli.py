from __future__ import print_function

import argparse
import ckanapi
import sys

from .util import make_registration_decorator
from .sync import sync_metadata
from .bpa import create_bpa
from .ops import print_accounts
from .genhash import genhash as genhash_fn

from .wheat_cultivars.ingest import WheatCultivarsMetadata
from .wheat_cultivars.download import download as download_wheatcultivars

from .wheat_pathogens.ingest import WheatPathogensMetadata
from .wheat_pathogens.download import download as download_wheat_pathogens

from .gbr_amplicon.ingest import GbrAmpliconMetadata
from .gbr_amplicon.download import download as download_gbr_amplicon

from .libs.fetch_data import get_password

register_command, command_fns = make_registration_decorator()


@register_command
def bootstrap(ckan, args):
    "bootstrap basic organisation data"
    create_bpa(ckan)


sync_handlers = {
    'wheat-cultivars': (download_wheatcultivars, WheatCultivarsMetadata, None),
    'wheat-pathogens': (download_wheat_pathogens, WheatPathogensMetadata, None),
    'gbr-amplicon': (download_gbr_amplicon, GbrAmpliconMetadata, lambda: ('bpa', get_password('gbr'))),
}


def setup_sync(subparser):
    subparser.add_argument('project_name', choices=sync_handlers.keys(), help='path to metadata')
    subparser.add_argument('path', help='path to metadata')
    subparser.add_argument('--clean', action='store_true', help='clean up path before run')
    subparser.add_argument('--uploads', type=int, default=4, help='number of parallel uploads')


def setup_hash(subparser):
    subparser.add_argument('project_name', choices=sync_handlers.keys(), help='path to metadata')
    subparser.add_argument('path', help='path to metadata')
    subparser.add_argument('mirror_path', help='path to locally mounted mirror')
    subparser.add_argument('--clean', action='store_true', help='clean up path before run')


@register_command
def sync(ckan, args):
    """sync a project"""
    dl_fn, meta_cls, auth_fn = sync_handlers[args.project_name]
    dl_fn(args.path, args.clean)
    meta = meta_cls(args.path)
    auth = None
    if auth_fn:
        auth = auth_fn()
    sync_metadata(ckan, meta, auth, args.uploads)
    print_accounts()

sync.setup = setup_sync


@register_command
def genhash(ckan, args):
    """
    verify MD5 sums for a local (filesystem mounted) mirror of the BPA
    data, and generate expected E-Tag and SHA256 values.
    """
    dl_fn, meta_cls, auth_fn = sync_handlers[args.project_name]
    dl_fn(args.path, args.clean)
    meta = meta_cls(args.path)
    genhash_fn(ckan, meta, args.mirror_path)
    print_accounts()

genhash.setup = setup_hash


def make_ckan_api(args):
    ckan = ckanapi.RemoteCKAN(args.ckan_url, apikey=args.api_key)
    return ckan


def version():
    import pkg_resources
    version = pkg_resources.require("bpaingest")[0].version
    print('''\
bpa-ingest, version %s

Copyright 2016 CCG, Murdoch University
License GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>.
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law.''' % (version))
    sys.exit(0)


def usage(parser):
    parser.print_usage()
    sys.exit(0)


def commands():
    for fn in command_fns:
        name = fn.__name__.replace('_', '-')
        yield name, fn, getattr(fn, 'setup', None), fn.__doc__


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='store_true', help='print version and exit')
    parser.add_argument('-k', '--api-key', required=True, help='CKAN API Key')
    parser.add_argument('-u', '--ckan-url', required=True, help='CKAN base url')

    subparsers = parser.add_subparsers(dest='name')
    for name, fn, setup_fn, help_text in sorted(commands()):
        subparser = subparsers.add_parser(name, help=help_text)
        subparser.set_defaults(func=fn)
        if setup_fn is not None:
            setup_fn(subparser)
    args = parser.parse_args()
    if args.version:
        version()
    if 'func' not in args:
        usage(parser)
    ckan = make_ckan_api(args)
    args.func(ckan, args)
