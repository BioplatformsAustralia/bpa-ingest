from __future__ import print_function

import argparse
import ckanapi
import sys

from .util import make_registration_decorator
from .bpa import create_bpa
from .wheat_cultivars.ingest import ingest as ingest_wheatcultivars
from .wheat_cultivars.download import download as download_wheatcultivars
from .wheat_pathogens.ingest import ingest as ingest_wheat_pathogens
from .wheat_pathogens.download import download as download_wheat_pathogens

register_command, command_fns = make_registration_decorator()


@register_command
def bootstrap(ckan, args):
    "bootstrap basic organisation data"
    create_bpa(ckan)


@register_command
def wheat_cultivars(ckan, args):
    "download and ingest wheat7a metadata"
    download_wheatcultivars(args.path, args.clean)
    ingest_wheatcultivars(ckan, args.path)


@register_command
def wheat_pathogens(ckan, args):
    "download and ingest wheat pathogen genome metadata"
    download_wheat_pathogens(args.path, args.clean)
    ingest_wheat_pathogens(ckan, args.path)


def setup_metadata_path(subparser):
    subparser.add_argument('path', help='path to metadata')
    subparser.add_argument('--clean', action='store_true', help='clean up path before run')
wheat_cultivars.setup = setup_metadata_path


def make_ckan_api(args):
    ckan = ckanapi.RemoteCKAN(args.ckan_url, apikey=args.api_key)
    return ckan


def version():
    import pkg_resources
    version = pkg_resources.require("wrfy")[0].version
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
    parser.add_argument(
        '--version', action='store_true',
        help='print version and exit')
    parser.add_argument(
        '-k', '--api-key', required=True,
        help='CKAN API Key')
    parser.add_argument(
        '-u', '--ckan-url', required=True,
        help='CKAN base url')

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
