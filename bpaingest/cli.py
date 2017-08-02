

import argparse
import sys
import os

from .util import make_registration_decorator, make_ckan_api
from .sync import sync_metadata
from .schema import generate_schemas
from .ops import print_accounts, make_organization
from .dump import dump_state
from .util import make_logger
from .genhash import genhash as genhash_fn
from .projects import PROJECTS
from .organizations import ORGANIZATIONS
from .metadata import DownloadMetadata


logger = make_logger(__name__)
register_command, command_fns = make_registration_decorator()


@register_command
def bootstrap(args):
    "bootstrap basic organisation data"
    ckan = make_ckan_api(args)
    for organization in ORGANIZATIONS:
        make_organization(ckan, organization)


def setup_ckan(subparser):
    subparser.add_argument('-k', '--api-key', required=True, help='CKAN API Key')
    subparser.add_argument('-u', '--ckan-url', required=True, help='CKAN base url')


def setup_sync(subparser):
    setup_ckan(subparser)
    subparser.add_argument('project_name', choices=sorted(PROJECTS.keys()), help='path to metadata')
    subparser.add_argument('--uploads', type=int, default=4, help='number of parallel uploads')
    subparser.add_argument('--metadata-only', '-m', action='store_const', const=True, default=False, help='set metadata only, no data uploads')
    subparser.add_argument('--skip-resource-checks', action='store_const', const=True, default=False, help='skip resource checks')


def setup_hash(subparser):
    setup_ckan(subparser)
    subparser.add_argument('project_name', choices=sorted(PROJECTS.keys()), help='path to metadata')
    subparser.add_argument('mirror_path', help='path to locally mounted mirror', nargs='?', default=os.environ.get('MIRROR_PATH'))


def setup_dump(subparser):
    subparser.add_argument('filename', help='output target')


@register_command
def sync(args):
    """sync a project"""
    ckan = make_ckan_api(args)
    with DownloadMetadata(PROJECTS[args.project_name], path=args.download_path) as dlmeta:
        sync_metadata(ckan, dlmeta.meta, dlmeta.auth, args.uploads, not args.metadata_only, not args.skip_resource_checks)
        print_accounts()


@register_command
def makeschema(args):
    generate_schemas(args)


@register_command
def dumpstate(args):
    dump_state(args)


@register_command
def genhash(args):
    ckan = make_ckan_api(args)
    """
    verify MD5 sums for a local (filesystem mounted) mirror of the BPA
    data, and generate expected E-Tag and SHA256 values.
    """
    with DownloadMetadata(PROJECTS[args.project_name], path=args.download_path) as dlmeta:
        genhash_fn(ckan, dlmeta.meta, args.mirror_path, num_threads=4)
        print_accounts()


sync.setup = setup_sync
bootstrap.setup = setup_ckan
dumpstate.setup = setup_dump
genhash.setup = setup_hash


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
    parser.add_argument('-p', '--download-path', required=False, default=None, help='CKAN base url')

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
    args.func(args)
