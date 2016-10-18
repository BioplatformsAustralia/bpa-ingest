from __future__ import print_function

import tempfile
import argparse
import shutil
import sys

from .util import make_registration_decorator, make_ckan_api
from .sync import sync_metadata
from .ops import print_accounts, make_organization
from .util import make_logger
from .genhash import genhash as genhash_fn
from .projects import PROJECTS
from .organizations import ORGANIZATIONS
from .libs.fetch_data import Fetcher, get_password


logger = make_logger(__name__)
register_command, command_fns = make_registration_decorator()


class DownloadMetadata(object):
    def __init__(self, project_class, track_csv_path):
        self.path = tempfile.mkdtemp(prefix='bpaingest-metadata-')
        self.auth = None
        if hasattr(project_class, 'auth'):
            auth_user, auth_env_name = project_class.auth
            self.auth = (auth_user, get_password(auth_env_name))
        fetcher = Fetcher(self.path, project_class.metadata_url, self.auth)
        logger.info("metadata url is: %s" % (project_class.metadata_url))
        fetcher.fetch_metadata_from_folder()
        self.meta = project_class(self.path, track_csv_path=track_csv_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.path)
        pass


@register_command
def bootstrap(ckan, args):
    "bootstrap basic organisation data"
    for organization in ORGANIZATIONS:
        logger.info("%s" % (organization['name']))
        make_organization(ckan, organization)


def setup_sync(subparser):
    subparser.add_argument('project_name', choices=sorted(PROJECTS.keys()), help='path to metadata')
    subparser.add_argument('--uploads', type=int, default=4, help='number of parallel uploads')
    subparser.add_argument('--track-metadata', type=str, default=None, help='metadata tracking spreadsheet (CSV)')


def setup_hash(subparser):
    subparser.add_argument('project_name', choices=sorted(PROJECTS.keys()), help='path to metadata')
    subparser.add_argument('mirror_path', help='path to locally mounted mirror')


@register_command
def sync(ckan, args):
    """sync a project"""
    with DownloadMetadata(PROJECTS[args.project_name], args.track_metadata) as dlmeta:
        sync_metadata(ckan, dlmeta.meta, dlmeta.auth, args.uploads)
        print_accounts()

sync.setup = setup_sync


@register_command
def genhash(ckan, args):
    """
    verify MD5 sums for a local (filesystem mounted) mirror of the BPA
    data, and generate expected E-Tag and SHA256 values.
    """
    with DownloadMetadata(PROJECTS[args.project_name], None) as dlmeta:
        genhash_fn(ckan, dlmeta.meta, args.mirror_path)
        print_accounts()

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
