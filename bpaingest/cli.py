from __future__ import print_function

import tempfile
import argparse
import shutil
import json
import sys
import os

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
    def __init__(self, project_class, track_csv_path, path=None):
        self.cleanup = True
        self.path = tempfile.mkdtemp(prefix='bpaingest-metadata-')
        fetch = True
        if path is not None:
            self.path = path
            self.cleanup = False
            if os.access(path, os.R_OK):
                logger.info("skipping metadata download, specified directory `%s' exists" % path)
                fetch = False
        self.auth = None
        self.contextual = []
        if hasattr(project_class, 'auth'):
            auth_user, auth_env_name = project_class.auth
            self.auth = (auth_user, get_password(auth_env_name))
        meta_kwargs = {
            'track_csv_path': track_csv_path
        }
        info_json = os.path.join(self.path, 'bpa-ingest.json')
        contextual_classes = getattr(project_class, 'contextual_classes', [])
        self.contextual = [(os.path.join(self.path, c.name), c) for c in contextual_classes]
        if fetch:
            metadata_info = {}
            for metadata_url in project_class.metadata_urls:
                logger.info("fetching submission metadata: %s" % (project_class.metadata_urls))
                fetcher = Fetcher(self.path, metadata_url, self.auth)
                fetcher.fetch_metadata_from_folder(
                    getattr(project_class, 'metadata_patterns', None),
                    metadata_info,
                    getattr(project_class, 'metadata_url_components', []))
            for contextual_path, contextual_cls in self.contextual:
                os.mkdir(contextual_path)
                logger.info("fetching contextual metadata: %s" % (contextual_cls.metadata_urls))
                for metadata_url in contextual_cls.metadata_urls:
                    fetcher = Fetcher(contextual_path, metadata_url, self.auth)
                    fetcher.fetch_metadata_from_folder(
                        getattr(contextual_cls, 'metadata_patterns', None),
                        metadata_info,
                        getattr(contextual_cls, 'metadata_url_components', []))
            with open(info_json, 'w') as fd:
                json.dump(metadata_info, fd)
        with open(info_json, 'r') as fd:
            meta_kwargs['metadata_info'] = json.load(fd)
        if self.contextual:
            meta_kwargs['contextual_metadata'] = [c(p) for (p, c) in self.contextual]
        self.meta = project_class(self.path, **meta_kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cleanup:
            shutil.rmtree(self.path)


@register_command
def bootstrap(ckan, args):
    "bootstrap basic organisation data"
    for organization in ORGANIZATIONS:
        make_organization(ckan, organization)


def setup_sync(subparser):
    subparser.add_argument('project_name', choices=sorted(PROJECTS.keys()), help='path to metadata')
    subparser.add_argument('--uploads', type=int, default=4, help='number of parallel uploads')
    subparser.add_argument('--metadata-only', '-m', action='store_const', const=True, default=False, help='set metadata only, no data uploads')
    subparser.add_argument('--track-metadata', type=str, default=None, help='metadata tracking spreadsheet (CSV)')


def setup_hash(subparser):
    subparser.add_argument('project_name', choices=sorted(PROJECTS.keys()), help='path to metadata')
    subparser.add_argument('mirror_path', help='path to locally mounted mirror')
    subparser.add_argument('--track-metadata', type=str, default=None, help='metadata tracking spreadsheet (CSV)')


@register_command
def sync(ckan, args):
    """sync a project"""
    with DownloadMetadata(PROJECTS[args.project_name], args.track_metadata, path=args.download_path) as dlmeta:
        sync_metadata(ckan, dlmeta.meta, dlmeta.auth, args.uploads, not args.metadata_only)
        print_accounts()

sync.setup = setup_sync


@register_command
def makeschema(ckan, args):
    schema_template = {
        "scheming_version": 1,
        "about_url": "https://data.bioplatforms.com/",
        "dataset_fields": [
            {
                "field_name": "owner_org",
                "label": "Organization",
                "display_property": "dct:publisher",
                "validators": "owner_org_validator unicode",
                "form_snippet": "organization.html"
            },
            {
                "field_name": "title",
                "label": "Title",
                "preset": "title",
                "form_placeholder": ""
            },
            {
                "field_name": "notes",
                "label": "Description",
                "display_property": "dcat:Dataset/dct:description",
                "form_snippet": "markdown.html",
                "form_placeholder": "eg. Some useful notes about the data"
            },
            {
                "field_name": "name",
                "label": "URL",
                "preset": "dataset_slug",
                "form_placeholder": ""
            },
            {
                "field_name": "tag_string",
                "label": "Tags",
                "display_property": "dcat:Dataset/dct:keyword",
                "validators": "ignore_missing tag_string_convert",
                "form_placeholder": "type to auto-complete",
                "form_attrs": {
                    "data-module": "autocomplete",
                    "data-module-tags": "",
                    "data-module-source": "/api/2/util/tag/autocomplete?incomplete=?"
                }
            },
            {
                "field_name": "spatial",
                "label": "Geospatial Coverage",
                "display_property": "dcat:Dataset/dct:spatial",
                "form_placeholder": "Paste a valid GeoJSON geometry",
                "display_snippet": "spatial.html"
            }
        ],
        "resource_fields": [
            {
                "field_name": "name",
                "label": "Name"
            },
            {
                "field_name": "description",
                "label": "Description"
            },
            {
                "field_name": "url",
                "label": "Data File",
                "preset": "resource_url_upload",
                "form_placeholder": "http://downloads-qcif.bioplatforms.com/my-dataset.fastq.gz",
                "upload_label": "Sequence File"
            },
            {
                "field_name": "md5",
                "label": "MD5"
            },
            {
                "field_name": "sha256",
                "label": "SHA256"
            },
            {
                "field_name": "s3etag_8388608",
                "label": "S3 E-Tag (8MB multipart)"
            }
        ]}
    with DownloadMetadata(PROJECTS[args.project_name], args.track_metadata, path=args.download_path) as dlmeta:
        meta = dlmeta.meta
        schema = schema_template.copy()
        print(type(meta).__name__)
        p = {}
        for package in meta.get_packages():
            p.update(package)
        skip = ('id', 'tags', 'private', 'type', 'spatial')
        for k in sorted(p.keys()):
            if k in skip:
                continue
            schema['dataset_fields'].append({
                "field_name": k,
                "label": k,
                "form_placeholder": ""
            })
        r = {}
        for _, _, resource in meta.get_resources():
            r.update(resource)
        for k in sorted(r.keys()):
            if k in skip:
                continue
            schema['resource_fields'].append({
                "field_name": k,
                "label": k,
            })
        schema['dataset_type'] = meta.ckan_data_type
        outf = '/tmp/{}.json'.format(meta.ckan_data_type.replace('-', '_'))
        with open(outf, 'w') as fd:
            json.dump(schema, fd, sort_keys=True, indent=4, separators=(',', ': '))
            fd.write('\n')
        print("generated schema written to: {}".format(outf))

makeschema.setup = setup_sync


@register_command
def genhash(ckan, args):
    """
    verify MD5 sums for a local (filesystem mounted) mirror of the BPA
    data, and generate expected E-Tag and SHA256 values.
    """
    with DownloadMetadata(PROJECTS[args.project_name], args.track_metadata, path=args.download_path) as dlmeta:
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
    ckan = make_ckan_api(args)
    args.func(ckan, args)
