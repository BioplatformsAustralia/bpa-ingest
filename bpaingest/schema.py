
import json
import os
from collections import defaultdict
from .projects import PROJECTS
from .metadata import DownloadMetadata
from .util import make_logger
from copy import deepcopy


logger = make_logger(__name__)


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
        },
        {
            "field_name": "format",
            "label": "Format",
            "preset": "resource_format_autocomplete",
            "display_property": "dcat:Dataset/dcat:distribution/dcat:Distribution/dcat:format"
        },
    ]}


def _write_schemas(package_keys, resource_keys, package_field_mapping, resource_field_mapping):
    skip_fields = ('id', 'tags', 'private', 'type', 'spatial')
    for data_type in sorted(package_keys):
        schema = deepcopy(schema_template)
        mapping = package_field_mapping[data_type]
        for k in sorted(package_keys[data_type]):
            if k in skip_fields:
                continue
            schema['dataset_fields'].append({
                "field_name": k,
                "label": mapping.get(k, k),
                "form_placeholder": ""
            })
        mapping = resource_field_mapping[data_type]
        for k in sorted(resource_keys[data_type]):
            if k in skip_fields:
                continue
            schema['resource_fields'].append({
                "field_name": k,
                "label": mapping.get(k, k),
            })
        schema['dataset_type'] = data_type
        outf = '/tmp/{}.json'.format(data_type.replace('-', '_'))
        with open(outf, 'w') as fd:
            json.dump(schema, fd, sort_keys=True, indent=4, separators=(',', ': '))
            fd.write('\n')
        print(("generated schema written to: {}".format(outf)))


def generate_schemas(args):
    """
    Generate schemas for all data types.
    Note that several classes may have the same CKAN data type: e.g. MM Amplicons
    As a result, we must build the union of all possible package and resource fields.
    """
    package_keys = defaultdict(set)
    resource_keys = defaultdict(set)
    package_field_mapping = defaultdict(dict)
    resource_field_mapping = defaultdict(dict)

    # download metadata for all project types and aggregate metadata keys
    for project_name, project_cls in sorted(PROJECTS.items()):
        logger.info("Schema generation: %s / %s" % (project_name, project_cls.__name__))
        dlpath = os.path.join(args.download_path, project_cls.__name__)
        with DownloadMetadata(project_cls, path=dlpath) as dlmeta:
            meta = dlmeta.meta
            data_type = meta.ckan_data_type
            package_field_mapping[data_type].update(getattr(meta, 'package_field_names', {}))
            resource_field_mapping[data_type].update(getattr(meta, 'resource_field_names', {}))
            for package in meta.get_packages():
                package_keys[data_type].update(list(package.keys()))
            for _, _, resource in meta.get_resources():
                resource_keys[data_type].update(list(resource.keys()))

    _write_schemas(package_keys, resource_keys, package_field_mapping, resource_field_mapping)
