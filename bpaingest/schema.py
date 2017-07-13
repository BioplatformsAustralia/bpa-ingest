
import json

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


def generate_schema(cka, args, meta):
    schema = schema_template.copy()
    print(type(meta).__name__)
    p = {}
    for package in meta.get_packages():
        p.update(package)
    skip = ('id', 'tags', 'private', 'type', 'spatial')
    package_field_mapping = getattr(meta, 'package_field_names', {})
    for k in sorted(p.keys()):
        if k in skip:
            continue
        schema['dataset_fields'].append({
            "field_name": k,
            "label": package_field_mapping.get(k, k),
            "form_placeholder": ""
        })
    r = {}
    for _, _, resource in meta.get_resources():
        r.update(resource)
    resource_field_mapping = getattr(meta, 'resource_field_names', {})
    for k in sorted(r.keys()):
        if k in skip:
            continue
        schema['resource_fields'].append({
            "field_name": k,
            "label": resource_field_mapping.get(k, k),
        })
    schema['dataset_type'] = meta.ckan_data_type
    outf = '/tmp/{}.json'.format(meta.ckan_data_type.replace('-', '_'))
    with open(outf, 'w') as fd:
        json.dump(schema, fd, sort_keys=True, indent=4, separators=(',', ': '))
        fd.write('\n')
    print("generated schema written to: {}".format(outf))
