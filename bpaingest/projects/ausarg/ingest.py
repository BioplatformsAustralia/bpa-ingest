import json
import os
import re
from glob import glob
from urllib.parse import urljoin

from unipath import Path

from .contextual import AusargLibraryContextual
from .tracking import AusArgGoogleTrackMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import sample_id_to_ckan_name, clean_tag_name
from . import files

common_context = [AusargLibraryContextual]


class AusargIlluminaFastqMetadata(BaseMetadata):
    organization = "bpa-plants"
    ckan_data_type = "ausarg-illumina-fastq"
    technology = "illumina-fastq"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/illumina-fastq/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("sample_id", "library_id", "flow_cell_id")
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "plant sample unique id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "library_id",
                re.compile(r"^[Ll]ibrary [Ii][Dd]$"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("dataset_id", "dataset id", coerce=ingest_utils.extract_ands_id),
            fld("library_construction_protocol", "library construction protocol"),
            fld("sequencer", "sequencer"),
            fld("analysissoftwareversion", "analysissoftwareversion"),
        ],
        "options": {
            "sheet_name": None,
            "header_length": 2,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [files.illumina_fastq_re],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    notes_mapping = [
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "AusARG_project", "separator": ", "},
        {"key": "sequencing_platforms", "separator": " "},
        {"key": "library_type", "separator": ", "},
        {"key": "state_or_origin", "separator": ", "},
        {"key": "country"},
    ]

    def __init__(
            self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = AusArgGoogleTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing GAP metadata file {0}".format(fname))
            flow_cell_id = re.match(r"^.*_([^_]+)_metadata.*\.xlsx", fname).groups()[0]
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                library_id = row.library_id
                obj = row._asdict()
                if track_meta is not None:
                    obj.update(track_meta._asdict())
                raw_library_id = library_id.split("/")[-1]
                name = sample_id_to_ckan_name(raw_library_id, self.ckan_data_type)
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(library_id))
                obj.update(
                    {
                        "sample_id": sample_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "flow_cell_id": flow_cell_id,
                        "data_generated": True,
                        "library_id": raw_library_id,
                    }
                )
                notes = self.build_notes_without_blanks(obj)
                ingest_utils.permissions_organization_member(self._logger, obj)
                tag_names = ["amphibians", "reptiles", ""]
                scientific_name = obj.get("scientific_name", "").strip()
                if scientific_name:
                    tag_names.append(clean_tag_name(scientific_name))
                obj["tags"] = [{"name": "{:.100}".format(t)} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["sample_id"] = ingest_utils.extract_ands_id(
                    self._logger, resource["sample_id"]
                )
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                # This will be used by sync/dump later to check resource_linkage in resources against that in packages
                resources.append(
                    (
                        (
                            resource["sample_id"],
                            file_info.get("library_id"),
                            resource["flow_cell_id"],
                        ),
                        legacy_url,
                        resource,
                    )
                )
        return resources
