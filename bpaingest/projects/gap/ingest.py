import json
import os
import re
from glob import glob
from urllib.parse import urljoin

from unipath import Path

from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import sample_id_to_ckan_name, clean_tag_name
from . import files
from .contextual import GAPLibraryContextual
from .tracking import GAPTrackMetadata

common_context = [GAPLibraryContextual]


def gap_describe(obj, description):
    obj["title"] = "GAP {}, {}, {}, {}".format(
        description,
        obj.get("project_aim", ""),
        obj.get("sample_id", "").split("/")[-1],
        obj.get("bait_set_name", ""),
    )
    obj["notes"] = "{} {}, {}, {}".format(
        obj.get("scientific_name", ""),
        obj.get("scientific_name_authorship", ""),
        obj.get("family", ""),
        obj.get("sample_submitter_name", ""),
    )


class GAPIlluminaShortreadMetadata(BaseMetadata):
    organization = "bpa-plants"
    ckan_data_type = "gap-illumina-shortread"
    technology = "illumina-shortread"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/genomics-illumina-shortread/",
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
            fld("run_format", "run format", optional=True),
            fld("analysissoftwareversion", "analysissoftwareversion"),
        ],
        "options": {
            "sheet_name": None,
            "header_length": 2,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [files.illumina_shortread_re, files.illumina_shortread_rna_phylo_re],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    description = "Illumina short read"

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata()

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
                dataset_id = row.dataset_id
                obj = row._asdict()
                if track_meta is not None:
                    obj.update(track_meta._asdict())
                raw_library_id = library_id.split("/")[-1]
                raw_dataset_id = dataset_id.split("/")[-1]
                name = sample_id_to_ckan_name(
                    raw_library_id, self.ckan_data_type, raw_dataset_id
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(library_id, dataset_id))
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
                gap_describe(obj, self.description)
                ingest_utils.permissions_organization_member(self._logger, obj)
                tag_names = ["genomics", self.description.replace(" ", "-").lower()]
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


class GAPHiCMetadata(GAPIlluminaShortreadMetadata):
    ckan_data_type = "gap-hi-c"
    description = "Hi-C"
    technology = "hi-c"
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/genomics-hi-c/",
    ]


class GAPONTMinionMetadata(BaseMetadata):
    organization = "bpa-plants"
    ckan_data_type = "gap-ont-minion"
    technology = "ont-minion"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/ont-minion/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("sample_id", "run_id")
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "bioplatforms_sample_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "library_id",
                "bioplatforms_library_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                "bioplatforms_dataset_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("insert_size_range", "insert size range"),
            fld("library_construction_protocol", "library construction protocol"),
            fld("sequencer", "sequencer"),
            fld("flow_cell_type", "flow cell type"),
            fld("run_id", "run id"),
            fld("cell_postion", "cell postion"),
            fld("nanopore_software_version", "nanopore software version"),
        ],
        "options": {
            "sheet_name": None,
            "header_length": 2,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [files.ont_minion_re],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing GAP metadata file {0}".format(fname))
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                library_id = row.library_id
                dataset_id = row.dataset_id
                obj = row._asdict()
                if track_meta is not None:
                    obj.update(track_meta._asdict())
                raw_library_id = library_id.split("/")[-1]
                raw_dataset_id = dataset_id.split("/")[-1]
                name = sample_id_to_ckan_name(
                    raw_library_id, self.ckan_data_type, raw_dataset_id
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(library_id, dataset_id))
                obj.update(
                    {
                        "sample_id": sample_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "data_generated": True,
                    }
                )
                gap_describe(obj, "ONT MinION")
                ingest_utils.permissions_organization_member(self._logger, obj)
                tag_names = ["ont-minion"]
                if "scientific_name" in obj:
                    tag_names.append(clean_tag_name(obj["scientific_name"]))
                obj["tags"] = [{"name": t} for t in tag_names]
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
                resources.append(
                    ((resource["sample_id"], resource["run_id"]), legacy_url, resource)
                )
        return resources


class GAPONTPromethionMetadata(BaseMetadata):
    organization = "bpa-plants"
    ckan_data_type = "gap-ont-promethion"
    technology = "ont-promethion"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/ont-promethion/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("sample_id", "run_id")
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "bioplatforms_sample_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "library_id",
                "bioplatforms_library_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                "bioplatforms_dataset_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("insert_size_range", "insert size range"),
            fld("library_construction_protocol", "library construction protocol"),
            fld("sequencer", "sequencer"),
            fld("flow_cell_type", "flow cell type"),
            fld("run_id", "run id"),
            fld("cell_postion", "cell postion"),
            fld("nanopore_software_version", "nanopore software version"),
        ],
        "options": {
            "sheet_name": None,
            "header_length": 2,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [files.ont_promethion_re],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing GAP metadata file {0}".format(fname))
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                library_id = row.library_id
                dataset_id = row.dataset_id
                obj = row._asdict()
                if track_meta is not None:
                    obj.update(track_meta._asdict())
                raw_library_id = library_id.split("/")[-1]
                raw_dataset_id = dataset_id.split("/")[-1]
                name = sample_id_to_ckan_name(
                    raw_library_id, self.ckan_data_type, raw_dataset_id
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(library_id, dataset_id))
                obj.update(
                    {
                        "sample_id": sample_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "data_generated": True,
                    }
                )
                gap_describe(obj, "PromethION")
                ingest_utils.permissions_organization_member(self._logger, obj)
                tag_names = ["ont-promethion"]
                if "scientific_name" in obj:
                    tag_names.append(clean_tag_name(obj["scientific_name"]))
                obj["tags"] = [{"name": t} for t in tag_names]
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
                resources.append(
                    ((resource["sample_id"], resource["run_id"]), legacy_url, resource)
                )
        return resources


class GAPGenomics10XMetadata(BaseMetadata):
    organization = "bpa-plants"
    ckan_data_type = "gap-genomics-10x"
    technology = "genomics-10x"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/genomics-10x/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket",)
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "plant sample unique id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("library_id", "library id", coerce=ingest_utils.extract_ands_id),
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
        "match": [files.genomics_10x_re,],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing GAP metadata file {0}".format(fname))
            flow_cell_id = re.match(r"^.*_([^_]+)_metadata.xlsx", fname).groups()[0]
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                library_id = row.library_id
                dataset_id = row.dataset_id
                obj = row._asdict()
                if track_meta is not None:
                    obj.update(track_meta._asdict())
                raw_library_id = library_id.split("/")[-1]
                raw_dataset_id = dataset_id.split("/")[-1]
                name = sample_id_to_ckan_name(
                    raw_library_id, self.ckan_data_type, raw_dataset_id
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(library_id, dataset_id))
                obj.update(
                    {
                        "sample_id": sample_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "flow_cell_id": flow_cell_id,
                        "data_generated": True,
                    }
                )
                gap_describe(obj, "Genomics 10X")
                ingest_utils.permissions_organization_member(self._logger, obj)
                tag_names = ["genomics", "10x"]
                if "scientific_name" in obj:
                    tag_names.append(clean_tag_name(obj["scientific_name"]))
                obj["tags"] = [{"name": t} for t in tag_names]
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
                resources.append(((xlsx_info["ticket"],), legacy_url, resource))
        return resources
