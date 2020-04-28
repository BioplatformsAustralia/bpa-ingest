import os
import re
from glob import glob
from urllib.parse import urljoin

from unipath import Path

from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import make_logger, sample_id_to_ckan_name, clean_tag_name
from . import files
from .contextual import GAPLibraryContextual
from .tracking import GAPTrackMetadata

logger = make_logger(__name__)
common_context = [GAPLibraryContextual]


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
    resource_linkage = ("sample_id", "flow_cell_id")
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
        "match": [files.illumina_shortread_re],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super().__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata()

    def _get_packages(self):
        logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            logger.info("Processing GAP metadata file {0}".format(fname))
            flow_cell_id = re.match(r"^.*_([^_]+)_metadata.xlsx", fname).groups()[0]
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
                name = sample_id_to_ckan_name(
                    library_id.split("/")[-1], self.ckan_data_type
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(library_id))
                obj.update(
                    {
                        "title": "GAP Illumina short read {} {}".format(
                            sample_id, flow_cell_id
                        ),
                        "notes": "{}, {}".format(
                            obj.get("scientific_name", ""),
                            obj.get("sample_submitter_name", ""),
                        ),
                        "sample_id": sample_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "flow_cell_id": flow_cell_id,
                        "private": True,
                        "data_generated": True,
                    }
                )
                tag_names = ["genomics", "illumina-shortread"]
                if "scientific_name" in obj:
                    tag_names.append(clean_tag_name(obj["scientific_name"]))
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["sample_id"] = ingest_utils.extract_ands_id(
                    resource["sample_id"]
                )
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (resource["sample_id"], resource["flow_cell_id"]),
                        legacy_url,
                        resource,
                    )
                )
        return resources


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

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super().__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata()

    def _get_packages(self):
        logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            logger.info("Processing GAP metadata file {0}".format(fname))
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
                name = sample_id_to_ckan_name(
                    library_id.split("/")[-1], self.ckan_data_type
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(library_id))
                obj.update(
                    {
                        "title": "GAP ONT MinION {} {}".format(sample_id, row.run_id),
                        "notes": "{}, {}".format(
                            obj.get("scientific_name", ""),
                            obj.get("sample_submitter_name", ""),
                        ),
                        "sample_id": sample_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "private": True,
                        "data_generated": True,
                    }
                )
                tag_names = ["ont-minion"]
                if "scientific_name" in obj:
                    tag_names.append(clean_tag_name(obj["scientific_name"]))
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["sample_id"] = ingest_utils.extract_ands_id(
                    resource["sample_id"]
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

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super().__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata()

    def _get_packages(self):
        logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            logger.info("Processing GAP metadata file {0}".format(fname))
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
                name = sample_id_to_ckan_name(
                    library_id.split("/")[-1], self.ckan_data_type
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(library_id))
                obj.update(
                    {
                        "title": "GAP ONT PromethION {} {}".format(
                            sample_id, row.run_id
                        ),
                        "notes": "{}, {}".format(
                            obj.get("scientific_name", ""),
                            obj.get("sample_submitter_name", ""),
                        ),
                        "sample_id": sample_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "private": True,
                        "data_generated": True,
                    }
                )
                tag_names = ["ont-promethion"]
                if "scientific_name" in obj:
                    tag_names.append(clean_tag_name(obj["scientific_name"]))
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["sample_id"] = ingest_utils.extract_ands_id(
                    resource["sample_id"]
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

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super().__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata()

    def _get_packages(self):
        logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            logger.info("Processing GAP metadata file {0}".format(fname))
            flow_cell_id = re.match(r"^.*_([^_]+)_metadata.xlsx", fname).groups()[0]
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
                name = sample_id_to_ckan_name(
                    library_id.split("/")[-1], self.ckan_data_type
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(library_id))
                obj.update(
                    {
                        "title": "GAP Genomics 10X {} {}".format(
                            sample_id, flow_cell_id
                        ),
                        "notes": "{}, {}".format(
                            obj.get("scientific_name", ""),
                            obj.get("sample_submitter_name", ""),
                        ),
                        "sample_id": sample_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "flow_cell_id": flow_cell_id,
                        "private": True,
                        "data_generated": True,
                    }
                )
                tag_names = ["genomics", "10x"]
                if "scientific_name" in obj:
                    tag_names.append(clean_tag_name(obj["scientific_name"]))
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["sample_id"] = ingest_utils.extract_ands_id(
                    resource["sample_id"]
                )
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((xlsx_info["ticket"],), legacy_url, resource))
        return resources
