import json
import os
import re
from collections import defaultdict
from urllib.parse import urljoin

from glob import glob
from unipath import Path

from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import sample_id_to_ckan_name, common_values, clean_tag_name
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


def gap_describe_ddrad(obj, description):
    obj["title"] = "GAP {}, {}, Dataset ID {}".format(
        description,
        obj.get("project_aim", ""),
        obj.get("dataset_id", "").split("/")[-1],
    )
    obj["notes"] = "{}, {}, {}".format(
        obj.get("species_complex", ""),
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
    resource_linkage = ("ticket", "sample_id", "library_id", "flow_cell_id")
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
                            xlsx_info["ticket"],
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
    resource_linkage = ("ticket", "sample_id", "run_id")
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
                    (
                        (
                            xlsx_info["ticket"],
                            resource["sample_id"],
                            resource["run_id"],
                        ),
                        legacy_url,
                        resource,
                    )
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
    resource_linkage = ("ticket", "sample_id", "run_id")
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
                    (
                        (
                            xlsx_info["ticket"],
                            resource["sample_id"],
                            resource["run_id"],
                        ),
                        legacy_url,
                        resource,
                    )
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


class GAPGenomicsDDRADMetadata(BaseMetadata):
    """
    This data conforms to the BPA Genomics ddRAD workflow. future data
    will use this ingest class.
    Issue: bpa-archive-ops#699
    """

    organization = "bpa-plants"
    ckan_data_type = "gap-genomics-ddrad"
    omics = "genomics"
    technology = "ddrad"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/ddrad/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("dataset_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld(
                "dataset_id",
                "bioplatforms_dataset_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "library_id",
                "bioplatforms_library_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_id",
                "bioplatforms_sample_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("plate_name", "plate_name"),
            fld("plate_well", "plate_well"),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps", coerce=ingest_utils.get_int),
            fld("library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int),
            fld("library_comments", "library_comments"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("library_pool_index_id", "library_pool_index_id", optional=True),
            fld(
                "library_pool_index_sequence",
                "library_pool_index_sequence",
                optional=True,
            ),
            fld(
                "library_pool_oligo_sequence",
                "library_pool_oligo_sequence",
                optional=True,
            ),
            fld("library_conc_ng_ul", "library_conc_ng_ul"),
            fld("project_aim", "project_aim"),
            fld("sample_submitter_name", "sample_submitter_name"),
            fld("sample_submitter_email", "sample_submitter_email"),
            fld("scientific_name", "scientific_name"),
            fld("scientific_name_authorship", "scientific_name_authorship"),
            fld("family", "family"),
            fld("scientific_name_notes", "scientific_name_notes"),
            fld("country", "country"),
            fld("state_or_territory", "state_or_territory"),
            fld("location_id", "location_id"),
            fld("location_notes", "location_notes"),
            fld("population_group", "population_group"),
            fld("species_complex", "species_complex", optional=True),
        ],
        "options": {
            "sheet_name": "GAP_library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.ddrad_fastq_filename_re,],
        "skip": [
            files.ddrad_metadata_sheet_re,
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^err$"),
            re.compile(r"^out$"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = GAPTrackMetadata()
        self.flow_lookup = {}

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        self._logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing GAP metadata file {0}".format(os.path.basename(fname))
            )
            flow_id = get_flow_id(fname)
            objs = defaultdict(list)
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                obj = row._asdict()
                if not obj["dataset_id"] or not obj["flowcell_id"]:
                    continue
                objs[(obj["dataset_id"], obj["flowcell_id"])].append(obj)

            for (dataset_id, flowcell_id), row_objs in list(objs.items()):

                if dataset_id is None or flowcell_id is None:
                    continue

                obj = common_values(row_objs)
                track_meta = self.track_meta.get(obj["ticket"])

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                name = sample_id_to_ckan_name(
                    dataset_id, self.ckan_data_type, flowcell_id
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "dataset_id": dataset_id,
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer")
                        ),
                        "data_type": track_get("data_type"),
                        "description": track_get("description"),
                        "folder_name": track_get("folder_name"),
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer")
                        ),
                        "data_generated": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer_to_archive")
                        ),
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer_to_archive")
                        ),
                        "dataset_url": track_get("download"),
                        "type": self.ckan_data_type,
                    }
                )
                gap_describe_ddrad(obj, "ddRAD")
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                tag_names = ["genomics-ddrad"]
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting GAP md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (
                            ingest_utils.extract_ands_id(
                                self._logger, resource["dataset_id"]
                            ),
                            resource["flowcell_id"],
                        ),
                        legacy_url,
                        resource,
                    )
                )
        return resources + self.generate_xlsx_resources()
