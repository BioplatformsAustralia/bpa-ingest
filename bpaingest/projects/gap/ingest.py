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
from ...libs.fetch_data import Fetcher, get_password
from ...util import (
    sample_id_to_ckan_name,
    common_values,
    merge_values,
    apply_cc_by_license,
    clean_tag_name,
)
from . import files
from .contextual import GAPLibraryContextual, GAPDatasetControlContextual
from .tracking import GAPTrackMetadata

common_context = [GAPLibraryContextual, GAPDatasetControlContextual]


class GAPBaseMetadata(BaseMetadata):
    organization = "bpa-plants"
    initiative = "GAP"

    title_mapping = [
        {"key": "initiative", "separator": " "},
        {"key": "title_description", "separator": ", "},
        {"key": "project_aim", "separator": ", "},
        {"key": "split_sample_id", "separator": ", "},
        {"key": "bait_set_name", "separator": ""},
    ]

    notes_mapping = [
        {"key": "scientific_name", "separator": " "},
        {"key": "scientific_name_authorship", "separator": ", "},
        {"key": "family", "separator": ", "},
        {"key": "sample_submitter_name", "separator": ""},
    ]

    def _build_title_into_object(self, obj):
        self.build_title_into_object(obj, {"initiative": self.initiative,
                                            "title_description": self.description,
                                            "split_sample_id": obj.get("sample_id", "").split("/")[-1], })


class GAPIlluminaShortreadMetadata(GAPBaseMetadata):
    ckan_data_type = "gap-illumina-shortread"
    technology = "illumina-shortread"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
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
                re.compile(r"(plant sample unique id|bioplatforms_sample_id)"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "library_id",
                re.compile(r"^([Ll]ibrary [Ii][Dd]|bioplatforms_library_id)$"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                re.compile(r"(dataset id|bioplatforms_dataset_id)"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("library_construction_protocol", "library construction protocol"),
            fld("sequencer", "sequencer"),
            fld("run_format", "run format", optional=True),
            fld("analysissoftwareversion", "analysissoftwareversion"),
            fld("flow_cell_id", "flow_cell_id", optional=True),
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
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }
    description = "Illumina short read"
    tag_names = ["genomics", description.replace(" ", "-").lower()]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata(logger)

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
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "flow_cell_id": flow_cell_id,
                        "data_generated": True,
                        "library_id": raw_library_id,
                    }
                )
                self._build_title_into_object(obj)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": "{:.100}".format(t)} for t in self.tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["sample_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["sample_id"]
        )

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
                        xlsx_info["ticket"],
                        resource["sample_id"],
                        file_info.get("library_id"),
                        resource["flow_cell_id"],
                    )


class GAPHiCMetadata(GAPIlluminaShortreadMetadata):
    ckan_data_type = "gap-hi-c"
    description = "Hi-C"
    technology = "hi-c"
    sequence_data_type = "illumina-hic"
    embargo_days = 365
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/genomics-hi-c/",
    ]
    tag_names = ["genomics", description.replace(" ", "-").lower()]


class GAPONTMinionMetadata(GAPBaseMetadata):
    ckan_data_type = "gap-ont-minion"
    technology = "ont-minion"
    sequence_data_type = "ont-minion"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/ont-minion/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "sample_id", "flow_cell_id")
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
            fld("flow_cell_id", re.compile(r"(run id|flow_cell_id)")),
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
    description = "ONT MinION"
    tag_names = ["ont-minion"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata(logger)

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
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "data_generated": True,
                    }
                )
                self._build_title_into_object(obj)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["sample_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["sample_id"]
        )

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            xlsx_info["ticket"],
            resource["sample_id"],
            resource["flow_cell_id"],
        )


class GAPONTPromethionMetadata(GAPBaseMetadata):
    ckan_data_type = "gap-ont-promethion"
    technology = "ont-promethion"
    sequence_data_type = "ont-promethion"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/ont-promethion/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "sample_id", "library_id", "flow_cell_id")
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
            fld("flow_cell_id", re.compile(r"(run id|flow_cell_id)")),
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
        "match": [files.ont_promethion_re, files.ont_promethion_re_2],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    description = "PromethION"
    tag_names = ["ont-promethion"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata(logger)

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
                        "library_id": library_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "data_generated": True,
                    }
                )
                self._build_title_into_object(obj)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["sample_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["sample_id"]
        )
        if "library_id" in resource and resource["library_id"] is not None:
            resource["library_id"] = ingest_utils.extract_ands_id(
                self._logger, resource["library_id"]
            )

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        # if library_id is in file name use that, 
        # otherwise grab from tracking sheet
        ticket = xlsx_info["ticket"]
        track_meta = self.google_track_meta.get(ticket)
        track_library_id = getattr(track_meta,"library_id")
        resource_library_id = resource.get("library_id",None)
        if resource_library_id is not None:
            library_id = resource_library_id
        else:
            library_id = track_library_id
        library_id = ingest_utils.extract_ands_id(
            self._logger,
            library_id
        )

        return (
            ticket,
            resource["sample_id"],
            library_id,
            resource["flow_cell_id"],
        )


class GAPGenomics10XMetadata(GAPBaseMetadata):
    ckan_data_type = "gap-genomics-10x"
    technology = "genomics-10x"
    sequence_data_type = "illumina-10x"
    embargo_days = 365
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
    description = "Genomics 10X"
    tag_names = ["genomics", "10x"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata(logger)

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
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "flow_cell_id": flow_cell_id,
                        "data_generated": True,
                    }
                )
                self._build_title_into_object(obj)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["sample_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["sample_id"]
        )

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            xlsx_info["ticket"],
        )


class GAPGenomicsDDRADMetadata(GAPBaseMetadata):
    """
    This data conforms to the BPA Genomics ddRAD workflow. future data
    will use this ingest class.
    Issue: bpa-archive-ops#699
    """

    ckan_data_type = "gap-genomics-ddrad"
    omics = "genomics"
    technology = "ddrad"
    sequence_data_type = "illumina-ddrad"
    embargo_days = 365
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
            fld(
                "library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int
            ),
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
    description = "ddRAD"
    tag_names = ["genomics-ddrad"]

    notes_mapping = [
        {"key": "scientific_name", "separator": ", "},
        {"key": "species_complex", "separator": ", "},
        {"key": "family", "separator": ", "},
        {"key": "sample_submitter_name", "separator": ""},
    ]

    title_mapping = [
        {"key": "initiative", "separator": " "},
        {"key": "title_description", "separator": ", "},
        {"key": "project_aim", "separator": ", "},
        {"key": "dataset_label", "separator": " "},
        {"key": "split_dataset_id", "separator": ""},
    ]

    def _build_title_into_object(self, obj):
        self.build_title_into_object(obj, {"initiative": self.initiative,
                                            "title_description": self.description,
                                            "dataset_label": "Dataset ID",
                                            "split_dataset_id": obj.get("dataset_id", "").split("/")[-1], })



    """
    def build_title_into_object(self, obj):
        obj["title"] = "GAP {}, {}, Dataset ID {}".format(
            self.description,
            obj.get("project_aim", ""),
            obj.get("dataset_id", "").split("/")[-1],
        )
    """
    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = GAPTrackMetadata(logger)
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
                for contextual_source in self.contextual_metadata:
                    obj.update(
                        contextual_source.get(obj["library_id"], obj["dataset_id"])
                    )
                objs[(obj["dataset_id"], obj["flowcell_id"])].append(obj)

            for (dataset_id, flowcell_id), row_objs in list(objs.items()):

                if dataset_id is None or flowcell_id is None:
                    continue

                context_objs = []
                for row in row_objs:
                    context = {}
                    for contextual_source in self.contextual_metadata:
                        context.update(contextual_source.get(row.get("library_id"),row.get("dataset_id")))
                    context_objs.append(context)

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
                        "data_generated": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer_to_archive")
                        ),
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer_to_archive")
                        ),
                        "dataset_url": track_get("download"),
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                    }
                )
                obj.update(common_values(context_objs))
                obj.update(merge_values("scientific_name", " , ", context_objs))
                self._build_title_into_object(obj)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return packages

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # no additional data for ddrad
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return(
                    ingest_utils.extract_ands_id(
                        self._logger, resource["dataset_id"]
                    ),
                    resource["flowcell_id"],

            )


class GAPPacbioHifiMetadata(GAPBaseMetadata):
    ckan_data_type = "gap-pacbio-hifi"
    technology = "pacbio-hifi"
    sequence_data_type = "pacbio-hifi"
    embargo_days = 365
    description = "PacBio HiFi"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*(\.|_)metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/pacbio-hifi/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "sample_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "sample_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "library_id",
                "library_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                "dataset_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld('library_type', 'library_type'),
            fld('library_layout', 'library_layout'),
            fld('facility_sample_id', 'facility_sample_id'),
            fld('sequencing_facility', 'sequencing_facility'),
            fld('sequencing_platform', 'sequencing_platform'),
            fld('sequencing_model', 'sequencing_model'),
            fld('library_construction_protocol', 'library_construction_protocol'),
            fld('library_strategy', 'library_strategy'),
            fld('library_selection', 'library_selection'),
            fld('library_source', 'library_source'),
            fld('library_prep_date', 'library_prep_date', coerce=ingest_utils.get_date_isoformat),
            fld('library_prepared_by', 'library_prepared_by'),
            fld('library_location', 'library_location'),
            fld('library_status', 'library_status'),
            fld('library_comments', 'library_comments'),
            fld('dna_treatment', 'dna_treatment'),
            fld('insert_size_range', 'insert_size_range'),
            fld('library_ng_ul', 'library_ng_ul'),
            fld('library_pcr_cycles', 'library_pcr_cycles', coerce=ingest_utils.get_int),
            fld('library_pcr_reps', 'library_pcr_reps', coerce=ingest_utils.get_int),
            fld('n_libraries_pooled', 'n_libraries_pooled', coerce=ingest_utils.get_int),
            fld('flowcell_type', 'flowcell_type'),
            fld('flowcell_id', 'flowcell_id'),
            fld('cell_postion', 'cell_postion'),
            fld('movie_length', 'movie_length'),
            fld('analysis_software', 'analysis_software'),
            fld('analysis_software_version', 'analysis_software_version'),
        ],
        "options": {
            "sheet_name": "Sequencing metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.pacbio_hifi_filename_re, files.pacbio_hifi_metadata_sheet_re],
        "skip": [
            re.compile(r"^.*(\.|_)metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    description = ""
    tag_names = ["pacbio-hifi"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata(logger)

    def _get_packages(self):
        self._logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []

        filename_re = files.pacbio_hifi_metadata_sheet_re

        objs = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing GAP metadata file {0}".format(os.path.basename(fname))
            )

            metadata_sheet_dict = re.search(
                filename_re, os.path.basename(fname)
            ).groupdict()
            metadata_sheet_flowcell_ids = []
            for f in ["flowcell_id", "flowcell2_id"]:
                if f in metadata_sheet_dict:
                    metadata_sheet_flowcell_ids.append(metadata_sheet_dict[f])

            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            track_meta = self.google_track_meta.get(ticket)

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            for row in rows:
                if not row.library_id and not row.flowcell_id:
                    # skip empty rows
                    continue
                obj = row._asdict()
                if row.flowcell_id not in metadata_sheet_flowcell_ids:
                    raise Exception(
                        "The metadata row for library ID: {} has a flow cell ID of {}, which cannot be found in the metadata sheet name: {}".format(
                            row.library_id, row.flowcell_id, fname
                        )
                    )
                name = sample_id_to_ckan_name(
                    "{}".format(row.library_id.split("/")[-1]),
                    self.ckan_data_type,
                    "{}".format(row.flowcell_id),
                )

                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(row.library_id, row.dataset_id))

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer")
                        ),
                        "data_type": track_get("data_type"),
                        "description": track_get("description"),
                        "folder_name": track_get("folder_name"),
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer")
                        ),
                        "contextual_data_submission_date": None,
                        "data_generated": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer_to_archive")
                        ),
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer_to_archive")
                        ),
                        "dataset_url": track_get("download"),
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                    }
                )
                obj.update(context)
                self.description = obj.get("description") # set the description for the title, it comes from the spreadsheet
                self._build_title_into_object(obj)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                packages.append(obj)

        return packages

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):

        resource["sample_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["sample_id"]
        )

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
                xlsx_info["ticket"],
                resource["sample_id"],
                resource["flowcell_id"],
            )


