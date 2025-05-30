import os
import re
from collections import defaultdict

from glob import glob
from unipath import Path

from . import files
from .contextual import AusargLibraryContextual, AusargDatasetControlContextual
from .tracking import AusArgGoogleTrackMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld, make_skip_column as skp
from ...libs.fetch_data import Fetcher, get_password
from ...sensitive_species_wrapper import SensitiveSpeciesWrapper
from ...util import (
    sample_id_to_ckan_name,
    common_values,
    merge_values,
    apply_cc_by_license,
)

common_context = [AusargLibraryContextual, AusargDatasetControlContextual]

CONSORTIUM_ORG_NAME = "ausarg-consortium-members"


class AusargBaseMetadata(BaseMetadata):
    initiative = "AusARG"
    organization = "ausarg"

    notes_mapping = [
        {"key": "family", "separator": ", "},
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "specimen_id", "separator": ", "},
        {"key": "taxonomic_group", "separator": ", Project Lead: "},
        {"key": "data_custodian"},
    ]
    title_mapping = [
        {"key": "common_name", "separator": ", "},
        {"key": "data_context", "separator": ", "},
        {"key": "data_type", "separator": ", "},
        {"key": "tissue_type"},
    ]

    # below are defined here so they can be used with self., but values set in the init method of the relevant subclass.
    generaliser = None
    google_track_meta = None
    path = None
    contextual_metadata = None
    metadata_info = None
    google_track_meta = None
    linkage_xlsx = {}
    xlsx_info = None
    ticket = None
    flow_lookup = {}  # only used in dart

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.generaliser = SensitiveSpeciesWrapper(
            self._logger, package_id_keyname="dataset_id"
        )
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = AusArgGoogleTrackMetadata(logger)

    # this method just for here for backwards compatibility

    def apply_location_generalisation(self, packages):
        if packages:
            return self.generaliser.apply_location_generalisation(packages)

    def get_tracking_info(self, ticket, field_name=None):
        if self.google_track_meta is None:
            return None

        if ticket is None:
            return None

        tracking_row = self.google_track_meta.get(ticket)
        if tracking_row is None:
            self._logger.warn("No tracking row found for {}".format(ticket))
            return None

        if field_name is None:
            return tracking_row
        # todo check attribute exists, throw error/log if not
        return getattr(tracking_row, field_name)

    def _build_title_into_object(self, obj):
        self.build_title_into_object(
            obj, {"initiative": self.initiative, "title_description": self.description}
        )

    def _get_common_packages(self):
        self._logger.info(
            "Ingesting {} metadata from {}".format(self.initiative, self.path)
        )
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing {} metadata file {}".format(
                    self.initiative, os.path.basename(fname)
                )
            )
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            if self.method_exists("_set_metadata_vars"):
                self._set_metadata_vars(fname)
            for row in rows:
                if not row.library_id and not row.flowcell_id:
                    # skip empty rows
                    continue
                obj = row._asdict()
                name = sample_id_to_ckan_name(
                    "{}".format(row.library_id.split("/")[-1]),
                    self.ckan_data_type,
                    "{}".format(row.flowcell_id),
                )
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(row.sample_id))
                obj.update(context)

                tracking_row = self.get_tracking_info(row.ticket)

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, tracking_row.date_of_transfer
                        ),
                        "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                            self._logger, tracking_row.date_of_transfer_to_archive
                        ),
                        "description": tracking_row.description,
                        "facility": tracking_row.facility,
                        "folder_name": tracking_row.folder_name,
                        "project_aim": tracking_row.project_aim,
                    }
                )

                if tracking_row.data_type is not None:
                    # force the sequence datatype to the one in the tracking spreadsheet
                    obj["sequence_data_type"] = tracking_row.data_type
                    obj["data_type"] = tracking_row.data_type

                self._add_datatype_specific_info_to_package(obj, row, fname)
                self._build_title_into_object(obj)
                if (
                    "notes" not in obj.keys()
                ):  # some classes have a special notes construction method which is run before
                    self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "date_of_transfer_to_archive",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )

                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": "{:.100}".format(t)} for t in self.tag_names]
                packages.append(obj)
        return packages


class AusargIlluminaFastqMetadata(AusargBaseMetadata):
    ckan_data_type = "ausarg-illumina-fastq"
    technology = "illumina-fastq"
    sequence_data_type = "illumina-shortread"
    embargo_days = 10
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/illumina-fastq/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld(
                "library_id",
                re.compile(r"library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_id",
                re.compile(r"sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                re.compile(r"dataset_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("work_order", "work_order", coerce=ingest_utils.get_int),
            fld("specimen_id", re.compile(r"specimen_[Ii][Dd]")),
            fld("tissue_number", "tissue_number", optional=True),
            fld("voucher_or_tissue", "voucher_or_tissue", optional=True),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("data_custodian", "data_custodian"),
            fld("experimental_design", re.compile("experimental[ _]design")),
            fld(
                "ausarg_project",
                re.compile(r"[Aa]us[aA][rR][gG]_project"),
                optional=True,
            ),
            fld("facility_sample_id", re.compile(r"facility_sample_[Ii][Dd]")),
            fld("sequencing_facility", "sequencing_facility"),
            fld("sequencing_platform", "sequencing_platform"),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status", optional=True),
            fld("library_comments", "library_comments"),
            fld("dna_treatment", re.compile(r"[Dd][nN][aA]_treatment")),
            fld("library_index_id", re.compile(r"library_index_[Ii][Dd]")),
            fld("library_index_seq", "library_index_seq"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_index_id_dual", "library_index_id_dual", optional=True),
            fld("library_index_seq_dual", "library_index_seq_dual", optional=True),
            fld(
                "library_oligo_sequence_dual",
                "library_oligo_sequence_dual",
                optional=True,
            ),
            fld(
                "i5_index_reverse_complement",
                "i5 index reverse complement",
                optional=True,
            ),
            fld("insert_size_range", "insert_size_range"),
            fld("library_ng_ul", "library_ng_ul"),
            fld(
                "library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int
            ),
            fld("library_pcr_reps", "library_pcr_reps", coerce=ingest_utils.get_int),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("flowcell_type", "flowcell_type"),
            fld("flowcell_id", re.compile(r"flowcell_[Ii][Dd]")),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("facility_project_code", "facility_project_code", optional=True),
            fld("data_context", "data_context", optional=True),
            fld("library_layout", "library_layout", optional=True),
            fld("sequencing_model", "sequencing_model", optional=True),
            fld("library_strategy", "library_strategy", optional=True),
            fld("library_selection", "library_selection", optional=True),
            fld("library_source", "library_source", optional=True),
            fld(
                "sequencing_kit_chemistry_version",
                "sequencing_kit_chemistry_version",
                optional=True,
            ),
            fld("file_type", "file_type", optional=True),
            fld("fast5_compression", "fast5_compression", optional=True),
            fld("model_base_caller", "model_base_caller", optional=True),
        ],
        "options": {
            "sheet_name": "Library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
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
    tag_names = ["illumina-fastq"]
    description = "Ilumina FASTQ"

    def _get_packages(self):
        return self._get_common_packages()

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        # overwrite potentially incorrect values from tracking data - fail if source fields don't exist
        obj["bioplatforms_sample_id"] = obj["sample_id"]
        obj["bioplatforms_library_id"] = obj["library_id"]
        obj["bioplatforms_dataset_id"] = obj["dataset_id"]
        obj["scientific_name"] = "{} {}".format(obj["genus"], obj["species"])

    def _set_metadata_vars(self, filename):
        self.xlsx_info = self.metadata_info[os.path.basename(filename)]
        self.ticket = self.xlsx_info["ticket"]

    def _validate_row_and_metadata(self, fname, ticket, row):
        metadata_sheet_flowcell_id = re.match(
            r"^.*_([^_]+)_metadata.*\.xlsx", fname
        ).groups()[0]
        if metadata_sheet_flowcell_id != row.flowcell_id:
            raise Exception(
                "The metadata row for library ID: {} has a flow cell ID of {}, which cannot be found in the metadata sheet name: {}".format(
                    row.library_id, row.flowcell_id, fname
                )
            )
        tracking_folder_name = self.get_tracking_info(ticket, "folder_name")
        if not re.search(row.flowcell_id, tracking_folder_name):
            raise Exception(
                "The metadata row for library ID: {} has a flow cell ID of {}, which cannot be found in the tracking field value: {}".format(
                    row.library_id, row.flowcell_id, tracking_folder_name
                )
            )

    def _add_datatype_specific_info_to_resource(self, resource, md5_file):
        resource["library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["library_id"]
        )

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            resource["library_id"],
            resource["flowcell_id"],
        )


class AusargONTPromethionMetadata(AusargBaseMetadata):
    ckan_data_type = "ausarg-ont-promethion"
    technology = "ont-promethion"
    sequence_data_type = "ont-promethion"
    embargo_days = 10
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/promethion/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld("genus", "genus"),
            fld("species", "species"),
            fld("voucher_id", "voucher_id", optional=True),
            fld(
                "library_id",
                re.compile(r"library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_id",
                re.compile(r"sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                re.compile(r"dataset_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("facility_sample_id", re.compile(r"facility_sample_[Ii][Dd]")),
            fld("facility_project_code", "facility_project_code", optional=True),
            fld("specimen_id", "specimen_id"),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            # fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", re.compile("experimental[ _]design")),
            fld(
                "ausarg_project",
                re.compile(r"[Aa]us[aA][rR][gG]_project"),
                optional=True,
            ),
            fld("data_custodian", "data_custodian"),
            fld("data_context", "data_context", optional=True),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id", optional=True),
            fld("library_index_sequence", "library_index_seq", optional=True),
            fld("library_oligo_sequence", "library_oligo_sequence", optional=True),
            fld("library_index_id_dual", "library_index_id_dual", optional=True),
            fld("library_index_seq_dual", "library_index_seq_dual", optional=True),
            fld(
                "library_oligo_sequence_dual",
                "library_oligo_sequence_dual",
                optional=True,
            ),
            fld(
                "library_pcr_reps",
                "library_pcr_reps",
                coerce=ingest_utils.get_int,
                optional=True,
            ),
            fld(
                "library_pcr_cycles",
                "library_pcr_cycles",
                coerce=ingest_utils.get_int,
                optional=True,
            ),
            fld("library_ng_ul", "library_ng_ul", optional=True),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status", optional=True),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled",
                "n_libraries_pooled",
                optional=True,
                coerce=ingest_utils.get_int,
            ),
            fld("work_order", "work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length", optional=True),
            fld("flowcell_id", "flowcell_id"),
            # fld("software_version", "software_version"),
            fld("file", "file", optional=True),
            fld("insert_size_range", "insert_size_range", optional=True),
            fld("flowcell_type", "flowcell_type", optional=True),
            fld("cell_position", "cell_position", optional=True),
            fld("voucher_number", "voucher_number", optional=True),
            fld("tissue_number", "tissue_number", optional=True),
            fld("voucher_or_tissue_number", "voucher_or_tissue_number", optional=True),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("library_layout", "library_layout", optional=True),
            fld("sequencing_model", "sequencing_model", optional=True),
            fld("library_strategy", "library_strategy", optional=True),
            fld("library_selection", "library_selection", optional=True),
            fld("library_source", "library_source", optional=True),
            fld(
                "sequencing_kit_chemistry_version",
                "sequencing_kit_chemistry_version",
                optional=True,
            ),
            fld("file_type", "file_type", optional=True),
            fld("fast5_compression", "fast5_compression", optional=True),
            fld("model_base_caller", "model_base_caller", optional=True),
        ],
        "options": {
            "sheet_name": "Library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.ont_promethion_re, files.ont_promethion_2_re],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }

    description = "ONT PromethION"
    tag_names = ["ont-promethion"]

    def _get_packages(self):
        packages = self._get_common_packages()
        for package in packages:
            self.track_xlsx_resource(package, package["filename"])
            del package["filename"]

        return self.apply_location_generalisation(packages)

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        obj.update(
            {
                "data_type": self.get_tracking_info(row.ticket, "data_type"),
                "description": self.get_tracking_info(row.ticket, "description"),
                "folder_name": self.get_tracking_info(row.ticket, "folder_name"),
                "dataset_url": self.get_tracking_info(row.ticket, "download"),
                "filename": filename,  # this is removed, it is only added for resource linkage tracking.
            }
        )

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file):
        resource["library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["library_id"]
        )

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            resource["library_id"],
            resource["flowcell_id"],
        )


class AusargPacbioHifiMetadata(AusargBaseMetadata):
    ckan_data_type = "ausarg-pacbio-hifi"
    technology = "pacbio-hifi"
    sequence_data_type = "pacbio-hifi"
    embargo_days = 10
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*[\._]metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/pacbio-hifi/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("library_id", "flowcell_id")
    resource_info = {}
    spreadsheet = {
        "fields": [
            fld(
                "library_id",
                re.compile(r"library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_id",
                re.compile(r"sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                re.compile(r"dataset_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("work_order", "work_order", coerce=ingest_utils.get_int),
            fld(
                "ausarg_project",
                re.compile(r"[Aa]us[aA][rR][gG]_project"),
                optional=True,
            ),
            fld("specimen_id", re.compile(r"specimen_[Ii][Dd]"), optional=True),
            fld("tissue_number", "tissue_number"),
            fld("insert_size_range", "insert_size_range"),
            fld("flowcell_type", "flowcell_type"),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_seq"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_index_id_dual", "library_index_id_dual", optional=True),
            fld("library_index_seq_dual", "library_index_seq_dual", optional=True),
            fld(
                "library_oligo_sequence_dual",
                "library_oligo_sequence_dual",
                optional=True,
            ),
            fld("library_pcr_reps", "library_pcr_reps", coerce=ingest_utils.get_int),
            fld(
                "library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int
            ),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status", optional=True),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("sequencing_platform", "sequencing_platform"),
            fld("flowcell_id", "flowcell_id"),
            fld("experimental_design", re.compile("experimental[_ ]design")),
            fld("voucher_number", "voucher_number", optional=True),
            fld("file_name", "file_name", optional=True),
            fld("facility_project_code", "facility_project_code", optional=True),
            fld("data_context", "data_context", optional=True),
            fld("library_layout", "library_layout", optional=True),
            fld("sequencing_model", "sequencing_model", optional=True),
            fld("library_strategy", "library_strategy", optional=True),
            fld("library_selection", "library_selection", optional=True),
            fld("library_source", "library_source", optional=True),
            fld(
                "sequencing_kit_chemistry_version",
                "sequencing_kit_chemistry_version",
                optional=True,
            ),
            fld("file_type", "file_type", optional=True),
            fld("fast5_compression", "fast5_compression", optional=True),
            fld("model_base_caller", "model_base_caller", optional=True),
        ],
        "options": {
            "sheet_name": "Library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [
            files.pacbio_hifi_filename_re,
            files.pacbio_hifi_filename_2_re,
            files.pacbio_hifi_metadata_sheet_re,
            files.pacbio_hifi_common_re,
        ],
        "skip": [
            re.compile(r"^.*[\._]metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    common_files_match = [
        files.pacbio_hifi_common_re,
    ]
    common_files_linkage = ("flowcell_id",)

    description = "Pacbio HiFi"
    tag_names = ["pacbio-hifi"]

    def _get_packages(self):
        packages = self._get_common_packages()
        return self.apply_location_generalisation(packages)

    def _validate_row_and_metadata(self, fname, ticket, row):
        filename_re = files.pacbio_hifi_metadata_sheet_re

        metadata_sheet_dict = re.match(filename_re, os.path.basename(fname)).groupdict()
        metadata_sheet_flowcell_ids = []
        for f in ["flowcell_id", "flowcell2_id"]:
            if f in metadata_sheet_dict:
                metadata_sheet_flowcell_ids.append(metadata_sheet_dict[f])

        if row.flowcell_id not in metadata_sheet_flowcell_ids:
            raise Exception(
                "The metadata row for library ID: {} has a flow cell ID of {}, which cannot be found in the metadata sheet name: {}".format(
                    row.library_id, row.flowcell_id, fname
                )
            )

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        name = sample_id_to_ckan_name(
            "{}".format(row.library_id.split("/")[-1]),
            self.ckan_data_type,
            "{}".format(row.flowcell_id),
        )
        obj.update(
            {
                "name": name,
                "id": name,
                "data_type": self.get_tracking_info(row.ticket, "data_type"),
                "description": self.get_tracking_info(row.ticket, "description"),
                "folder_name": self.get_tracking_info(row.ticket, "folder_name"),
                "dataset_url": self.get_tracking_info(row.ticket, "download"),
                "type": self.ckan_data_type,
                "sequence_data_type": self.sequence_data_type,
            }
        )

        ingest_utils.add_spatial_extra(self._logger, obj)

    def _get_resource_info(self, metadata_info):
        auth_user, auth_env_name = self.auth
        ri_auth = (auth_user, get_password(auth_env_name))
        self._logger.info(metadata_info)

        for metadata_url in self.metadata_urls:
            self._logger.info("fetching resource metadata: %s" % (self.metadata_urls))
            fetcher = Fetcher(self._logger, self.path, metadata_url, ri_auth)
            fetcher.fetch_metadata_from_folder(
                [
                    files.pacbio_hifi_filename_re,
                ],
                metadata_info,
                getattr(self, "metadata_url_components", []),
                download=False,
            )

    def _get_resources(self):
        resources = self._get_common_resources()
        return resources + self.generate_common_files_resources(resources)

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        #    no additional fields for pacbio-hifi needed, just empty return
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            ingest_utils.extract_ands_id(self._logger, resource["library_id"]),
            resource["flowcell_id"],
        )

    def _build_common_files_linkage(self, xlsx_info, resource, file_info):
        return (resource["flowcell_id"],)


class AusargExonCaptureMetadata(AusargBaseMetadata):
    ckan_data_type = "ausarg-exon-capture"
    technology = "exoncapture"
    sequence_data_type = "illumina-exoncapture"
    embargo_days = 10
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*[lL]ibrary[mM]etadata.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/exon_capture/",
    ]
    metadata_url_components = (
        "facility",
        "ticket",
    )
    resource_linkage = ("library_id", "flowcell_id", "library_index_seq")
    spreadsheet = {
        "fields": [
            fld("genus", "genus", optional=True),
            fld("species", "species", optional=True),
            fld("voucher_id", "voucher_id", optional=True),
            fld("dataset_id", "dataset_id", coerce=ingest_utils.extract_ands_id),
            fld("library_id", "library_id", coerce=ingest_utils.extract_ands_id),
            fld(
                "sample_id",
                "sample_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("experimental_design", "experimental_design"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id", optional=True),
            fld("library_oligo_sequence", "library_oligo_sequence", optional=True),
            fld("library_pcr_reps", "library_pcr_reps", coerce=ingest_utils.get_int),
            fld(
                "library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int
            ),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("sequencing_platform", "sequencing_platform"),
            fld("flowcell_id", "flowcell_id"),
            fld("voucher_or_tissue_number", "voucher_or_tissue_number", optional=True),
            fld("work_order", "work_order", coerce=ingest_utils.get_int),
            fld("facility_project_code", "facility_project_code"),
            fld("specimen_id", "specimen_id"),
            fld("tissue_number", "tissue_number", optional=True),
            fld("data_context", "data_context"),
            fld("library_layout", "library_layout"),
            fld("sequencing_model", "sequencing_model"),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("library_strategy", "library_strategy"),
            fld("library_selection", "library_selection"),
            fld("library_source", "library_source"),
            fld("insert_size_range", "insert_size_range"),
            fld("flowcell_type", "flowcell_type"),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("sequencing_kit_chemistry_version", "sequencing_kit_chemistry_version"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("file_type", "file_type"),
            fld("library_index_seq", "library_index_seq"),
            fld("library_index_id_dual", "library_index_id_dual"),
            fld("library_index_seq_dual", "library_index_seq_dual"),
            fld("library_oligo_sequence_dual", "library_oligo_sequence_dual"),
            fld("fast5_compression", "fast5_compression"),
            fld("model_base_caller", "model_base_caller"),
        ],
        "options": {
            "sheet_name": "Library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.exon_filename_re],
        "skip": [
            re.compile(r"^.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }

    description = "Exon Capture Raw"
    tag_names = ["exon-capture", "raw"]

    @classmethod
    def flow_cell_index_linkage(cls, flow_id, index):
        return flow_id + "_" + index.replace("-", "").replace("_", "")

    def _get_packages(self):
        packages = self._get_common_packages()

        return self.apply_location_generalisation(packages)

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        obj.update(
            {
                "data_type": self.get_tracking_info(row.ticket, "data_type"),
                "description": self.get_tracking_info(row.ticket, "description"),
                "folder_name": self.get_tracking_info(row.ticket, "folder_name"),
                "dataset_url": self.get_tracking_info(row.ticket, "download"),
            }
        )
        linkage = self.flow_cell_index_linkage(
            obj["flowcell_id"], obj["library_index_seq"]
        )

        obj["name"] = sample_id_to_ckan_name(
            obj["library_id"], self.ckan_data_type, linkage
        )
        obj["id"] = obj["name"]

        ingest_utils.add_spatial_extra(self._logger, obj)

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        #    no additional fields needed, just empty return
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            ingest_utils.extract_ands_id(self._logger, resource["library_id"]),
            resource["flowcell_id"],
            resource["index"],
        )


class AusargHiCMetadata(AusargBaseMetadata):
    ckan_data_type = "ausarg-hi-c"
    description = "Hi-C"
    technology = "hi-c"
    sequence_data_type = "illumina-hic"
    embargo_days = 10
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/genomics-hi-c/",
    ]
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*\.xlsx$"]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld(
                "library_id",
                re.compile(r"library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_id",
                re.compile(r"sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                re.compile(r"dataset_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("run_format", "run format", optional=True),
            fld("facility_project_code", "facility_project_code"),
            fld("specimen_id", "specimen_id"),
            fld("tissue_number", "tissue_number"),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("data_custodian", "data_custodian"),
            fld("data_context", "data_context"),
            fld("library_type", "library_type"),
            fld("library_layout", "library_layout"),
            fld("facility_sample_id", "facility_sample_id"),
            fld("sequencing_facility", "sequencing_facility"),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequencing_model", "sequencing_model"),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("library_strategy", "library_strategy"),
            fld("library_selection", "library_selection"),
            fld("library_source", "library_source"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_location", "library_location"),
            fld("library_comments", "library_comments"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_seq", "library_index_seq"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("insert_size_range", "insert_size_range"),
            fld("library_ng_ul", "library_ng_ul"),
            fld(
                "library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int
            ),
            fld("library_pcr_reps", "library_pcr_reps", coerce=ingest_utils.get_int),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("flowcell_type", "flowcell_type"),
            fld("flowcell_id", "flowcell_id"),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("sequencing_kit_chemistry_version", "sequencing_kit_chemistry_version"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("file_type", "file_type"),
            fld("experimental_design", "experimental_design"),
            fld("work_order", "work_order", coerce=ingest_utils.get_int),
            fld("library_index_id_dual", "library_index_id_dual", optional=True),
            fld("library_index_seq_dual", "library_index_seq_dual", optional=True),
            fld(
                "library_oligo_sequence_dual",
                "library_oligo_sequence_dual",
                optional=True,
            ),
            fld("fast5_compression", "fast5_compression", optional=True),
            fld("model_base_caller", "model_base_caller", optional=True),
        ],
        "options": {
            "sheet_name": "Library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.illumina_hic_re],
        "skip": [
            re.compile(r"^.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }

    tag_names = ["genomics"]

    def _build_notes_into_object(self, obj, library_id):
        self.build_notes_into_object(
            obj,
            {
                "initiative": self.initiative,
                "title_description": self.description,
                "complete_library_id": library_id,
            },
        )

    def _get_packages(self):
        return self._get_common_packages()

    def _set_metadata_vars(self, filename):
        self.xlsx_info = self.metadata_info[os.path.basename(filename)]
        self.ticket = self.xlsx_info["ticket"]

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        sample_id = row.sample_id
        library_id = row.library_id
        raw_library_id = library_id.split("/")[-1]
        name = sample_id_to_ckan_name(
            raw_library_id, self.ckan_data_type, row.flowcell_id
        )
        tracking_row = self.get_tracking_info(self.ticket)
        if tracking_row is not None:
            obj.update(
                {
                    "scientific_name": tracking_row.scientific_name,
                    "bioplatforms_dataset_id": tracking_row.bioplatforms_dataset_id,
                    "bioplatforms_library_id": tracking_row.bioplatforms_library_id,
                    "bioplatforms_sample_id": tracking_row.bioplatforms_sample_id,
                }
            )
        obj.update(
            {
                "sample_id": sample_id,
                "name": name,
                "id": name,
                "sequence_data_type": self.sequence_data_type,
                "flowcell_id": row.flowcell_id,
                "library_id": raw_library_id,
            }
        )
        self._build_notes_into_object(obj, row.library_id)

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["library_id"]
        )

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            xlsx_info["ticket"],
            file_info.get("library_id"),
            resource["flowcell_id"],
        )


class AusargGenomicsDArTMetadata(AusargBaseMetadata):
    """
    This data conforms to the BPA Genomics DArT workflow. future data
    will use this ingest class.
    """

    ckan_data_type = "ausarg-genomics-dart"
    omics = "genomics"
    technology = "dart"
    sequence_data_type = "illumina-dart"
    embargo_days = 10
    contextual_classes = common_context
    metadata_patterns = [
        r"^.*\.md5$",
        r"^.*\.xlsx$",
    ]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/dart/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("dataset_id",)
    spreadsheet = {
        "fields": [
            fld("genus", "genus"),
            fld("species", "species"),
            fld(
                "library_id",
                re.compile(r"library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_id",
                re.compile(r"sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                re.compile(r"dataset_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("work_order", "work_order", coerce=ingest_utils.get_int),
            fld("specimen_id", re.compile(r"specimen_[Ii][Dd]")),
            fld("facility_sample_id", "facility_sample_id"),
            fld("facility_project_code", "facility_project_code", optional=True),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("experimental_design", "experimental_design"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps", coerce=ingest_utils.get_int),
            fld(
                "library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int
            ),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("sequencing_platform", "sequencing_platform"),
            fld("flowcell_id", "flowcell_id"),
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
            fld("voucher_number", "voucher_number", optional=True),
            fld("tissue_number", "tissue_number", optional=True),
            fld("voucher_or_tissue_number", "voucher_or_tissue_number", optional=True),
            fld("library_layout", "library_layout"),
            fld("sequencing_model", "sequencing_model"),
            fld("library_strategy", "library_strategy"),
            fld("library_selection", "library_selection"),
            fld("cell_postion", "cell_postion"),
            fld("data_context", "data_context"),
            fld("sequencing_kit_chemistry_version", "sequencing_kit_chemistry_version"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("library_index_seq", "library_index_seq"),
            fld("library_index_id_dual", "library_index_id_dual"),
            fld("library_index_seq_dual", "library_index_seq_dual"),
            fld("library_oligo_sequence_dual", "library_oligo_sequence_dual"),
            fld("library_source", "library_source"),
            fld("fast5_compression", "fast5_compression"),
            fld("model_base_caller", "model_base_caller"),
            fld("insert_size_range", "insert_size_range"),
            fld("movie_length", "movie_length"),
            fld("file_type", "file_type"),
            fld("flowcell_type", "flowcell_type"),
        ],
        "options": {
            "sheet_name": "Library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [
            files.dart_filename_re,
        ],
        "skip": [
            files.dart_xlsx_filename_re,
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^err$"),
            re.compile(r"^out$"),
            re.compile(r"^.*DataValidation\.pdf.*"),
        ],
    }

    description = "DArT"
    tag_names = ["genomics-dart"]

    def _get_packages(self):
        self._logger.info(
            "Ingesting {} metadata from {}".format(self.initiative, self.path)
        )
        packages = []

        # this is a folder-oriented ingest, so we crush each xlsx down into a single row
        filename_re = re.compile(r"^AusARG.*_(\d{5,6})_librarymetadata\.xlsx")

        objs = []
        flattened_objs = defaultdict(list)
        for fname in glob(self.path + "/*librarymetadata.xlsx"):
            row_objs = []
            self._logger.info(
                "Processing {} metadata file {}".format(
                    self.initiative, os.path.basename(fname)
                )
            )
            file_dataset_id = ingest_utils.extract_ands_id(
                self._logger, filename_re.match(os.path.basename(fname)).groups()[0]
            )

            for row in self.parse_spreadsheet(fname, self.metadata_info):
                obj = row._asdict()

                if not obj["dataset_id"]:
                    continue
                if file_dataset_id != obj["dataset_id"]:
                    self._logger.warn(
                        "Skipping metadata row related to unrelated dataset {0} (should be {1})".format(
                            obj["dataset_id"], file_dataset_id
                        )
                    )
                    continue

                # Add sample contextual metadata
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(obj.get("sample_id")))

                # obj.pop("file")

                row_objs.append(obj)
                # self._logger.info(obj)

            combined_obj = common_values(row_objs)
            combined_obj.update(merge_values("scientific_name", " , ", row_objs))

            objs.append((fname, combined_obj))

        for fname, obj in objs:
            ticket = obj["ticket"]

            name = sample_id_to_ckan_name(
                obj["dataset_id"], self.ckan_data_type, ticket
            )
            obj.update(
                {
                    "name": name,
                    "id": name,
                    # "bpa_dataset_id": bpa_dataset_id,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, self.get_tracking_info(ticket, "date_of_transfer")
                    ),
                    "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                        self._logger,
                        self.get_tracking_info(ticket, "date_of_transfer_to_archive"),
                    ),
                    "data_type": self.get_tracking_info(ticket, "data_type"),
                    "description": self.get_tracking_info(ticket, "description"),
                    "folder_name": self.get_tracking_info(ticket, "folder_name"),
                    "dataset_url": self.get_tracking_info(ticket, "download"),
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                }
            )
            organism_scientific_name = obj.get(
                "scientific_name",
                "%s %s" % (obj.get("genus", ""), obj.get("species", "")),
            )
            additional_notes = "DArT dataset not demultiplexed"
            self._build_title_into_object(obj)
            self.build_notes_into_object(
                obj,
                {
                    "organism_scientific_name": organism_scientific_name,
                    "additional_notes": additional_notes,
                },
            )
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "date_of_transfer_to_archive",
                self.embargo_days,
                CONSORTIUM_ORG_NAME,
            )

            attach_message = (
                "Attached metadata spreadsheets were produced when data was generated."
            )
            if "related_data" not in obj:
                obj["related_data"] = attach_message
            else:
                obj["related_data"] = "{0} {1}".format(
                    attach_message, obj["related_data"]
                )
            ingest_utils.apply_access_control(self._logger, self, obj)
            ingest_utils.add_spatial_extra(self._logger, obj)
            obj["tags"] = [{"name": t} for t in self.tag_names]
            self.track_xlsx_resource(obj, fname)
            for sample_metadata_file in glob(
                self.path
                + "/*_"
                + ingest_utils.short_ands_id(self._logger, obj["dataset_id"])
                + "_samplemetadata_ingest.xlsx"
            ):
                self.track_xlsx_resource(obj, sample_metadata_file)
            packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file):
        def __dataset_id_from_md5_file(fname):
            fname = os.path.basename(fname)
            assert files.dart_md5_filename_re.match(fname) is not None
            md5match = files.dart_md5_filename_re.match(fname)
            assert "dataset_id" in md5match.groupdict()
            return md5match.groupdict()["dataset_id"]

        resource["dataset_id"] = __dataset_id_from_md5_file(md5_file)

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (ingest_utils.extract_ands_id(self._logger, resource["dataset_id"]),)


class AusargGenomicsDDRADMetadata(AusargBaseMetadata):
    """
    This data conforms to the BPA Genomics ddRAD workflow.
    """

    ckan_data_type = "ausarg-genomics-ddrad"
    omics = "genomics"
    technology = "ddrad"
    sequence_data_type = "illumina-ddrad"
    embargo_days = 10
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/ddrad/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_dataset_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld("genus", "genus"),
            fld("species", "species"),
            fld(
                "sample_id",
                re.compile(r"(sample_id|bioplatforms_sample_id)"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "library_id",
                re.compile(r"(library_id|bioplatforms_library_id)"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                re.compile(r"(dataset_id|bioplatforms_dataset_id)"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("experimental_design", "experimental_design"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_seq"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps", coerce=ingest_utils.get_int),
            fld(
                "library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int
            ),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status", optional=True),
            fld("sequencing_facility", "sequencing_facility"),
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("work_order", "work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("flowcell_id", "flowcell_id"),
            fld("file", "file", optional=True),
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
            fld("specimen_id", re.compile(r"specimen_[Ii][Dd]")),
            fld("tissue_number", "tissue_number"),
            fld("data_context", "data_context"),
            fld("library_layout", "library_layout"),
            fld("sequencing_model", "sequencing_model"),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("library_strategy", "library_strategy"),
            fld("library_selection", "library_selection"),
            fld("library_source", "library_source"),
            fld("insert_size_range", "insert_size_range"),
            fld("flowcell_type", "flowcell_type"),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("file_name", "file_name", optional=True),
            fld("file_type", "file_type"),
            fld(
                "sequencing_kit_chemistry_version",
                "sequencing_kit_chemistry_version",
                optional=True,
            ),
            fld("facility_project_code", "facility_project_code", optional=True),
            fld(
                "library_index_id_dual",
                re.compile(r"(library_index_id_dual|library_dual_index_id)"),
                optional=True,
            ),
            fld(
                "library_index_seq_dual",
                re.compile(r"(library_index_seq_dual|library_dual_index_seq)"),
                optional=True,
            ),
            fld(
                "library_oligo_sequence_dual",
                re.compile(
                    r"(library_oligo_sequence_dual|library_dual_oligo_sequence)"
                ),
                optional=True,
            ),
            fld("fast5_compression", "fast5_compression", optional=True),
            fld("model_base_caller", "model_base_caller", optional=True),
            fld("bait_set_name", "bait_set_name", optional=True),
            fld("bait_set_reference", "bait_set_reference", optional=True),
            fld("bioplatforms_project", "bioplatforms_project", optional=True),
        ],
        "options": {
            "sheet_name": "Library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [
            files.ddrad_fastq_filename_re,
            files.ddrad_metadata_sheet_re,
            files.ddrad_analysed_tar_re,
        ],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
        ],
    }

    description = "Genomics ddRAD"
    tag_names = ["genomics-ddrad"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = AusArgGoogleTrackMetadata(logger)
        self.flow_lookup = {}

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            flow_id = get_flow_id(fname)
            objs = defaultdict(list)
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                obj = row._asdict()
                obj.pop("file")
                if not obj["dataset_id"] or not obj["flowcell_id"]:
                    continue
                objs[(obj["dataset_id"], obj["flowcell_id"])].append(obj)

            for (bpa_dataset_id, flowcell_id), row_objs in list(objs.items()):
                if bpa_dataset_id is None or flowcell_id is None:
                    continue

                context_objs = []
                for row in row_objs:
                    context = {}
                    for contextual_source in self.contextual_metadata:
                        context.update(contextual_source.get(row.get("sample_id")))
                    context_objs.append(context)

                obj = common_values(row_objs)
                ticket = obj["ticket"]

                name = sample_id_to_ckan_name(
                    bpa_dataset_id, self.ckan_data_type, flowcell_id
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "bpa_dataset_id": bpa_dataset_id,
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger,
                            self.get_tracking_info(ticket, "date_of_transfer"),
                        ),
                        "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                            self._logger,
                            self.get_tracking_info(
                                ticket, "date_of_transfer_to_archive"
                            ),
                        ),
                        "data_type": self.get_tracking_info(ticket, "data_type"),
                        "description": self.get_tracking_info(ticket, "description"),
                        "folder_name": self.get_tracking_info(ticket, "folder_name"),
                        "dataset_url": self.get_tracking_info(ticket, "download"),
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                    }
                )
                obj.update(common_values(context_objs))
                obj.update(merge_values("scientific_name", " , ", context_objs))
                additional_notes = "ddRAD dataset not demultiplexed"
                self._build_title_into_object(obj)
                self.build_notes_into_object(
                    obj, {"additional_notes": additional_notes}
                )
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "date_of_transfer_to_archive",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return packages

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        obj.update({"dataset_url": self.get_tracking_info(row.ticket, "download")})
        # below fields are in the metadata, but not required in the packages schema

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # none for Genomics ddrad
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            ingest_utils.extract_ands_id(self._logger, resource["bpa_dataset_id"]),
            resource["flowcell_id"],
        )
