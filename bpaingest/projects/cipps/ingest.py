import os
import re

from glob import glob
from unipath import Path

from . import files
from .contextual import CIPPSLibraryContextual, CIPPSDatasetControlContextual
from .tracking import CIPPSGoogleTrackMetadata
from .tracking import CIPPSProjectsGoogleMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld

from ...util import (
    sample_id_to_ckan_name,
    apply_cc_by_license,
)

common_context = [CIPPSLibraryContextual, CIPPSDatasetControlContextual]


class CIPPSBaseMetadata(BaseMetadata):
    initiative = "CIPPS"
    organization = "cipps"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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

    def _set_metadata_vars(self, filename):
        self.xlsx_info = self.metadata_info[os.path.basename(filename)]
        self.ticket = self.xlsx_info["ticket"]

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
                if not row.bioplatforms_sample_id:
                    # skip empty rows
                    continue
                obj = row._asdict()
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(row.bioplatforms_sample_id))
                obj.update(context)

                if not hasattr(row, "bioplatforms_library_id"):
                    # name is populated by the subclass after the fact
                    name = "No Library ID - override in sublass"
                else:
                    name = sample_id_to_ckan_name(
                        "{}".format(row.bioplatforms_library_id.split("/")[-1]),
                        self.ckan_data_type,
                        "{}".format(row.flowcell_id),
                    )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.get_tracking_info(
                            row.ticket, "data_type"
                        ),
                        "license_id": apply_cc_by_license(),
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger,
                            self.get_tracking_info(row.ticket, "date_of_transfer"),
                        ),
                        "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                            self._logger,
                            self.get_tracking_info(
                                row.ticket, "date_of_transfer_to_archive"
                            ),
                        ),
                    }
                )

                self._add_datatype_specific_info_to_package(obj, row, fname)
                self.build_title_into_object(obj)
                self.build_notes_into_object(obj)
                project_slug = None
                project_code = None
                # get the slug for the org that matches the Project Code.
                if "bioplatforms_project_code" in list(obj.keys()):
                    project_code = obj.get("bioplatforms_project_code", None)
                if project_code is None:
                    self._logger.error(
                        "No project code found for {} in Dataset Control.".format(
                            obj["bioplatforms_sample_id"]
                        )
                    )
                else:
                    for trrow in self.google_project_codes_meta.project_code_rows:
                        if trrow.short_description == project_code:
                            project_slug = trrow.slug

                # If no org exists, fail with ah error, as teh security is based around these orgs.
                if project_slug is None:
                    self._logger.error(
                        "No project found for {} in project_codes tracking sheet".format(
                            project_code
                        )
                    )
                else:
                    ingest_utils.permissions_organization_member_after_embargo(
                        self._logger,
                        obj,
                        "date_of_transfer_to_archive",
                        self.embargo_days,
                        project_slug,
                    )
                    ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": "{:.100}".format(t)} for t in self.tag_names]
                packages.append(obj)
        return packages


class CIPPSIlluminaShortreadMetadata(CIPPSBaseMetadata):
    ckan_data_type = "cipps-illumina-shortread"
    technology = "illumina-shortread"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/cipps_staging/illumina-shortread/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("library_id",)
    spreadsheet = {
        "fields": [
            fld(
                "bioplatforms_sample_id",
                re.compile(r"(sample_id|bioplatforms_sample_id)"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_library_id",
                re.compile(r"(library_id|bioplatforms_library_[Ii][Dd])"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_dataset_id",
                re.compile(r"(dataset_id|bioplatforms_dataset_id)"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("run_format", "run format", optional=True),
            fld("work_order", "work_order", coerce=ingest_utils.int_or_comment),
            fld(
                "specimen_id",
                re.compile(r"specimen_[Ii][Dd]"),
                coerce=ingest_utils.int_or_comment,
                optional=True,
            ),
            fld("tissue_number", "tissue_number"),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("data_custodian", "data_custodian"),
            fld("data_context", "data_context", optional=True),
            fld("library_type", "library_type"),
            fld("library_layout", "library_layout"),
            fld("facility_sample_id", "facility_sample_id"),
            fld("sequencing_facility", "sequencing_facility"),
            fld("sequencing_model", "sequencing_model"),
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
            fld("library_status", "library_status", optional=True),
            fld("library_comments", "library_comments"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_seq"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("insert_size_range", "insert_size_range"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_pcr_reps", "library_pcr_reps", coerce=ingest_utils.get_int),
            fld(
                "library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int
            ),
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("flowcell_type", "flowcell_type"),
            fld("flowcell_id", "flowcell_id"),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("file_name", "file_name", optional=True),
            fld("file_type", "file_type"),
            fld("experimental_design", "experimental_design"),
            fld("sequencing_platform", "sequencing_platform"),
            fld("facility_project_code", "facility_project_code", optional=True),
            fld(
                "sequencing_kit_chemistry_version",
                "sequencing_kit_chemistry_version",
                optional=True,
            ),
            fld("bioplatforms_project", "bioplatforms_project", optional=True),
            fld("scientific_name", "scientific_name", optional=True),
            fld("project_lead", "project_lead", optional=True),
            fld("project_collaborators", "project_collaborators", optional=True),
            fld("bait_set_name", "bait_set_name", optional=True),
            fld("bait_set_reference", "bait_set_reference", optional=True),
            fld("library_index_id_dual", "library_index_id_dual", optional=True),
            fld("library_index_seq_dual", "library_index_seq_dual", optional=True),
            fld("library_oligo_sequence_dual", "library_oligo_sequence_dual", optional=True),
            fld("fast5_compression", "fast5_compression", optional=True),
            fld("model_base_caller", "model_base_caller", optional=True),
        ],
        "options": {
            "sheet_name": "Library metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [
            files.illumina_shortread_re,
        ],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }
    description = "Illumina Shortread"
    tag_names = ["genomics", "illumina-short-read"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = CIPPSGoogleTrackMetadata(logger)
        self.google_project_codes_meta = CIPPSProjectsGoogleMetadata(logger)

    def _get_packages(self):
        return self._get_common_packages()

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        obj.update(
            {
                "library_id": row.bioplatforms_library_id.split("/")[-1],
            }
        )

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # none

        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (resource["library_id"],)


class CIPPSHiCMetadata(CIPPSIlluminaShortreadMetadata):
    ckan_data_type = "cipps-hi-c"
    description = "Hi-C"
    technology = "hi-c"
    sequence_data_type = "illumina-hic"
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/cipps_staging/genomics-hi-c/",
    ]
    tag_names = ["genomics", description.replace(" ", "-").lower()]


class CIPPSPacbioHifiMetadata(CIPPSBaseMetadata):
    ckan_data_type = "cipps-pacbio-hifi"
    technology = "pacbio-hifi"
    sequence_data_type = "pacbio-hifi"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*[\._]metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/cipps_staging/pacbio-hifi/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bioplatforms_library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld(
                "bioplatforms_library_id",
                re.compile(r"library_[Ii][Dd]|bioplatforms_library_id"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_sample_id",
                re.compile(r"sample_[Ii][Dd]|bioplatforms_sample_id"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_dataset_id",
                re.compile(r"dataset_[Ii][Dd]|bioplatforms_dataset_id"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("work_order", "work_order", coerce=ingest_utils.int_or_comment),
            fld("specimen_id", re.compile(r"specimen_[Ii][Dd]")),
            fld("tissue_number", "tissue_number"),
            fld("data_context", "data_context"),
            fld("library_layout", "library_layout"),
            fld("sequencing_model", "sequencing_model"),
            fld("insert_size_range", "insert_size_range"),
            fld("flowcell_type", "flowcell_type"),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("file_name", "file_name", optional=True),
            fld("file_type", "file_type"),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("library_strategy", "library_strategy"),
            fld("library_selection", "library_selection"),
            fld("library_source", "library_source"),
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
            fld("experimental_design", "experimental_design"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_seq"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status", optional=True),
            fld("sequencing_facility", "sequencing_facility"),
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("sequencing_platform", "sequencing_platform"),
            fld("flowcell_id", "flowcell_id"),
            fld(
                "sequencing_kit_chemistry_version",
                "sequencing_kit_chemistry_version",
                optional=True,
            ),
            fld("facility_project_code", "facility_project_code", optional=True),
            fld("bioplatforms_project", "bioplatforms_project", optional=True),
            fld("bait_set_name", "bait_set_name", optional=True),
            fld("bait_set_reference", "bait_set_reference", optional=True),
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
            "sheet_name": "Library metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.pacbio_hifi_filename_re],
        "skip": [
            re.compile(r"^.*[\._]metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }

    description = "Pacbio HiFi"

    tag_names = ["pacbio-hifi"]

    title_mapping = [
        {"key": "common_name", "separator": ", "},
        {"key": "data_context", "separator": ", "},
        {"key": "sequence_data_type", "separator": ", "},
        {"key": "tissue_type"},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = CIPPSGoogleTrackMetadata(logger)
        self.google_project_codes_meta = CIPPSProjectsGoogleMetadata(logger)

    def _get_packages(self):
        return self._get_common_packages()

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        obj.update({"dataset_url": self.get_tracking_info(row.ticket, "download")})

        ingest_utils.add_spatial_extra(self._logger, obj)

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # none for PACbio-hifi
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            ingest_utils.extract_ands_id(self._logger, resource["library_id"]),
            resource["flowcell_id"],
        )
