import os
import re

from glob import glob
from unipath import Path

from . import files
from .contextual import CollaborationsLibraryContextual, CollaborationsDatasetControlContextual
from .tracking import CollaborationsGoogleTrackMetadata
from .tracking import CollaborationsProjectsGoogleMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld

from ...util import (
    sample_id_to_ckan_name,
    apply_cc_by_license,
)

common_context = [CollaborationsLibraryContextual, CollaborationsDatasetControlContextual]


class CollaborationsBaseMetadata(BaseMetadata):
    initiative = "Collaborations"
    organization = "bpa-collaborations"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    notes_mapping = [
        {"key": "general_env_feature", "separator": ", "},
        {"key": "vegetation_type", "separator": ", Collection Date: "},
        {"key": "collection_date", "separator": ", Collaboration: "},
        {"key": "bioplatforms_project_code", "separator": ""},
    ]
    title_mapping = [
        {"key": "sample_type", "separator": ", "},
        {"key": "geo_loc_name", "separator": ", "},
        {"key": "sample_site_location_description", "separator": ", "},
        {"key": "sequence_data_type", "separator": ", "},
    ]

    def _set_metadata_vars(self, filename):
        self.xlsx_info = self.metadata_info[os.path.basename(filename)]
        self.ticket = self.xlsx_info["ticket"]

    def _get_common_packages(self):
        self._logger.info("Ingesting {} metadata from {}".format(self.initiative, self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing {} metadata file {}".format(self.initiative, os.path.basename(fname)))
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            if self.method_exists('_set_metadata_vars'):
                self._set_metadata_vars(fname)
            for row in rows:
                if not row.bioplatforms_sample_id:
                    # skip empty rows
                    continue
                obj = row._asdict()
                context = {}
                library_id = row.bioplatforms_library_id.split("/")[-1]
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(row.bioplatforms_sample_id))
                obj.update(context)

                if not hasattr(row, "bioplatforms_library_id"):
                    # name is populated by the subclass after the fact
                    name = "No Library ID - override in sublass"
                else:
                    name = sample_id_to_ckan_name(
                        "{}".format(library_id),
                        self.ckan_data_type
                    )

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "library_id": library_id,
                        "sequence_data_type": self.get_tracking_info(row.ticket, "data_type"),
                        "license_id": apply_cc_by_license(),
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, self.get_tracking_info(row.ticket, "date_of_transfer")
                        ),
                        "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                            self._logger, self.get_tracking_info(row.ticket, "date_of_transfer_to_archive")
                        ),
                     }
                )
                self._add_datatype_specific_info_to_package(obj, row, fname)
                self.build_title_into_object(obj)
                self.build_notes_into_object(obj)
                project_slug = None
                project_code = None
                # get the slug for the org that matches the Project Code.
                if 'bioplatforms_project_code' in list(obj.keys()):
                    project_code = obj.get('bioplatforms_project_code', None)
                if project_code is None:
                    self._logger.error(
                        "No project code found for {} in Dataset Control.".format(obj["bioplatforms_sample_id"]))
                else:
                    for trrow in self.google_project_codes_meta.project_code_rows:
                        if (trrow.short_description == project_code):
                            project_slug = trrow.slug

                # If no org exists, fail with ah error, as teh security is based around these orgs.
                if project_slug is None:
                    self._logger.error("No project found for {} in project_codes tracking sheet".format(project_code))
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


class CollaborationsMetagenomicsNovaseqMetadata(CollaborationsBaseMetadata):

    ckan_data_type = "collaborations-metagenomics-novaseq"
    omics = "metagenomics"
    technology = "novaseq"
    sequence_data_type = "illumina-shortread"
    embargo_days = 1826  # 5 years
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/collaborations/metagenomics-novaseq/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld("bioplatforms_project", "bioplatforms_project"),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("voucher_id", "voucher_id", optional=True),
            fld(
                "bioplatforms_library_id",
                re.compile(r"bioplatforms_library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_sample_id",
                re.compile(r"bioplatforms_sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_dataset_id",
                re.compile(r"bioplatforms_dataset_[Ii][Dd]"),
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
            fld("data_custodian", "data_custodian"),
            fld("data_context", "data_context", optional=True),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id", optional=True),
            fld("library_index_sequence", "library_index_seq", optional=True),
            fld("library_oligo_sequence", "library_oligo_sequence", optional=True),
            fld('library_index_id_dual', 'library_index_id_dual', optional=True),
            fld('library_index_seq_dual', 'library_index_seq_dual', optional=True),
            fld('library_oligo_sequence_dual', 'library_oligo_sequence_dual', optional=True),
            fld("library_pcr_reps", "library_pcr_reps", coerce=ingest_utils.get_int, optional=True),
            fld("library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int, optional=True),
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
            fld("work_order", "work_order", coerce=ingest_utils.int_or_comment),
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
            fld('bait_set_name', 'bait_set_name', optional=True),
            fld('bait_set_reference', 'bait_set_reference', optional=True),
            fld('number_of_raw_reads', 'number_of_raw_reads', coerce=ingest_utils.get_int, optional=True),

        ],
        "options": {
            "sheet_name": "Library metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.metagenomics_novaseq_re, ],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }
    description = "Metagenomics"
    tag_names = ["genomics", "illumina-short-read"]


    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = CollaborationsGoogleTrackMetadata(logger)
        self.google_project_codes_meta = CollaborationsProjectsGoogleMetadata(logger)

    def _get_packages(self):
        return self._get_common_packages()


    def _add_datatype_specific_info_to_package(self, obj, row, filename):

        return
        #    obj.update(
        #        {"sample_id": row.bioplatforms_sample_id.split("/")[-1],}
        #    )

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # none

        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            resource["library_id"], resource["flowcell_id"],
        )


class CollaborationsONTPromethionMetadata(CollaborationsBaseMetadata):
    ckan_data_type = "collaborations-ont-promethion"
    technology = "ont-promethion"
    sequence_data_type = "ont-promethion"
    embargo_days = 1826  # 5 years
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/collaborations/ont-promethion/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "bioplatforms_library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld("bioplatforms_project", "bioplatforms_project"),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("voucher_id", "voucher_id", optional=True),
            fld(
                "bioplatforms_library_id",
                re.compile(r"bioplatforms_library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_sample_id",
                re.compile(r"bioplatforms_sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_dataset_id",
                re.compile(r"bioplatforms_dataset_[Ii][Dd]"),
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
            fld("data_custodian", "data_custodian"),
            fld("data_context", "data_context", optional=True),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id", optional=True),
            fld("library_index_sequence", "library_index_seq", optional=True),
            fld("library_oligo_sequence", "library_oligo_sequence", optional=True),
            fld('library_index_id_dual', 'library_index_id_dual', optional=True),
            fld('library_index_seq_dual', 'library_index_seq_dual', optional=True),
            fld('library_oligo_sequence_dual', 'library_oligo_sequence_dual', optional=True),
            fld("library_pcr_reps", "library_pcr_reps", coerce=ingest_utils.get_int, optional=True),
            fld("library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int, optional=True),
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
            fld("work_order", "work_order", coerce=ingest_utils.int_or_comment),
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
            fld('bait_set_name', 'bait_set_name', optional=True),
            fld('bait_set_reference', 'bait_set_reference', optional=True),
            fld('number_of_raw_reads', 'number_of_raw_reads', coerce=ingest_utils.get_int, optional=True),

        ],

        "options": {
            "sheet_name": "Sequencing metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [
            files.ont_promethion_re,
        ],
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
        self.google_track_meta = CollaborationsGoogleTrackMetadata(logger)
        self.google_project_codes_meta = CollaborationsProjectsGoogleMetadata(logger)

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        if "library_id" in resource and resource["library_id"] is not None:
            resource["library_id"] = ingest_utils.extract_ands_id(
                self._logger, resource["library_id"]
            )

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        ticket = xlsx_info["ticket"]
        return (
            ticket,
            ingest_utils.extract_ands_id(self._logger, resource["library_id"]),
            resource["flowcell_id"],
        )

    def _get_packages(self):
        return self._get_common_packages()

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        obj.update(
            {"bioplatforms_library_id": row.bioplatforms_library_id,
             "library_id": row.bioplatforms_library_id.split("/")[-1],
             "sample_id": row.bioplatforms_sample_id.split("/")[-1],
             }
        )

