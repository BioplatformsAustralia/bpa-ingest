import json
import os
import re
from collections import defaultdict

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
from .contextual import ForestLibraryContextual, ForestDatasetControlContextual
from .tracking import ForestTrackMetadata

common_context = [ForestLibraryContextual, ForestDatasetControlContextual]
CONSORTIUM_ORG_NAME = "forest-resilience-consortium-members"

class ForestBaseMetadata(BaseMetadata):
    organization = "forest-resilience"
    initiative = "Forest Resilience"


    title_mapping = [
        {"key": "initiative", "separator": " "},
        {"key": "description", "separator": ", "},
        {"key": "project_aim", "separator": ", Library ID: "},
        {"key": "library_id", "separator": ", "},
    ]

    notes_mapping = [
        {"key": "scientific_name", "separator": ", "},
        {"key": "common_name", "separator": ", "},
        {"key": "family", "separator": ", "},
        {"key": "sample_submitter_name", "separator": ""},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = ForestTrackMetadata(logger)

    def _build_title_into_object(self, obj):
        self.build_title_into_object(
            obj,
            {
                "initiative": self.initiative,
                "description": self.description,
                "project_aim": obj.get("project_aim", ""),
                "libray_id": obj.get("bioplatforms_library_id", "").split("/")[-1],
            },
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
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                if not row.bioplatforms_dataset_id and not row.flowcell_id:
                    # skip empty rows
                    continue
                sample_id = row.bioplatforms_sample_id
                library_id = row.bioplatforms_library_id
                dataset_id = row.bioplatforms_dataset_id
                raw_library_id = library_id.split("/")[-1]
                raw_dataset_id = dataset_id.split("/")[-1]
                obj = row._asdict()
                if track_meta is not None:
                    if track_meta.dataset_id == raw_dataset_id:
                        obj.update(track_meta._asdict())
                    else:
                        self._logger.error(
                            "Mismatch between Tracking sheet dataset ID: {0} and  Metadata dataset ID: {1} in Ticket {2}".format(
                                track_meta.dataset_id, raw_dataset_id, ticket
                            )
                        )
                name = sample_id_to_ckan_name(
                    raw_library_id, self.ckan_data_type,
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(library_id,))

                obj.update(
                    {
                        "library_id": library_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
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
                self._build_title_into_object(obj)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "date_of_transfer_to_archive",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                packages.append(obj)
        return packages


class ForestPacbioHifiMetadata(ForestBaseMetadata):
    ckan_data_type = "forest-pacbio-hifi"
    technology = "pacbio-hifi"
    sequence_data_type = "pacbio-hifi"
    embargo_days = 365
    description = "PacBio HiFi"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*(\.|_)metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/forest_staging/pacbio-hifi/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld('bioplatforms_project', 'bioplatforms_project'),
            fld('bioplatforms_sample_id', 'bioplatforms_sample_id', coerce=ingest_utils.extract_ands_id,),
            fld('bioplatforms_library_id', 'bioplatforms_library_id', coerce=ingest_utils.extract_ands_id,),
            fld('bioplatforms_dataset_id', 'bioplatforms_dataset_id', coerce=ingest_utils.extract_ands_id,),
            fld('work_order', 'work_order'),
            fld('facility_project_code', 'facility_project_code'),
            fld('library_type', 'library_type'),
            fld('library_layout', 'library_layout'),
            fld('facility_sample_id', 'facility_sample_id'),
            fld('sequencing_facility', 'sequencing_facility'),
            fld('sequencing_platform', 'sequencing_platform'),
            fld('sequencing_model', 'sequencing_model'),
            fld('library_construction_protocol', 'library_construction_protocol'),
            fld('library_strategy', 'library_strategy'),
            fld('bait_set_name', 'bait_set_name'),
            fld('bait_set_reference', 'bait_set_reference'),
            fld('library_selection', 'library_selection'),
            fld('library_source', 'library_source'),
            fld('library_prep_date', 'library_prep_date', coerce=ingest_utils.get_date_isoformat),
            fld('library_prepared_by', 'library_prepared_by'),
            fld('library_location', 'library_location'),
            fld('library_comments', 'library_comments'),
            fld('dna_treatment', 'dna_treatment'),
            fld('library_index_id', 'library_index_id'),
            fld('library_index_seq', 'library_index_seq'),
            fld('library_oligo_sequence', 'library_oligo_sequence'),
            fld('library_index_id_dual', 'library_index_id_dual'),
            fld('library_index_seq_dual', 'library_index_seq_dual'),
            fld('library_oligo_sequence_dual', 'library_oligo_sequence_dual'),
            fld('insert_size_range', 'insert_size_range'),
            fld('library_ng_ul', 'library_ng_ul'),
            fld('library_pcr_cycles', 'library_pcr_cycles', coerce=ingest_utils.get_int),
            fld('library_pcr_reps', 'library_pcr_reps', coerce=ingest_utils.get_int),
            fld('n_libraries_pooled', 'n_libraries_pooled', coerce=ingest_utils.get_int),
            fld('flowcell_type', 'flowcell_type'),
            fld('flowcell_id', 'flowcell_id'),
            fld('cell_postion', 'cell_postion'),
            fld('movie_length', 'movie_length'),
            fld('sequencing_kit_chemistry_version', 'sequencing_kit_chemistry_version'),
            fld('analysis_software', 'analysis_software'),
            fld('experimental_design', 'experimental_design'),
            fld('fast5_compression', 'fast5_compression'),
            fld('model_base_caller', 'model_base_caller'),

        ],
        "options": {
            "sheet_name": "Sequencing metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [
            # files.pacbio_hifi_filename_re,
            files.pacbio_hifi_filename_revio_re,
            files.pacbio_hifi_revio_pdf_re,
            # files.pacbio_hifi_metadata_sheet_re,
            files.pacbio_hifi_revio_metadata_sheet_re,
        ],
        "skip": [
            re.compile(r"^.*(\.|_)metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    common_files_match = [
        files.pacbio_hifi_revio_pdf_re,
    ]
    common_files_linkage = ("flowcell_id",)
    description = ""
    tag_names = ["pacbio-hifi"]

    def _get_packages(self):
        return self._get_common_packages()

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        revio_xls_filename_re = files.pacbio_hifi_revio_metadata_sheet_re
        metadata_sheet = re.search(revio_xls_filename_re, os.path.basename(filename))

        if metadata_sheet is not None:
            metadata_sheet_dict = metadata_sheet.groupdict()
            metadata_sheet_flowcell_ids = []
            for f in ["flowcell_id", "flowcell2_id"]:
                if f in metadata_sheet_dict:
                    metadata_sheet_flowcell_ids.append(metadata_sheet_dict[f])

            if row.flowcell_id not in metadata_sheet_flowcell_ids:
                raise Exception(
                    "The metadata row for library ID: {} has a flow cell ID of {}, which cannot be found in the metadata sheet name: {}".format(
                        row.bioplatforms_library_id, row.flowcell_id, filename
                    )
                )

            name = sample_id_to_ckan_name(
                "{}".format(row.bioplatforms_library_id.split("/")[-1]),
                self.ckan_data_type,
                "{}".format(row.flowcell_id),
            )
            track_meta = self.get_tracking_info(row.ticket)

            obj.update(
                {
                    "id": name,
                    "name": name,
                    "library_id": row.bioplatforms_library_id.split("/")[-1],
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    # "data_type": track_meta.data_type,  # there is no datatype in the tracking sheet for forest
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                }
            )
            self.description = obj.get(
                "description"
            )  # set the description for the title, it comes from the spreadsheet


        else:
            self._logger.error(
                "Metadata Sheet {0} cannot be parsed wih RegExp".format(filename)
            )

    def _get_resources(self):
        resources = self._get_common_resources()
        return resources + self.generate_common_files_resources(resources)

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        """
        if "sample_id" in resource:
            resource["sample_id"] = ingest_utils.extract_ands_id(
                self._logger, resource["sample_id"]
            )
            """
        pass   # nothing here yet...

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            xlsx_info["ticket"],
            resource["library_id"],
            resource["flowcell_id"],
        )

    def _build_common_files_linkage(self, xlsx_info, resource, file_info):
        return (resource["flowcell_id"],)

class ForestIlluminaShortreadMetadata(ForestBaseMetadata):
    ckan_data_type = "forest-illumina-shortread"
    technology = "illumina-shortread"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/forest_staging/illumina-shortread/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bioplatforms_library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld(
                "bioplatforms_sample_id",
                "bioplatforms_sample_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample_id", "sample_id", optional=True),
            fld(
                "bioplatforms_library_id",
                re.compile(r"bioplatforms_library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_dataset_id",
                "bioplatforms_dataset_id",
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
            fld('tissue_number', 'tissue_number'),
            fld('genus', 'genus'),
            fld('species', 'species'),
            fld('data_custodian', 'data_custodian'),
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
            fld("file_name", "file_name", optional=True),
            fld("experimental_design", "experimental_design"),
            fld("sequencing_platform", "sequencing_platform"),
            fld("facility_project_code", "facility_project_code", optional=True),
            fld(
                "sequencing_kit_chemistry_version",
                "sequencing_kit_chemistry_version",
                optional=True,
            ),
            fld("bioplatforms_project", "bioplatforms_project"),
            fld("scientific_name", "scientific_name", optional=True),
            fld("project_lead", "project_lead", optional=True),
            fld("project_collaborators", "project_collaborators", optional=True),
            fld("bait_set_name", "bait_set_name"),
            fld("bait_set_reference", "bait_set_reference"),
            fld("library_index_id_dual", "library_index_id_dual"),
            fld("library_index_seq_dual", "library_index_seq_dual"),
            fld("library_oligo_sequence_dual", "library_oligo_sequence_dual"),
            fld("fast5_compression", "fast5_compression"),
            fld("model_base_caller", "model_base_caller"),
        ],
        "options": {
            "sheet_name": "Sequencing metadata",
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

    def _get_packages(self):
        packages = self._get_common_packages()
        return packages

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        obj.update(
            {
                "bioplatforms_library_id": row.bioplatforms_library_id,
                "library_id": row.bioplatforms_library_id.split("/")[-1],
            }
        )

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["library_id"]
        )
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            resource["library_id"],
            resource["flowcell_id"],
        )

