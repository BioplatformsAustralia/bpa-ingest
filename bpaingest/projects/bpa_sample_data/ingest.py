import os
import re
from collections import defaultdict

from glob import glob
from unipath import Path

from . import files
from .contextual import BSDLibraryContextual, BSDDatasetControlContextual
from .tracking import BSDGoogleTrackMetadata
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

common_context = [BSDLibraryContextual, BSDDatasetControlContextual]

CONSORTIUM_ORG_NAME = "bpa-sample-data-consortium-members"


class BSDBaseMetadata(BaseMetadata):
    initiative = "bpa-sample-data"
    organization = "bpa-sample-data"
    path = None
    notes_mapping = [
        {"key": "family", "separator": ", "},
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "specimen_id", "separator": ", "},
        {"key": "taxonomic_group", "separator": ", Project Lead: "},
        {"key": "project_lead"},
    ]
    title_mapping = [
        {"key": "common_name", "separator": ", "},
        {"key": "data_context", "separator": ", "},
        {"key": "data_type", "separator": ", "},
        {"key": "tissue_type"},
    ]
    metadata_url_components = ("ticket",)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _set_metadata_vars(self, filename):
        self.xlsx_info = self.metadata_info[os.path.basename(filename)]
        print(self.xlsx_info)
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
                if not row.library_id and not row.flowcell_id:
                    continue
                sample_id = row.sample_id
                library_id = row.library_id
                dataset_id = row.dataset_id
                obj = row._asdict()
                track_meta = self.get_tracking_info(row.ticket)
                if track_meta is not None:
                    obj.update(track_meta._asdict())

                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(row.sample_id))
                obj.update(context)
                if not hasattr(row, "flowcell_id"):
                    # name is populated by the subclass after the fact
                    name = "No flowcell- override in sublass"
                else:
                    name = sample_id_to_ckan_name(
                        "{}".format(library_id.split("/")[-1]),
                        self.ckan_data_type,
                        "{}".format(row.flowcell_id),
                    )

                obj.update(
                    {
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
                self.build_title_into_object(obj)
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


class BSDSampleImagesMetadata(BSDBaseMetadata):
    ckan_data_type = "bsd-sample-image"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$"]
    omics = None
    technology = "site-images"
    sequence_data_type = "image"
    embargo_days = 365
    metadata_patterns = [r"^.*\.md5$", r"^.*[\._]metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/bpa_sample_data/sample-images/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("library_id", "flowcell_id")

    spreadsheet = {
        "fields": [
            fld(
                "bioplatforms_project",
                "bioplatforms_project",
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
            fld(
                "dataset_id",
                "bioplatforms_dataset_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("work_order", "work_order"),
            fld("facility_project_code", "facility_project_code"),
            fld("specimen_id", "specimen_id"),
            fld("scientific_name", "scientific_name"),
            fld("project_lead", "project_lead"),
            fld("project_collaborators", "project_collaborators"),
            fld("data_context", "data_context"),
            fld("library_type", "library_type"),
            fld("library_layout", "library_layout"),
            fld("facility_sample_id", "facility_sample_id"),
            fld("sequencing_facility", "sequencing_facility"),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequencing_model", "sequencing_model"),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("library_strategy", "library_strategy"),
            fld("bait_set_name", "bait_set_name"),
            fld("bait_set_reference", "bait_set_reference"),
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
            fld("library_index_id_dual", "library_index_id_dual"),
            fld("library_index_seq_dual", "library_index_seq_dual"),
            fld("library_oligo_sequence_dual", "library_oligo_sequence_dual"),
            fld("insert_size_range", "insert_size_range"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("flowcell_type", "flowcell_type"),
            fld("flowcell_id", "flowcell_id"),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("sequencing_kit_chemistry_version", "sequencing_kit_chemistry_version"),
            fld("analysis_software", "analysis_software"),
            fld("experimental_design", "experimental_design"),
            fld("fast5_compression", "fast5_compression"),
            fld("model_base_caller", "model_base_caller"),
        ],
        "options": {
            "sheet_name": "Library metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }

    md5 = {
        "match": [files.bsd_site_image_filename_re, files.bsd_site_pdf_filename_re],
        "skip": [
            re.compile(r"^.*[\._]metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    # add_md5_as_resource = True
    common_files_match = [
        files.bsd_site_pdf_filename_re,
    ]

    common_files_linkage = ("flowcell_id",)

    description = "Test Site Images"

    tag_names = ["site-images"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = BSDGoogleTrackMetadata(logger)

    def _get_packages(self):
        return self._get_common_packages()

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        # below fields are in the metadata, but not required in the packages schema
        del obj["ccg_jira_ticket"]
        del obj["download"]

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
        # none for site images
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            ingest_utils.extract_ands_id(self._logger, resource["library_id"]),
            resource["flowcell_id"],
        )

    def _build_common_files_linkage(self, xlsx_info, resource, file_info):
        return (resource["flowcell_id"],)


"""
class BSDIlluminaShortreadMetadata(BSDBaseMetadata):
    ckan_data_type = "bsd-illumina-shortread"
    technology = "illumina-shortread"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/bpa-sample-data/illumina-shortread/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                re.compile(r"sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "library_id",
                re.compile(r"library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                re.compile(r"dataset_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("run_format", "run format", optional=True),
            fld("work_order", "work_order", coerce=ingest_utils.get_int),
            fld("specimen_id", re.compile(r"specimen_[Ii][Dd]")),
            fld("tissue_number", "tissue_number"),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("data_custodian", "data_custodian"),
            fld("data_context", "data_context"),
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
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
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
            fld('sequencing_kit_chemistry_version', 'sequencing_kit_chemistry_version', optional=True),
        ],
        "options": {
            "sheet_name": "Library metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.illumina_shortread_re, ],
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
        self.google_track_meta = BSDGoogleTrackMetadata(logger)
    def _get_packages(self):
        packages = self._get_common_packages()
        return self.apply_location_generalisation(packages)

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        flow_cell_id = re.match(r"^.*_([^_]+)_metadata.*\.xlsx", filename).groups()[0]

        obj.update(
            {   "flow_cell_id": flow_cell_id,
                "library_id": row.library_id.split("/")[-1]
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
            file_info.get("library_id"),
            resource["flow_cell_id"],
        )



class BSDPacbioHifiMetadata(BSDBaseMetadata):
    ckan_data_type = "bsd-pacbio-hifi"
    technology = "pacbio-hifi"
    sequence_data_type = "pacbio-hifi"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*[\._]metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/bpa-sample-data/pacbio-hifi/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld(
                "library_id",
                re.compile(r"library_[Ii][Dd]|bioplatforms_library_id"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_id",
                re.compile(r"sample_[Ii][Dd]|bioplatforms_sample_id"),

                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
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
            fld('sequencing_kit_chemistry_version', 'sequencing_kit_chemistry_version', optional=True),
            fld('facility_project_code', 'facility_project_code', optional=True),
            fld('bioplatforms_project', 'bioplatforms_project', optional=True),
            fld('bait_set_name', 'bait_set_name', optional=True),
            fld('bait_set_reference', 'bait_set_reference', optional=True),
            fld('library_index_id_dual', 'library_index_id_dual', optional=True),
            fld('library_index_seq_dual', 'library_index_seq_dual', optional=True),
            fld('library_oligo_sequence_dual', 'library_oligo_sequence_dual', optional=True),
            fld('fast5_compression', 'fast5_compression', optional=True),
            fld('model_base_caller', 'model_base_caller', optional=True),
        ],
        "options": {
            "sheet_name": "Library metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.pacbio_hifi_filename_re,
                  files.pacbio_hifi_filename_2_re,
                  files.pacbio_hifi_metadata_sheet_re,
                  files.pacbio_hifi_common_re],

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

    def __init__(
            self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = BSDGoogleTrackMetadata(logger)

    def _get_packages(self):
        packages = self._get_common_packages()
        return self.apply_location_generalisation(packages)

    def _add_datatype_specific_info_to_package(self, obj, row, filename):

        obj.update(
            {
                 "dataset_url": self.get_tracking_info(row.ticket, "download")
             }
        )
        # below fields are in the metadata, but not required in the packages schema
        del obj["ccg_jira_ticket"]
        del obj["download"]
        del obj["file_count"]

        ingest_utils.add_spatial_extra(self._logger, obj)

    def _get_resource_info(self, metadata_info):
        auth_user, auth_env_name = self.auth
        ri_auth = (auth_user, get_password(auth_env_name))
        self._logger.info(metadata_info)

        for metadata_url in self.metadata_urls:
            self._logger.info("fetching resource metadata: %s" % (self.metadata_urls))
            fetcher = Fetcher(self._logger, self.path, metadata_url, ri_auth)
            fetcher.fetch_metadata_from_folder(
                [files.pacbio_hifi_filename_re, ],
                metadata_info,
                getattr(self, "metadata_url_components", []),
                download=False,
            )

    def _get_resources(self):
        resources = self._get_common_resources()
        return resources + self.generate_common_files_resources(resources)


    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # none for PACbio-hifi
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            (ingest_utils.extract_ands_id(self._logger, resource["library_id"]),
             resource["flowcell_id"],)
        )


    def _build_common_files_linkage(self, xlsx_info, resource, file_info):
        return (
            resource["flowcell_id"],
        )

class BSDONTPromethionMetadata(BSDBaseMetadata):
    ckan_data_type = "bsd-ont-promethion"
    technology = "ont-promethion"
    sequence_data_type = "ont-promethion"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/bpa_sample_data/promethion/",
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
                "filename": filename,    # this is removed, it is only added for resource linkage tracking.
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
"""
