import os
import re
from collections import defaultdict
from urllib.parse import urljoin

from glob import glob
from unipath import Path

from . import files
from .contextual import TSILibraryContextual, TSIDatasetControlContextual
from .tracking import TSIGoogleTrackMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...libs.fetch_data import Fetcher, get_password
from ...sensitive_species_wrapper import SensitiveSpeciesWrapper
from ...util import (
    sample_id_to_ckan_name,
    common_values,
    merge_values,
    apply_cc_by_license,
    clean_tag_name,
)

common_context = [TSILibraryContextual, TSIDatasetControlContextual]


class TSIBaseMetadata(BaseMetadata):
    initiative = "TSI"
    organization = "threatened-species"
    path = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # this method just for here for backwards compatibility
    def apply_location_generalisation(self, packages):
        # for TSI: lat and long determined by private/public lat and long from context metadata - no need to calculate separately
        for package in packages:
            package.update({"longitude": package.get("longitude")})
            package.update({"latitude": package.get("latitude")})
            package.update({"decimal_longitude_public": package.get("longitude")})
            package.update({"decimal_latitude_public": package.get("latitude")})
        return packages

    def _build_title_into_object(self, obj, field_value):
        if field_value is None:
            self.build_title_into_object(obj, {"initiative": self.initiative,
                                               "title_description": self.description, }
                                         )
        else:
            self.build_title_into_object(obj, {"initiative": self.initiative,
                                           "title_description": self.description,
                                           "field_value": field_value}
                                         )

    notes_mapping = [
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "voucher_or_tissue_number", "separator": " "},
        {"key": "country", "separator": " "},
        {"key": "state_or_region"},
    ]
    title_mapping = [
        {"key": "initiative", "separator": " "},
        {"key": "title_description", "separator": " "},
        {"key": "field_value", "separator": " "},
        {"key": "flowcell_id", "separator": ""},
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
                if not row.library_id and not row.flowcell_id:
                    # skip empty rows  -- do we want to do this for TSI??
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
                            self._logger, self.get_tracking_info(row.ticket, "date_of_transfer")
                        ),
                        "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                            self._logger, self.get_tracking_info(row.ticket, "date_of_transfer_to_archive")
                        ),
                     }
                )

                self._build_title_into_object(obj, library_id)
                self.build_notes_into_object(obj)
                self._add_datatype_specific_info_to_package(obj, row, fname)
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": "{:.100}".format(t)} for t in self.tag_names]
                packages.append(obj)
        return packages


class TSIIlluminaShortreadMetadata(TSIBaseMetadata):
    ckan_data_type = "tsi-illumina-shortread"
    technology = "illumina-shortread"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/tsi_staging/illumina-shortread/",
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
        self.google_track_meta = TSIGoogleTrackMetadata(logger)
    def _get_packages(self):
        packages = self._get_common_packages()
        return self.apply_location_generalisation(packages)

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        flow_cell_id = re.match(r"^.*_([^_]+)_metadata.*\.xlsx", filename).groups()[0]

        obj.update(
            {   "data_generated": True,
                "flow_cell_id": flow_cell_id,
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


class TSIIlluminaFastqMetadata(TSIBaseMetadata):
    ckan_data_type = "tsi-illumina-fastq"
    technology = "illumina-fastq"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/tsi_staging/illumina-fastq/",
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
            fld("work_order", "work_order", coerce=ingest_utils.get_int),
            fld("specimen_id", re.compile(r"specimen_[Ii][Dd]")),
            fld("tissue_number", "tissue_number"),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("data_custodian", "data_custodian"),
            fld("experimental_design", "experimental_design"),
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
            fld("insert_size_range", "insert_size_range"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("flowcell_type", "flowcell_type"),
            fld("flowcell_id", re.compile(r"flowcell_[Ii][Dd]")),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("file_name", "file_name", optional=True),
            fld("file_type", "file_type"),
            fld("library_strategy", "library_strategy"),
            fld("library_selection", "library_selection"),
            fld("library_source", "library_source"),
            fld("library_layout", "library_layout"),
            fld("sequencing_model", "sequencing_model"),
            fld("data_context", "data_context"),
            fld("facility_project_code", "facility_project_code", optional=True),
            fld('sequencing_kit_chemistry_version', 'sequencing_kit_chemistry_version', optional=True),
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
        "match": [files.illumina_fastq_re],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    title_mapping = [
        {"key": "initiative", "separator": " "},
        {"key": "title_description", "separator": " "},
        {"key": "field_value", "separator": " "},
        {"key": "flowcell_id", "separator": ""},
    ]
    description = "Illumina FastQ"
    tag_names = ["illumina-fastq"]

    def __init__(
            self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = TSIGoogleTrackMetadata(logger)

    def _get_packages(self):
        return self._get_common_packages()

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        obj["data_generated"] = True
        obj["scientific_name"] = "{} {}".format(
               obj["genus"], obj["species"]
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


class TSIPacbioHifiMetadata(TSIBaseMetadata):
    ckan_data_type = "tsi-pacbio-hifi"
    technology = "pacbio-hifi"
    sequence_data_type = "pacbio-hifi"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*[\._]metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/tsi_staging/pacbio-hifi/",
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
        "match": [files.pacbio_hifi_filename_re, files.pacbio_hifi_metadata_sheet_re],
        "skip": [
            re.compile(r"^.*[\._]metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    description = "Pacbio HiFi"
    title_mapping = [
        {"key": "initiative", "separator": " "},
        {"key": "title_description", "separator": " "},
        {"key": "field_value", "separator": " "},
    ]

    tag_names = ["pacbio-hifi"]

    def __init__(
            self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = TSIGoogleTrackMetadata(logger)

    def _get_packages(self):
        packages = self._get_common_packages()
        return self.apply_location_generalisation(packages)

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        filename_re = files.pacbio_hifi_metadata_sheet_re

        metadata_sheet_dict = re.match(
                filename_re, os.path.basename(filename)
            ).groupdict()
        metadata_sheet_flowcell_ids = []
        for f in ["flowcell_id", "flowcell2_id"]:
            if f in metadata_sheet_dict:
                    metadata_sheet_flowcell_ids.append(metadata_sheet_dict[f])

        if row.flowcell_id not in metadata_sheet_flowcell_ids:
            raise Exception(
                "The metadata row for library ID: {} has a flow cell ID of {}, which cannot be found in the metadata sheet name: {}".format(
                    row.library_id, row.flowcell_id, filename
                )
            )
        obj.update(
            {
                 "sample_submission_date": ingest_utils.get_date_isoformat(
                    self._logger, self.get_tracking_info(row.ticket, "date_of_transfer")
                ),
                "contextual_data_submission_date": None,
                "data_generated": ingest_utils.get_date_isoformat(
                    self._logger, self.get_tracking_info(row.ticket, "date_of_transfer_to_archive")
                ),
                "archive_ingestion_date": ingest_utils.get_date_isoformat(
                    self._logger, self.get_tracking_info(row.ticket, "date_of_transfer_to_archive")
                ),
                 "dataset_url": self.get_tracking_info(row.ticket, "download")
             }
        )
        # below fields are in the metadata, but not required in the packages schema
        del obj["ccg_jira_ticket"]
        del obj["date_of_transfer_to_archive"]
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
         return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # none for PACbio-hifi
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            (ingest_utils.extract_ands_id(self._logger, resource["library_id"]),
             resource["flowcell_id"],)
        )


class TSIGenomicsDDRADMetadata(TSIBaseMetadata):
    """
    This data conforms to the BPA Genomics ddRAD workflow.
    """

    ckan_data_type = "tsi-genomics-ddrad"
    omics = "genomics"
    technology = "ddrad"
    sequence_data_type = "illumina-ddrad"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/tsi_staging/ddrad/",
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
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status", optional=True),
            fld("sequencing_facility", "sequencing_facility"),
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("work_order", "work_order", coerce=ingest_utils.int_or_comment,),
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
            fld('sequencing_kit_chemistry_version', 'sequencing_kit_chemistry_version', optional=True),
            fld('facility_project_code', 'facility_project_code', optional=True),
            fld('library_index_id_dual', re.compile(r"(library_index_id_dual|library_dual_index_id)"), optional=True),
            fld('library_index_seq_dual', re.compile(r"(library_index_seq_dual|library_dual_index_seq)"),
                optional=True),
            fld('library_oligo_sequence_dual', re.compile(r"(library_oligo_sequence_dual|library_dual_oligo_sequence)"),
                optional=True),
            fld('fast5_compression', 'fast5_compression', optional=True),
            fld('model_base_caller', 'model_base_caller', optional=True),
            fld("bait_set_name", "bait_set_name", optional=True),
            fld("bait_set_reference", "bait_set_reference", optional=True),
            fld("bioplatforms_project", "bioplatforms_project", optional=True),
        ],
        "options": {
            "sheet_name": "Library metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.ddrad_fastq_filename_re, files.ddrad_metadata_sheet_re, files.ddrad_analysed_tar_re, ],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
        ],
    }
    notes_mapping = [
        {"key": "scientific_name", "separator": "\n"},
        {"key": "additional_notes"},
    ]
    title_mapping = [
        {"key": "initiative", "separator": " "},
        {"key": "title_description", "separator": " "},
        {"key": "bpa_dataset_id", "separator": " "},
        {"key": "field_value", "separator": " "},
    ]
    description = "Genomics ddRAD"
    tag_names = ["genomics-ddrad"]

    def __init__(
            self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = TSIGoogleTrackMetadata(logger)
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
                            self._logger, self.get_tracking_info(ticket, "date_of_transfer")),
                        "data_type": self.get_tracking_info(ticket, "data_type"),
                        "description": self.get_tracking_info(ticket, "description"),
                        "folder_name": self.get_tracking_info(ticket, "folder_name"),
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            self._logger, self.get_tracking_info(ticket, "date_of_transfer")
                        ),
                        "contextual_data_submission_date": None,
                        "data_generated": ingest_utils.get_date_isoformat(
                            self._logger, self.get_tracking_info(ticket, "date_of_transfer_to_archive")
                        ),
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, self.get_tracking_info(ticket, "date_of_transfer_to_archive")
                        ),
                        "dataset_url": self.get_tracking_info(ticket, "download"),
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                    }
                )
                obj.update(common_values(context_objs))
                obj.update(merge_values("scientific_name", " , ", context_objs))
                additional_notes = "ddRAD dataset not demultiplexed"
                self._build_title_into_object(obj, flow_id)
                self.build_notes_into_object(obj, {"additional_notes": additional_notes})
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        obj.update(
            {
                 "sample_submission_date": ingest_utils.get_date_isoformat(
                    self._logger, self.get_tracking_info(row.ticket, "date_of_transfer")
                ),
                "contextual_data_submission_date": None,
                "data_generated": ingest_utils.get_date_isoformat(
                    self._logger, self.get_tracking_info(row.ticket, "date_of_transfer_to_archive")
                ),
                "archive_ingestion_date": ingest_utils.get_date_isoformat(
                    self._logger, self.get_tracking_info(row.ticket, "date_of_transfer_to_archive")
                ),
                 "dataset_url": self.get_tracking_info(row.ticket, "download")
             }
        )
        # below fields are in the metadata, but not required in the packages schema

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # none for Genomics ddrad
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            (ingest_utils.extract_ands_id(
                self._logger, resource["bpa_dataset_id"]
            ),
             resource["flowcell_id"],)
        )


class TSIGenomeAssemblyMetadata(TSIBaseMetadata):
    ckan_data_type = "tsi-genome-assembly"
    technology = "genome-assembly"
    sequence_data_type = "genome-assembly"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/tsi_staging/assembly/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bioplatforms_secondarydata_id",)
    spreadsheet = {
        "fields": [
            fld(
                "bioplatforms_secondarydata_id",
                "bioplatforms_secondarydata_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample_id", "bioplatforms_sample_id", coerce=ingest_utils.int_or_comment),
            fld("library_id", "bioplatforms_library_id", coerce=ingest_utils.int_or_comment),
            fld("dataset_id", "bioplatforms_dataset_id", coerce=ingest_utils.get_int),
            fld("bioplatforms_project", "bioplatforms_project"),
            fld("contact_person", "contact_person"),
            fld("scientific_name", "scientific_name"),
            fld("common_name", "common_name"),
            fld("sequencing_technology", "sequencing_technology"),
            fld("genome_coverage", "genome_coverage"),
            fld("computational_infrastructure", "computational_infrastructure"),
            fld("system_used", "system_used"),
            fld("analysis_description", "analysis_description"),
            fld('assembly_date', 'assembly_date', coerce=ingest_utils.get_date_isoformat),
            fld('reference_genome', 'reference_genome'),
            fld('assembly_method', 'assembly_method'),
            fld('assembly_method_version', 'assembly_method_version'),
            fld('hybrid', 'hybrid'),
            fld('hybrid_details', 'hybrid_details'),
            fld('polishing_scaffolding_method', 'polishing_scaffolding_method'),
            fld('polishing_scaffolding_data', 'polishing_scaffolding_data'),
            fld('n_scaffolds', 'n_scaffolds'),
            fld('n50', 'n50'),
            fld('min_gap_length_bp', 'min_gap_length_bp'),
            fld('genome_size', 'genome_size'),
            fld('completion_score', 'completion_score'),
            fld('completion_score_method', 'completion_score_method')
        ],
        "options": {
            "sheet_name": "Metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.genome_assembly_filename_re],
        "skip": [re.compile(r"^.*\.xlsx$"), ],
    }
    notes_mapping = [
        {"key": "common_name", "separator": " "},
        {"key": "left-paren", "separator": ""},
        {"key": "scientific_name", "separator": ""},
        {"key": "right-paren"},
    ]
    title_mapping = [
        {"key": "initiative", "separator": " "},
        {"key": "title_description", "separator": " "},
        {"key": "bioplatforms_secondarydata_id", "separator": ""},
    ]
    description = "Genome Assembly"

    tag_names = ["tsi-genome-assembly"]

    def __init__(
            self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = TSIGoogleTrackMetadata(logger)

    def _get_packages(self):
        packages = self._get_common_packages()
        return self.apply_location_generalisation(packages)

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        bioplatforms_secondarydata_id = row.bioplatforms_secondarydata_id
        obj["name"] = sample_id_to_ckan_name(
            bioplatforms_secondarydata_id.split("/")[-1], self.ckan_data_type
        )
        obj["id"] = obj["name"]
        # explode sample_id, library_id
        sample_ids = re.split(",\s*", str(row.sample_id))
        library_ids = re.split(",\s*", str(row.library_id))

        # check same length
        if len(sample_ids) != len(library_ids):
            raise Exception("mismatch count of sample and library IDs")

        # if single item, add bpa_sample_id and bpa_library_id to metadata
        if len(sample_ids) == 1:
            obj["bpa_sample_id"] = ingest_utils.extract_ands_id(
                self._logger, row.sample_id
            )
            obj["bpa_library_id"] = ingest_utils.extract_ands_id(
                self._logger, row.library_id
            )
        else:
            obj["bpa_sample_id"] = None
            obj["bpa_library_id"] = None

        for contextual_source in self.contextual_metadata:
            context = []
            for i in range(0, len(sample_ids)):
                context.append(
                    contextual_source.get(
                        ingest_utils.extract_ands_id(
                            self._logger, sample_ids[i]
                        ),
                    )
                )
            obj.update(common_values(context))

        obj.update(
            {"data_generated": ingest_utils.get_date_isoformat(
                    self._logger, self.get_tracking_info(row.ticket, "date_of_transfer_to_archive")
             ),
             "folder_name": self.get_tracking_info(row.ticket, "folder_name"),
             "sample_submission_date": ingest_utils.get_date_isoformat(
                self._logger, self.get_tracking_info(row.ticket, "date_of_transfer")
             ),
             "contextual_data_submission_date": None,
             "archive_ingestion_date": ingest_utils.get_date_isoformat(
                self._logger, self.get_tracking_info(row.ticket, "date_of_transfer_to_archive")
             ),
             "dataset_url": self.get_tracking_info(row.ticket, "download"),
             }
        )
        self.build_notes_into_object(obj, {"left-paren": "(", "right-paren": ")"})
        # below fields are in the metadata, but not required in the packages schema
        del obj["ccg_jira_ticket"]
        del obj["date_of_transfer_to_archive"]
        del obj["download"]
        del obj["file_count"]


    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["bioplatforms_secondarydata_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["bioplatforms_secondarydata_id"]
        )
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (resource["bioplatforms_secondarydata_id"],
                )


class TSIHiCMetadata(TSIBaseMetadata):
    ckan_data_type = "tsi-hi-c"
    description = "Hi-C"
    technology = "hi-c"
    sequence_data_type = "illumina-hic"
    embargo_days = 365
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/tsi_staging/genomics-hi-c/",
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
    notes_mapping = [
        {"key": "library_id", "separator": "\n"},
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "voucher_or_tissue_number", "separator": " "},
        {"key": "country", "separator": " "},
        {"key": "state_or_region"},
    ]
    tag_names = ["genomics"]

    def __init__(
            self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = TSIGoogleTrackMetadata(logger)

    def _get_packages(self):
        return self._get_common_packages()

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        # note the complete library id is used to generate the notes field.
        obj.update(
            {"library_id": row.library_id.split("/")[-1],
             "data_generated": True
             }
        )
        self._build_title_into_object(obj, obj["sample_id"]) # this overrides the one set in the base class.

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["library_id"]
        )
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (xlsx_info["ticket"],
                file_info.get("library_id"),
                resource["flowcell_id"],
                )
class TSIGenomicsDArTMetadata(TSIBaseMetadata):
    """
    This data conforms to the BPA Genomics DArT workflow. future data
    will use this ingest class.
    """

    ckan_data_type = "tsi-genomics-dart"
    omics = "genomics"
    technology = "dart"
    sequence_data_type = "illumina-dart"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [
        r"^.*\.md5$",
        r"^.*\.xlsx$",
    ]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/tsi_staging/dart/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("dataset_id",)
    spreadsheet = {
        "fields": [
            fld("genus", "genus"),
            fld("species", "species"),
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
            fld("library_pcr_cycles", "library_pcr_cycles", coerce=ingest_utils.get_int),
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
            fld('bioplatforms_project', 'bioplatforms_project', optional=True),
            fld('bait_set_name', 'bait_set_name', optional=True),
            fld('bait_set_reference', 'bait_set_reference', optional=True),
        ],
        "options": {
            "sheet_name": "Library metadata",
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

    notes_mapping = [
        {"key": "organism_scientific_name", "separator": "\n"},
        {"key": "additional_notes"},
    ]
    title_mapping = [
        {"key": "initiative", "separator": " "},
        {"key": "title_description", "separator": " "},
        {"key": "dataset_id", "separator": ""},
    ]
    description = "DArT"
    tag_names = ["genomics-dart"]

    def __init__(
            self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = TSIGoogleTrackMetadata(logger)
    def _get_packages(self):
        self._logger.info("Ingesting {} metadata from {}".format(self.initiative, self.path))
        packages = []

        # this is a folder-oriented ingest, so we crush each xlsx down into a single row
        filename_re = re.compile(r"^TSI.*_(\d{5,6})_librarymetadata\.xlsx")

        objs = []
        flattened_objs = defaultdict(list)
        for fname in glob(self.path + "/*librarymetadata.xlsx"):
            row_objs = []
            self._logger.info("Processing {} metadata file {}".format(self.initiative, os.path.basename(fname)))
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

        for (fname, obj) in objs:
            ticket = obj["ticket"]

            name = sample_id_to_ckan_name(
                obj["dataset_id"], self.ckan_data_type,ticket
            )
            obj.update(
                {
                    "name": name,
                    "id": name,
                    # "bpa_dataset_id": bpa_dataset_id,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, self.get_tracking_info(ticket, "date_of_transfer")
                    ),
                    "data_type": self.get_tracking_info(ticket, "data_type"),
                    "description": self.get_tracking_info(ticket, "description"),
                    "folder_name": self.get_tracking_info(ticket, "folder_name"),
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, self.get_tracking_info(ticket, "date_of_transfer")
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, self.get_tracking_info(ticket, "date_of_transfer_to_archive")
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, self.get_tracking_info(ticket, "date_of_transfer_to_archive")
                    ),
                    "dataset_url": self.get_tracking_info(ticket, "download"),
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                }
            )
            organism_scientific_name = obj.get(
                "scientific_name",
                "%s %s" % (obj.get("genus", ""), obj.get("species", "")))
            additional_notes = "DArT dataset not demultiplexed"
            self._build_title_into_object(obj, "dataset_id")
            self.build_notes_into_object(obj, {"organism_scientific_name": organism_scientific_name,
                                               "additional_notes": additional_notes})
            ingest_utils.permissions_organization_member(self._logger, obj)
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
        return ingest_utils.extract_ands_id(self._logger, resource["dataset_id"]),
