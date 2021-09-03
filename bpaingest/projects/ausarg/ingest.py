import os
import re
from urllib.parse import urljoin

from glob import glob
from unipath import Path

from . import files
from .contextual import AusargLibraryContextual
from .tracking import AusArgGoogleTrackMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld, make_skip_column as skp
from ...libs.fetch_data import Fetcher, get_password
from ...sensitive_species_wrapper import SensitiveSpeciesWrapper
from ...util import (
    sample_id_to_ckan_name,
    clean_tag_name,
)

common_context = [AusargLibraryContextual]


class AusargBaseMetadata(BaseMetadata):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generaliser = SensitiveSpeciesWrapper(
            self._logger, package_id_keyname="dataset_id"
        )

    # this method just for here for backwards compatibility
    def apply_location_generalisation(self, packages):
        return self.generaliser.apply_location_generalisation(packages)

    def generate_notes_field(self, row_object):
        notes = "%s %s, %s %s %s" % (
            row_object.get("genus", ""),
            row_object.get("species", ""),
            row_object.get("voucher_or_tissue_number", ""),
            row_object.get("country", ""),
            row_object.get("state_or_region", ""),
        )
        return notes

    def generate_notes_field_with_id(self, row_object, id):
        notes = "%s\n%s %s, %s %s %s" % (
            id,
            row_object.get("genus", ""),
            row_object.get("species", ""),
            row_object.get("voucher_or_tissue_number", ""),
            row_object.get("country", ""),
            row_object.get("state_or_region", ""),
        )
        return notes

    def generate_notes_field_from_lists(self, row_list, ids):
        notes = "%s\n" % (ids)
        return notes + ". ".join(self.generate_notes_field(t) for t in row_list)


class AusargIlluminaFastqMetadata(AusargBaseMetadata):
    organization = "ausarg"
    ckan_data_type = "ausarg-illumina-fastq"
    technology = "illumina-fastq"
    sequence_data_type = "illumina-shortread"
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
            fld("work_order", "work_order"),
            fld("specimen_id", re.compile(r"specimen_[Ii][Dd]")),
            fld("tissue_number", "tissue_number"),
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
            fld('library_index_id_dual', 'library_index_id_dual', optional=True),
            fld('library_index_seq_dual', 'library_index_seq_dual', optional=True),
            fld('library_oligo_sequence_dual', 'library_oligo_sequence_dual', optional=True),
            fld("insert_size_range", "insert_size_range"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_pcr_reps", "library_pcr_reps"),
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
            fld('library_layout', 'library_layout', optional=True),
            fld('sequencing_model', 'sequencing_model', optional=True),
            fld('library_strategy', 'library_strategy', optional=True),
            fld('library_selection', 'library_selection', optional=True),
            fld('library_source', 'library_source', optional=True),
            fld('sequencing_kit_chemistry_version', 'sequencing_kit_chemistry_version', optional=True),
            fld('file_type', 'file_type', optional=True),
            fld('fast5_compression', 'fast5_compression', optional=True),
            fld('model_base_caller', 'model_base_caller', optional=True),
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
    notes_mapping = [
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "ausarg_project", "separator": ", "},
        {"key": "sequencing_platform", "separator": " "},
        {"key": "library_type", "separator": ", "},
        {"key": "state_or_origin", "separator": ", "},
        {"key": "country"},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = AusArgGoogleTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting AusARG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing AusARG metadata file {0}".format(fname))
            metadata_sheet_flowcell_id = re.match(
                r"^.*_([^_]+)_metadata.*\.xlsx", fname
            ).groups()[0]
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                if not row.library_id and not row.flowcell_id:
                    # skip empty rows
                    continue
                obj = row._asdict()
                if metadata_sheet_flowcell_id != row.flowcell_id:
                    raise Exception(
                        "The metadata row for library ID: {} has a flow cell ID of {}, which cannot be found in the metadata sheet name: {}".format(
                            row.library_id, row.flowcell_id, fname
                        )
                    )
                if track_meta is not None:
                    track_obj = track_meta._asdict()
                    tracking_folder_name = track_obj.get("folder_name", "")
                    if not re.search(row.flowcell_id, tracking_folder_name):
                        raise Exception(
                            "The metadata row for library ID: {} has a flow cell ID of {}, which cannot be found in the tracking field value: {}".format(
                                row.library_id, row.flowcell_id, tracking_folder_name
                            )
                        )
                    obj.update(track_obj)
                    # overwrite potentially incorrect values from tracking data - fail if source fields don't exist
                    obj["bioplatforms_sample_id"] = obj["sample_id"]
                    obj["bioplatforms_library_id"] = obj["library_id"]
                    obj["bioplatforms_dataset_id"] = obj["dataset_id"]
                    obj["scientific_name"] = "{} {}".format(
                        obj["genus"], obj["species"]
                    )
                name = sample_id_to_ckan_name(
                    "{}".format(row.library_id.split("/")[-1]),
                    self.ckan_data_type,
                    "{}".format(row.flowcell_id),
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(row.sample_id))
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "data_generated": True,
                        "notes": self.build_notes_without_blanks(obj),
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                tag_names = ["illumina-fastq"]
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
                resource["library_id"] = ingest_utils.extract_ands_id(
                    self._logger, resource["library_id"]
                )
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                # This will be used by sync/dump later to check resource_linkage in resources against that in packages
                resources.append(
                    (
                        (resource["library_id"], resource["flowcell_id"],),
                        legacy_url,
                        resource,
                    )
                )
        return resources


class AusargONTPromethionMetadata(AusargBaseMetadata):
    organization = "ausarg"
    ckan_data_type = "ausarg-ont-promethion"
    technology = "ont-promethion"
    sequence_data_type = "ont-promethion"
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
            fld("library_pcr_reps", "library_pcr_reps", optional=True),
            fld("library_pcr_cycles", "library_pcr_cycles", optional=True),
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
            fld("work_order", "work_order"),
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
            fld('library_layout', 'library_layout', optional=True),
            fld('sequencing_model', 'sequencing_model', optional=True),
            fld('library_strategy', 'library_strategy', optional=True),
            fld('library_selection', 'library_selection', optional=True),
            fld('library_source', 'library_source', optional=True),
            fld('sequencing_kit_chemistry_version', 'sequencing_kit_chemistry_version', optional=True),
            fld('file_type', 'file_type', optional=True),
            fld('fast5_compression', 'fast5_compression', optional=True),
            fld('model_base_caller', 'model_base_caller', optional=True),
        ],
        "options": {
            "sheet_name": "Library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
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
        self.track_meta = AusArgGoogleTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting AusARG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing AusARG metadata file {0}".format(fname))
            rows = self.parse_spreadsheet(fname, self.metadata_info)

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            for row in rows:
                track_meta = self.track_meta.get(row.ticket)
                bpa_library_id = row.library_id
                flowcell_id = row.flowcell_id
                obj = row._asdict()
                name = sample_id_to_ckan_name(
                    bpa_library_id.split("/")[-1], self.ckan_data_type, flowcell_id
                )

                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(obj["sample_id"]))

                obj.update(
                    {
                        "title": "AusARG ONT PromethION {} {}".format(
                            obj["sample_id"], row.flowcell_id
                        ),
                        "notes": self.generate_notes_field_with_id(obj, bpa_library_id),
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
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                tag_names = ["ont-promethion"]
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["library_id"] = ingest_utils.extract_ands_id(
                    self._logger, resource["library_id"]
                )
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (resource["library_id"], resource["flowcell_id"]),
                        legacy_url,
                        resource,
                    )
                )
        return resources + self.generate_xlsx_resources()


class AusargPacbioHifiMetadata(AusargBaseMetadata):
    organization = "ausarg"
    ckan_data_type = "ausarg-pacbio-hifi"
    technology = "pacbio-hifi"
    sequence_data_type = "pacbio-hifi"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*[\._]metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/pacbio-hifi/",
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
            fld("work_order", "work_order"),
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
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
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
            fld('library_layout', 'library_layout', optional=True),
            fld('sequencing_model', 'sequencing_model', optional=True),
            fld('library_strategy', 'library_strategy', optional=True),
            fld('library_selection', 'library_selection', optional=True),
            fld('library_source', 'library_source', optional=True),
            fld('sequencing_kit_chemistry_version', 'sequencing_kit_chemistry_version', optional=True),
            fld('file_type', 'file_type', optional=True),
        ],
        "options": {
            "sheet_name": "Library_metadata",
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

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = AusArgGoogleTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting AusARG metadata from {0}".format(self.path))
        packages = []

        filename_re = files.pacbio_hifi_metadata_sheet_re

        objs = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing AusARG metadata file {0}".format(os.path.basename(fname))
            )

            metadata_sheet_dict = re.match(
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
                    context.update(contextual_source.get(row.sample_id))

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "title": "AusARG Pacbio HiFi {}".format(row.library_id),
                        "notes": self.generate_notes_field(context),
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
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                obj.update(context)

                ingest_utils.add_spatial_extra(self._logger, obj)
                tag_names = ["pacbio-hifi"]
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)

        return self.apply_location_generalisation(packages)

    def _get_resource_info(self, metadata_info):
        auth_user, auth_env_name = self.auth
        ri_auth = (auth_user, get_password(auth_env_name))

        for metadata_url in self.metadata_urls:
            self._logger.info("fetching resource metadata: %s" % (self.metadata_urls))
            fetcher = Fetcher(self._logger, self.path, metadata_url, ri_auth)
            fetcher.fetch_metadata_from_folder(
                [files.pacbio_hifi_filename_re,],
                metadata_info,
                getattr(self, "metadata_url_components", []),
                download=False,
            )

    def _get_resources(self):
        self._logger.info(
            "Ingesting AusARG md5 file information from {0}".format(self.path)
        )
        resources = []
        resource_info = {}
        self._get_resource_info(resource_info)

        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                library_id = ingest_utils.extract_ands_id(
                    self._logger, resource["library_id"],
                )
                #
                raw_resources_info = resource_info.get(os.path.basename(filename), "")
                # if download_info exists for raw_resources, then use remote URL
                if raw_resources_info:
                    legacy_url = urljoin(
                        raw_resources_info["base_url"], os.path.basename(filename)
                    )
                else:
                    # otherwise if no download_info, then raise error
                    raise Exception("No download info for {} in {}".format(filename, md5_file))
                resources.append(
                    (
                        (
                            ingest_utils.extract_ands_id(self._logger, library_id),
                            resource["flowcell_id"],
                        ),
                        legacy_url,
                        resource,
                    )
                )
        return resources


class AusargExonCaptureMetadata(AusargBaseMetadata):
    organization = "ausarg"
    ckan_data_type = "ausarg-exon-capture"
    technology = "exoncapture"
    sequence_data_type = "illumina-exoncapture"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*[lL]ibrary[mM]etadata.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/exon_capture/",
    ]
    metadata_url_components = (
        "facility",
        "ticket",
    )
    resource_linkage = ("library_id", "flowcell_id", "p7_library_index_sequence")
    spreadsheet = {
        "fields": [
            fld("genus", "genus", optional=True),
            fld("species", "species", optional=True),
            fld("voucher_id", "voucher_id", optional=True),
            fld("dataset_id", "dataset_id", coerce=ingest_utils.extract_ands_id),
            fld("library_id", "library_id", coerce=ingest_utils.extract_ands_id),
            fld("sample_id", "sample_id", coerce=ingest_utils.extract_ands_id,),
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
            fld("library_index_sequence", "library_index_sequence", optional=True),
            fld("library_oligo_sequence", "library_oligo_sequence", optional=True),
            fld("p7_library_index_id", "p7_library_index_id", optional=True),
            fld(
                "p7_library_index_sequence", "p7_library_index_sequence", optional=True
            ),
            fld(
                "p7_library_oligo_sequence", "p7_library_oligo_sequence", optional=True
            ),
            fld("p5_library_index_id", "p5_library_index_id", optional=True),
            fld(
                "p5_library_index_sequence", "p5_library_index_sequence", optional=True
            ),
            fld(
                "p5_library_oligo_sequence", "p5_library_oligo_sequence", optional=True
            ),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
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
            fld("work_order", "work_order"),
            fld("facility_project_code", "facility_project_code"),
            fld("specimen_id", "specimen_id"),
            fld("tissue_number", "tissue_number"),
            fld("data_context", "data_context"),
            fld("library_layout", "library_layout"),
            fld("sequencing_model", "sequencing_model"),
            fld("library_construction_protocol", "library_construction_protocol"),
            fld("library_strategy", "library_strategy"),
            fld("library_selection", "library_selection"),
            fld("library_source", "library_source"),
            fld("library_index_seq_p7", "library_index_seq_p7"),
            fld("library_oligo_sequence_p7", "library_oligo_sequence_p7"),
            fld("insert_size_range", "insert_size_range"),
            fld("flowcell_type", "flowcell_type"),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("sequencing_kit_chemistry_version", "sequencing_kit_chemistry_version"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
            fld("file_type", "file_type"),
            fld("library_index_seq_p5", "library_index_seq_p5"),
            fld("library_oligo_sequence_p5", "library_oligo_sequence_p5"),
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

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = AusArgGoogleTrackMetadata()
        self.linkage_xlsx = {}

    @classmethod
    def flow_cell_index_linkage(cls, flow_id, index):
        return flow_id + "_" + index.replace("-", "").replace("_", "")

    def _get_packages(self):
        self._logger.info("Ingesting AusARG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing AusARG metadata file {0}".format(os.path.basename(fname))
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                library_id = row.library_id
                if library_id is None:
                    continue

                obj = row._asdict()

                def migrate_field(from_field, to_field):
                    old_val = obj[from_field]
                    new_val = obj[to_field]
                    del obj[from_field]
                    if old_val is not None and new_val is not None:
                        raise Exception(
                            "field migration clash, {}->{}".format(from_field, to_field)
                        )
                    if old_val:
                        obj[to_field] = old_val

                # library_index_sequence migrated into p7_library_index_sequence
                migrate_field("library_index_id", "p7_library_index_id"),
                migrate_field("library_index_seq_p7", "p7_library_index_sequence"),
                migrate_field("library_oligo_sequence_p7", "p7_library_oligo_sequence"),

                linkage = self.flow_cell_index_linkage(
                    row.flowcell_id, obj["p7_library_index_sequence"]
                )

                name = sample_id_to_ckan_name(library_id, self.ckan_data_type, linkage)

                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(row.sample_id))

                def cleanstring(s):
                    if s is not None:
                        return s
                    else:
                        return ""

                index_sequence = cleanstring(obj["p7_library_index_sequence"])

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "title": (
                            "AusARG Exon Capture Raw %s %s %s"
                            % (library_id, row.flowcell_id, index_sequence)
                        ).rstrip(),
                        "notes": self.generate_notes_field(context),
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
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                obj.update(context)

                # remove obsoleted fields
                obj.pop("library_index_id", False)
                obj.pop("library_index_seq_p7", False)
                obj.pop("library_oligo_sequence_p7", False)

                ingest_utils.add_spatial_extra(self._logger, obj)
                tag_names = ["exon-capture", "raw"]
                obj["tags"] = [{"name": t} for t in tag_names]

                self.track_xlsx_resource(obj, fname)

                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info(
            "Ingesting AusARG md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                library_id = ingest_utils.extract_ands_id(
                    self._logger, resource["library_id"]
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (library_id, resource["flowcell_id"], resource["index"]),
                        legacy_url,
                        resource,
                    )
                )

        return resources + self.generate_xlsx_resources()


class AusargHiCMetadata(AusargBaseMetadata):
    organization = "ausarg"
    ckan_data_type = "ausarg-hi-c"
    description = "Hi-C"
    technology = "hi-c"
    sequence_data_type = "illumina-hic"
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
            fld('library_index_id_dual', 'library_index_id_dual', optional=True),
            fld('library_index_seq_dual', 'library_index_seq_dual', optional=True),
            fld('library_oligo_sequence_dual', 'library_oligo_sequence_dual', optional=True),
            fld('fast5_compression', 'fast5_compression', optional=True),
            fld('model_base_caller', 'model_base_caller', optional=True)
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

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = AusArgGoogleTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting AusARG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing AusARG metadata file {0}".format(fname))
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
                    raw_library_id, self.ckan_data_type, row.flowcell_id
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id))
                obj.update(
                    {
                        "title": "AusARG Hi-C {} {}".format(
                            obj["sample_id"], row.flowcell_id
                        ),
                        "sample_id": sample_id,
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "flowcell_id": row.flowcell_id,
                        "data_generated": True,
                        "library_id": raw_library_id,
                        "notes": self.generate_notes_field_with_id(obj, library_id),
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                tag_names = ["genomics"]
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
                resource["library_id"] = ingest_utils.extract_ands_id(
                    self._logger, resource["library_id"]
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
                            file_info.get("library_id"),
                            resource["flowcell_id"],
                        ),
                        legacy_url,
                        resource,
                    )
                )
        return resources
