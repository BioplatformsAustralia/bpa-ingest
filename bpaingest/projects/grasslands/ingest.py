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
from .contextual import AGLibraryContextual,AGDatasetControlContextual
from .tracking import AGTrackMetadata

common_context = [AGLibraryContextual, AGDatasetControlContextual]
CONSORTIUM_ORG_NAME = "grassland-consortium-members"

class AGBaseMetadata(BaseMetadata):
    organization = "grasslands"
    initiative = "AG"
    initiative_prefix = "Grasslands"
    embargo_days = 365


    title_mapping = [
        {"key": "initiative_prefix", "separator": " "},
        {"key": "description", "separator": ", "},
        {"key": "project_aim", "separator": ", Sample ID "},
        {"key": "split_sample_id", "separator": ", "},
    ]

    notes_mapping = [
        {"key": "common_name", "separator": ", "},
        {"key": "scientific_name", "separator": ", "},
        {"key": "family", "separator": ", Location ID "},
        {"key": "location_id", "separator": ", "},
        {"key": "team_lead_name", "separator": ""},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = AGTrackMetadata(logger)

    def _build_title_into_object(self, obj):
        self.build_title_into_object(obj, {"initiative": self.initiative,
                                            "title_description": self.description,
                                            "split_sample_id": obj.get("sample_id", "").split("/")[-1], })

    def _get_common_packages(self):
        self._logger.info("Ingesting {} metadata from {}".format(self.initiative, self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing {} metadata file {}".format(self.initiative, os.path.basename(fname)))
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            if self.method_exists('_set_metadata_vars'):
                self._set_metadata_vars(fname)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                if not row.library_id and not row.flowcell_id:
                    # skip empty rows
                    continue
                sample_id = row.sample_id
                library_id = row.library_id
                dataset_id = row.dataset_id
                raw_library_id = library_id.split("/")[-1]
                raw_dataset_id = dataset_id.split("/")[-1]
                obj = row._asdict()
                if track_meta is not None:
                    if track_meta.dataset_id == raw_dataset_id:
                        obj.update(track_meta._asdict())
                    else:
                        self._logger.error("Mismatch between Tracking sheet dataset ID: {0} and  Metadata dataset ID: {1} in Ticket {2}"
                                           .format(track_meta.dataset_id, raw_dataset_id, ticket))
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
                        "initiative_prefix": self.initiative_prefix,
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
                self._build_title_into_object(obj)
                self.build_notes_into_object(obj)
                package_embargo_days = self.embargo_days
                if hasattr(obj, "access_control_date") and obj["access_control_date"] is not None:
                    package_embargo_days = obj["access_control_date"]
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "date_of_transfer_to_archive",
                    package_embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                packages.append(obj)
        return packages


class AGIlluminaShortreadMetadata(AGBaseMetadata):
    ckan_data_type = "grasslands-illumina-shortread"
    technology = "illumina-shortread"
    sequence_data_type = "illumina-shortread"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/grasslands/illumina-shortread/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "sample_id", "library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld('bioplatforms_project', 'bioplatforms_project'),
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
            fld('sample_replicate', 'sample_replicate'),
            fld('work_order', 'work_order'),
            fld('facility_project_code', 'facility_project_code'),
            fld('data_context', 'data_context'),
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
            fld('library_pcr_cycles', 'library_pcr_cycles',
                coerce=ingest_utils.date_or_int_or_comment,),
            fld('library_pcr_reps', 'library_pcr_reps',
                coerce=ingest_utils.date_or_int_or_comment,),
            fld('n_libraries_pooled', 'n_libraries_pooled',
                coerce=ingest_utils.date_or_int_or_comment,),
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
            "sheet_name": "Metadata",
            "header_length": 1,
            "column_name_row_index": 0,
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

    def _get_packages(self):
        return self._get_common_packages()

    def _add_datatype_specific_info_to_package(self, obj, row, filename):
        flowcell_id = re.match(r"^.*_([^_]+)_metadata.*\.xlsx", filename).groups()[0]

        obj.update(
            {"flowcell_id": flowcell_id,
             "library_id": row.library_id.split("/")[-1]  # because the linkages uses the raw lib id, from the file_name
             }
        )

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
            resource["flowcell_id"],
        )


class AGHiCMetadata(AGIlluminaShortreadMetadata):
    ckan_data_type = "grasslands-hi-c"
    description = "Hi-C"
    technology = "hi-c"
    sequence_data_type = "illumina-hic"
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/grasslands/genomics-hi-c/",
    ]
    tag_names = ["genomics", description.replace(" ", "-").lower()]

class AGPacbioHifiMetadata(AGBaseMetadata):
    ckan_data_type = "grasslands-pacbio-hifi"
    technology = "pacbio-hifi"
    sequence_data_type = "pacbio-hifi"
    description = "PacBio HiFi"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*(\.|_)metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/grasslands/pacbio-hifi/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "sample_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld('bioplatforms_project', 'bioplatforms_project'),
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
            fld('sample_replicate', 'sample_replicate'),
            fld('work_order', 'work_order'),
            fld('facility_project_code', 'facility_project_code'),
            fld('data_context', 'data_context'),
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
            fld('library_pcr_cycles', 'library_pcr_cycles',
                coerce=ingest_utils.date_or_int_or_comment, ),
            fld('library_pcr_reps', 'library_pcr_reps',
                coerce=ingest_utils.date_or_int_or_comment, ),
            fld('n_libraries_pooled', 'n_libraries_pooled',
                coerce=ingest_utils.date_or_int_or_comment, ),
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
            "sheet_name": "Metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.pacbio_hifi_filename_revio_re,
                  files.pacbio_hifi_revio_pdf_re,
                  files.pacbio_hifi_revio_metadata_sheet_re],
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
        metadata_sheet = re.search(
            revio_xls_filename_re, os.path.basename(filename)
        )
        metadata_sheet_dict = metadata_sheet.groupdict()
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
        name = sample_id_to_ckan_name(
                "{}".format(row.library_id.split("/")[-1]),
                self.ckan_data_type,
                "{}".format(row.flowcell_id),
        )
        track_meta = self.get_tracking_info(row.ticket)

        obj.update(
                    {  "id":name,
                       "name": name,
                       "dataset_id": row.dataset_id,
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, track_meta.date_of_transfer),
                        "data_type": track_meta.data_type,
                        "description": track_meta.description,
                        "folder_name": track_meta.folder_name,
                        "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                            self._logger, track_meta.date_of_transfer_to_archive
                        ),
                        "dataset_url": track_meta.download,
                    }
                )
        self.description = obj.get("description") # set the description for the title, it comes from the spreadsheet

# below fields are in the metadata, but not required in the packages schema
        del obj["ccg_jira_ticket"]
        del obj["download"]

    def _get_resources(self):
        resources = self._get_common_resources()
        return resources + self.generate_common_files_resources(resources)

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        if "sample_id" in resource:
            resource["sample_id"] = ingest_utils.extract_ands_id(
                self._logger, resource["sample_id"]
            )


    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
                xlsx_info["ticket"],
                resource["sample_id"],
                resource["flowcell_id"],
            )

    def _build_common_files_linkage(self, xlsx_info, resource, file_info):
        return (
            resource["flowcell_id"],
        )

