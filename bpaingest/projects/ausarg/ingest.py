import json
import os
import re
from glob import glob
from urllib.parse import urljoin

from unipath import Path

from .contextual import AusargLibraryContextual
from .tracking import AusArgGoogleTrackMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import sample_id_to_ckan_name, clean_tag_name
from . import files

common_context = [AusargLibraryContextual]


class AusargIlluminaFastqMetadata(BaseMetadata):
    organization = "bpa-plants"
    ckan_data_type = "ausarg-illumina-fastq"
    technology = "illumina-fastq"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/illumina-fastq/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld("library_id", "library_id"),
            fld("sample_id", "sample_id", coerce=ingest_utils.extract_ands_id),
            fld("dataset_id", "dataset_id"),
            fld("work_order", "work_order"),
            fld("specimen_id", "specimen_id"),
            fld("tissue_number", "tissue_number"),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("data_custodian", "data_custodian"),
            fld("experimental_design", "experimental design"),
            fld("ausarg_project", "ausarg_project"),
            fld("facility_sample_id", "facility_sample_id"),
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
            fld("library_status", "library_status"),
            fld("library_comments", "library_comments"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_seq", "library_index_seq"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("insert_size_range", "insert_size_range"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("flowcell_type", "flowcell_type"),
            fld("flowcell_id", "flowcell_id"),
            fld("cell_postion", "cell_postion"),
            fld("movie_length", "movie_length"),
            fld("analysis_software", "analysis_software"),
            fld("analysis_software_version", "analysis_software_version"),
        ],
        "options": {
            "sheet_name": None,
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
        {"key": "AusARG_project", "separator": ", "},
        {"key": "sequencing_platforms", "separator": " "},
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
        self._logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing GAP metadata file {0}".format(fname))
            metadata_sheet_flowcell_id = re.match(
                r"^.*_([^_]+)_metadata.*\.xlsx", fname
            ).groups()[0]
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            track_meta = self.google_track_meta.get(ticket)
            for row in rows:
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
                name = sample_id_to_ckan_name(
                    "{}".format(row.library_id),
                    self.ckan_data_type,
                    "{}".format(row.flowcell_id),
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(row.sample_id))
                notes = self.build_notes_without_blanks(obj)
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "data_generated": True,
                        "notes": notes,
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
                        (file_info.get("library_id"), resource["flowcell_id"],),
                        legacy_url,
                        resource,
                    )
                )
        return resources
