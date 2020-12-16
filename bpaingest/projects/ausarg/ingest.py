import json
import os
import re
from glob import glob
from urllib.parse import urljoin

from unipath import Path
from bpasslh.handler import SensitiveDataGeneraliser

from .contextual import AusargLibraryContextual
from .tracking import AusArgGoogleTrackMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...libs.fetch_data import Fetcher, get_password
from ...util import (
    sample_id_to_ckan_name,
    clean_tag_name,
    common_values,
    apply_cc_by_license,
)

from . import files

common_context = [AusargLibraryContextual]


class AusargBaseMetadata(BaseMetadata):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generaliser = SensitiveDataGeneraliser(self._logger)

    def apply_location_generalisation(self, packages):
        "Apply location generalisation for sensitive species found from ALA"

        def species_name(package):
            return "{} {}".format(package.get("genus", ""), package.get("species", ""))

        # prime the cache of responses
        self._logger.info("building location generalisation cache")
        names = sorted(set(species_name(p) for p in packages))
        self.generaliser.ala_lookup.get_bulk(names)

        cache = {}
        for package in packages:
            # if the sample wasn't collected in Australia, suppress the longitude
            # and latitude (ALA lookup via SSLH is irrelevant)
            country = package.get("country", "")
            if country.lower() != "australia":
                self._logger.debug(
                    "library_id {} outside Australia, suppressing location: {}".format(
                        package.get("dataset_id", ""), country
                    )
                )
                package.update({"latitude": None, "longitude": None})
                continue
            # Sample is in Australia; use ALA to determine whether it is sensitive,
            # and apply the relevant sensitisation level (if any)
            lat, lng = (
                get_clean_number(self._logger, package.get("latitude")),
                get_clean_number(self._logger, package.get("longitude")),
            )
            args = (species_name(package), lat, lng)
            if args not in cache:
                cache[args] = self.generaliser.apply(*args)
            generalised = cache[args]
            if generalised:
                package.update(generalised._asdict())

        return packages

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
            fld("experimental_design", "experimental design"),
            fld("ausarg_project", re.compile(r"[Aa]us[aA][rR][gG]_project")),
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
            fld("library_status", "library_status"),
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
                        "data_generated": True,
                        "notes": self.build_notes_without_blanks(obj),
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                tag_names = ["illumina-fastq"]
                scientific_name = obj.get("scientific_name", "").strip()
                if scientific_name:
                    tag_names.append(clean_tag_name(obj["scientific_name"]))
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


class AusargPacbioHifiMetadata(AusargBaseMetadata):
    organization = "ausarg"
    ckan_data_type = "ausarg-pacbio-hifi"
    technology = "pacbio-hifi"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
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
            fld("ausarg_project", re.compile(r"[Aa]us[aA][rR][gG]_project")),
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
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("sequencing_platform", "sequencing_platform"),
            fld("flowcell_id", "flowcell_id"),
            fld("experimental_design", "experimental design"),
            fld("voucher_number", "voucher_number", optional=True),
            fld("file_name", "file_name", optional=True),
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
                    self._logger,
                    resource["library_id"],
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
                    raise Exception("No download info for {}".format(filename))
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
