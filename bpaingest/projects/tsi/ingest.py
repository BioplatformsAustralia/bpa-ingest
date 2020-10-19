import json
import os
import re
from glob import glob
from urllib.parse import urljoin
from collections import defaultdict

from unipath import Path

from bpasslh.handler import SensitiveDataGeneraliser

from .contextual import TSILibraryContextual
from .tracking import TSIGoogleTrackMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import sample_id_to_ckan_name, clean_tag_name, common_values
from . import files

common_context = [TSILibraryContextual]


class TSIBaseMetadata(BaseMetadata):
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
                        package.get("bpa_dataset_id", ""), country
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


# VERIFY
class TSINovaseqMetadata(TSIBaseMetadata):
    organization = "threatened-species"
    # VERIFY
    ckan_data_type = "tsi-novaseq"
    technology = "novaseq"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        # VERIFY
        "https://downloads-qcif.bioplatforms.com/bpa/tsi_staging/tsi-novaseq/",
    ]
    metadata_url_components = ("ticket",)
    # FIX
    resource_linkage = ("bpa_library_id", "flowcell_id", "library_index_sequence")
    spreadsheet = {
        "fields": [
            # ADD library_ID
            fld(
                "library_id",
                re.compile(r"library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            # ADD sample_ID
            fld(
                "sample_id",
                re.compile(r"sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            # ADD dataset_ID
            fld(
                "dataset_id",
                re.compile(r"dataset_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            # ADD work_order
            fld("work_order", "work_order"),
            # ADD specimen_ID
            fld("specimen_id", re.compile(r"specimen_[Ii][Dd]")),
            # ADD tissue_number
            fld("tissue_number", "tissue_number"),
            # ADD data_context
            fld("data_context", "data_context"),
            # ADD library_layout
            fld("library_layout", "library_layout"),
            # ADD sequencing_model
            fld("sequencing_model", "sequencing_model"),
            # ADD insert_size_range
            fld("insert_size_range", "insert_size_range"),
            # ADD flowcell_type
            fld("flowcell_type", "flowcell_type"),
            # ADD cell_postion
            fld("cell_postion", "cell_postion"),
            # ADD movie_length
            fld("movie_length", "movie_length"),
            # ADD analysis_software
            fld("analysis_software", "analysis_software"),
            # ADD analysis_software_version
            fld("analysis_software_version", "analysis_software_version"),
            # ADD file_name QUERY - tracking??
            fld("file_name", "file_name"),
            # ADD file_type QUERY - tracking??
            fld("file_type", "file_type"),
            # ADD library_construction_protocol
            fld("library_construction_protocol", "library_construction_protocol"),
            # ADD library_strategy
            fld("library_strategy", "library_strategy"),
            # ADD library_selection
            fld("library_selection", "library_selection"),
            # ADD library_source
            fld("library_source", "library_source"),
            # genus
            fld("genus", "genus"),
            # species
            # FIX
            #           fld("species", "species"),
            # FIX
            #           fld("voucher_id", "voucher_id"),
            # FIX
            #           fld(
            #                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            #            ),
            # FIX
            #           fld(
            #                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            #            ),
            # FIX
            #           fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            # facility_sample_ID
            fld("facility_sample_id", "facility_sample_id"),
            # library_type
            fld("library_type", "library_type"),
            # library_prep_date
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            # library_prepared_by
            fld("library_prepared_by", "library_prepared_by"),
            # FIX
            #           fld("library_prep_method", "library_prep_method"),
            # experimental_design
            fld("experimental_design", "experimental_design"),
            # data_custodian
            fld("data_custodian", "data_custodian"),
            # DNA_treatment
            fld("dna_treatment", "dna_treatment"),
            # library_index_ID
            fld("library_index_id", "library_index_id"),
            # library_index_seq
            fld("library_index_sequence", "library_index_sequence"),
            # library_oligo_sequence
            fld("library_oligo_sequence", "library_oligo_sequence"),
            # library_pcr_reps
            fld("library_pcr_reps", "library_pcr_reps"),
            # library_pcr_cycles
            fld("library_pcr_cycles", "library_pcr_cycles"),
            # library_ng_ul
            fld("library_ng_ul", "library_ng_ul"),
            # library_comments
            fld("library_comments", "library_comments"),
            # library_location
            fld("library_location", "library_location"),
            # library_status
            fld("library_status", "library_status"),
            # sequencing_facility
            fld("sequencing_facility", "sequencing_facility"),
            # n_libraries_pooled
            fld("n_libraries_pooled", "n_libraries_pooled"),
            # FIX
            #           fld("bpa_work_order", "bpa_work_order"),
            # sequencing_platform
            fld("sequencing_platform", "sequencing_platform"),
            # FIX
            #           fld("sequence_length", "sequence_length"),
            # flowcell_ID
            fld("flowcell_id", "flowcell_id"),
            # FIX
            #           fld("software_version", "software_version"),
            # FIX
            #           fld("file", "file"),
        ],
        "options": {
            # VERIFY
            "sheet_name": "Library metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.novaseq_filename_re],
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
        self.track_meta = TSIGoogleTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting TSI metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing TSI metadata file {0}".format(os.path.basename(fname))
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                # VERIFY
                library_id = row.library_id
                if library_id is None:
                    continue

                obj = row._asdict()
                name = sample_id_to_ckan_name(
                    library_id, self.ckan_data_type, row.flowcell_id
                )

                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(row.sample_id, row.library_id))

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "title": "TSI Novaseq %s %s %s"
                        % (library_id, row.flowcell_id, row.library_index_sequence),
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
                # VERIFY
                tag_names = ["novaseq"]
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_xlsx_resource(obj, fname)

                packages.append(obj)
        # VERIFY
        # return self.apply_location_generalisation(packages)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting TSI md5 file information from {0}".format(self.path)
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
                    # VERIFY
                    self._logger,
                    resource["library_id"],
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                # VERIFY
                resources.append(
                    (
                        (library_id, resource["flow_cell_id"], resource["index"]),
                        legacy_url,
                        resource,
                    )
                )
        return resources + self.generate_xlsx_resources()


class TSIPacbioHifiMetadata(TSIBaseMetadata):
    organization = "threatened-species"
    # VERIFY
    ckan_data_type = "tsi-pacbio-hifi"
    technology = "pacbio-hifi"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        # VERIFY
        "https://downloads-qcif.bioplatforms.com/bpa/tsi_staging/tsi-pacbio-hifi/",
    ]
    metadata_url_components = ("ticket",)
    # FIX
    resource_linkage = ("bpa_library_id", "run_date")
    spreadsheet = {
        "fields": [
            # ADD library_ID
            fld(
                "library_id",
                re.compile(r"library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            # ADD sample_ID
            fld(
                "sample_id",
                re.compile(r"sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            # ADD dataset_ID
            fld(
                "dataset_id",
                re.compile(r"dataset_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            # ADD work_order
            fld("work_order", "work_order"),
            # ADD specimen_ID
            fld("specimen_id", re.compile(r"specimen_[Ii][Dd]")),
            # ADD tissue_number
            fld("tissue_number", "tissue_number"),
            # ADD data_context
            fld("data_context", "data_context"),
            # ADD library_layout
            fld("library_layout", "library_layout"),
            # ADD sequencing_model
            fld("sequencing_model", "sequencing_model"),
            # ADD insert_size_range
            fld("insert_size_range", "insert_size_range"),
            # ADD flowcell_type
            fld("flowcell_type", "flowcell_type"),
            # ADD cell_postion
            fld("cell_postion", "cell_postion"),
            # ADD movie_length
            fld("movie_length", "movie_length"),
            # ADD analysis_software
            fld("analysis_software", "analysis_software"),
            # ADD analysis_software_version
            fld("analysis_software_version", "analysis_software_version"),
            # ADD file_name QUERY - tracking??
            fld("file_name", "file_name"),
            # ADD file_type QUERY - tracking??
            fld("file_type", "file_type"),
            # ADD library_construction_protocol
            fld("library_construction_protocol", "library_construction_protocol"),
            # ADD library_strategy
            fld("library_strategy", "library_strategy"),
            # ADD library_selection
            fld("library_selection", "library_selection"),
            # ADD library_source
            fld("library_source", "library_source"),
            # genus
            fld("genus", "genus"),
            # species
            fld("species", "species"),
            # FIX
            #           fld("voucher_id", "voucher_id"),
            # FIX
            #           fld(
            #                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            #            ),
            # FIX
            #           fld(
            #                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            #            ),
            # FIX
            #           fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            # facility_sample_ID
            fld("facility_sample_id", "facility_sample_id"),
            # library_type
            fld("library_type", "library_type"),
            # library_prep_date
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            # library_prepared_by
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            # experimental_design
            fld("experimental_design", "experimental_design"),
            # data_custodian
            fld("data_custodian", "data_custodian"),
            # DNA_treatment
            fld("dna_treatment", "dna_treatment"),
            # library_index_ID
            fld("library_index_id", "library_index_id"),
            # library_index_seq
            # VERIFY
            fld("library_index_sequence", "library_index_sequence"),
            # library_oligo_sequence
            fld("library_oligo_sequence", "library_oligo_sequence"),
            # library_pcr_reps
            fld("library_pcr_reps", "library_pcr_reps"),
            # library_pcr_cycles
            fld("library_pcr_cycles", "library_pcr_cycles"),
            # library_ng_ul
            fld("library_ng_ul", "library_ng_ul"),
            # library_comments
            fld("library_comments", "library_comments"),
            # library_location
            fld("library_location", "library_location"),
            # library_status
            fld("library_status", "library_status"),
            # sequencing_facility
            fld("sequencing_facility", "sequencing_facility"),
            # n_libraries_pooled
            fld("n_libraries_pooled", "n_libraries_pooled"),
            # FIX
            #           fld("bpa_work_order", "bpa_work_order"),
            # sequencing_platform
            fld("sequencing_platform", "sequencing_platform"),
            # FIX
            #           fld("sequence_length", "sequence_length"),
            # flowcell_ID
            fld("flowcell_id", "flowcell_id"),
            # FIX
            #           fld("software_version", "software_version"),
            # FIX
            #           fld("file", "file"),
        ],
        "options": {
            # VERIFY
            "sheet_name": "Library metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.pacbio_hifi_filename_re],
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
        self.track_meta = TSIGoogleTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting TSI metadata from {0}".format(self.path))
        packages = []

        # VERIFY
        filename_re = re.compile(r"^TSI.*_(\d{8})_metadata\.xlsx")
        objs = []
        # this is a folder-oriented ingest, so we crush each xlsx down into a single row
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing TSI metadata file {0}".format(os.path.basename(fname))
            )

            xlsx_date = filename_re.match(os.path.basename(fname)).groups()[0]

            fname_obj = common_values(
                t._asdict() for t in self.parse_spreadsheet(fname, self.metadata_info)
            )
            fname_obj["run_date"] = xlsx_date
            objs.append((fname, fname_obj))

        for (fname, obj) in objs:
            track_meta = self.track_meta.get(obj["ticket"])

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            # VERIFY
            library_id = obj["library_id"]
            if library_id is None:
                continue

            name = sample_id_to_ckan_name(
                library_id, self.ckan_data_type, obj["run_date"]
            )

            context = {}
            for contextual_source in self.contextual_metadata:
                context.update(
                    # VERIFY
                    contextual_source.get(obj["sample_id"], obj["library_id"])
                )

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "title": "TSI Pacbio HiFi {} {}".format(
                        library_id, obj["run_date"]
                    ),
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
            # VERIFY
            tag_names = ["pacbio"]
            obj["tags"] = [{"name": t} for t in tag_names]
            self.track_xlsx_resource(obj, fname)
            packages.append(obj)

        # VERIFY
        # return self.apply_location_generalisation(packages)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting TSI md5 file information from {0}".format(self.path)
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
                    # VERIFY
                    self._logger,
                    resource["library_id"],
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (
                            ingest_utils.extract_ands_id(self._logger, library_id),
                            resource["run_date"],
                        ),
                        legacy_url,
                        resource,
                    )
                )
        return resources + self.generate_xlsx_resources()


class TSIGenomicsDDRADMetadata(TSIBaseMetadata):
    """
    This data conforms to the BPA Genomics ddRAD workflow.
    """

    organization = "threatened-species"
    ckan_data_type = "tsi-genomics-ddrad"
    omics = "genomics"
    technology = "ddrad"
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
            fld("dataset_id", "dataset_id", coerce=ingest_utils.extract_ands_id),
            fld("library_id", "library_id", coerce=ingest_utils.extract_ands_id),
            fld("sample_id", "sample_id", coerce=ingest_utils.extract_ands_id),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method", optional=True),
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
            fld("library_status", "library_status"),
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
            fld("file_name", "file_name"),
            fld("file_type", "file_type"),
        ],
        "options": {
            "sheet_name": "Library metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.ddrad_fastq_filename_re, files.ddrad_metadata_sheet_re,],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = TSIGoogleTrackMetadata()
        self.flow_lookup = {}

    def generate_notes_field(self, row_object):
        notes = "%s %s\nddRAD dataset not demultiplexed" % (
            row_object.get("genus", ""),
            row_object.get("species", ""),
        )
        return notes

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        self._logger.info("Ingesting TSI metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing TSI metadata file {0}".format(os.path.basename(fname))
            )
            flow_id = get_flow_id(fname)
            objs = defaultdict(list)
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                obj = row._asdict()
                obj.pop("file")
                objs[(obj["dataset_id"], obj["flowcell_id"])].append(obj)

            for (bpa_dataset_id, flowcell_id), row_objs in list(objs.items()):

                if bpa_dataset_id is None:
                    continue

                obj = common_values(row_objs)
                track_meta = self.track_meta.get(obj["ticket"])

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                name = sample_id_to_ckan_name(
                    bpa_dataset_id, self.ckan_data_type, flowcell_id
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "bpa_dataset_id": bpa_dataset_id,
                        "title": "TSI Genomics ddRAD %s %s" % (bpa_dataset_id, flow_id),
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer")
                        ),
                        "data_type": track_get("data_type"),
                        "description": track_get("description"),
                        "notes": self.generate_notes_field(obj),
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
                ingest_utils.add_spatial_extra(self._logger, obj)
                tag_names = ["genomics-ddrad"]
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info(
            "Ingesting TSI md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (
                            ingest_utils.extract_ands_id(
                                self._logger, resource["bpa_dataset_id"]
                            ),
                            resource["flowcell_id"],
                        ),
                        legacy_url,
                        resource,
                    )
                )
        return resources + self.generate_xlsx_resources()
