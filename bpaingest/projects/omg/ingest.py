import os
import re
from collections import defaultdict
from urllib.parse import urljoin

from glob import glob
from unipath import Path

from . import files
from .contextual import (
    OMGSampleContextual,
    OMGLibraryContextual,
    OMGDatasetControlContextual,
)
from .tracking import OMGTrackMetadata, OMGTrackGenomeAssemblyMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld, make_skip_column as skp
from ...secondarydata import SecondaryMetadata
from ...sensitive_species_wrapper import SensitiveSpeciesWrapper

from ...util import (
    sample_id_to_ckan_name,
    common_values,
    merge_values,
    apply_cc_by_license,
    clean_tag_name,
)

common_context = [
    OMGSampleContextual,
    OMGLibraryContextual,
    OMGDatasetControlContextual,
]

CONSORTIUM_ORG_NAME = "omg-consortium-members"


class OMGBaseMetadata(BaseMetadata):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generaliser = SensitiveSpeciesWrapper(self._logger)

    # this method just for here for backwards compatibility
    def apply_location_generalisation(self, packages):
        return self.generaliser.apply_location_generalisation(packages)
    notes_mapping = [
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "voucher_or_tissue_number", "separator": " "},
        {"key": "country", "separator": " "},
        {"key": "state_or_region"},
    ]

class OMG10XRawIlluminaMetadata(OMGBaseMetadata):
    """
    early run data, produced at AGRF.

    This data is unusual: it may contain more than one sample/library ID
    in a single tar file. It's been confirmed by AGRF that the data cannot
    be split by sample if this has happened.

    We use flow_id as the single key for resource linkage, and we then
    use the spreadsheet to determine the [library, sample, dataset] IDs
    for each tar file and present the metadata for each to the user.
    """

    organization = "bpa-omg"
    ckan_data_type = "omg-10x-raw-illumina"
    technology = "10x-raw-agrf"
    sequence_data_type = "illumina-10x"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_raw_agrf/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("archive_name",)
    spreadsheet = {
        "fields": [
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld("library_prep_date", "library_prep_date"),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
        ],
        "options": {
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.tenxtar_filename_re],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    tag_names = ["10x-raw"]
    notes_mapping = [
        {"key": "library_ids", "separator": "\n"},
        {"key": "mapped_rows"},
    ]
    row_mapping = [
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "voucher_or_tissue_number", "separator": " "},
        {"key": "country", "separator": " "},
        {"key": "state_or_region"},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)
        # each row in the spreadsheet maps through to a single tar file
        self.file_package = {}

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))

        def make_row_metadata(row):
            row_obj = {}
            context = {}
            for contextual_source in self.contextual_metadata:
                context.update(
                    contextual_source.get(row.bpa_sample_id, row.bpa_library_id)
                )
            row_obj.update(row._asdict())
            row_obj.update(context)
            return row_obj

        # glomp together the spreadsheet rows by filename
        fname_rows = defaultdict(list)

        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                fname_rows[(get_flow_id(fname), row.file, fname)].append(row)

        packages = []
        for (flow_id, fname, xlsx_fname), rows in fname_rows.items():
            name = sample_id_to_ckan_name(fname, self.ckan_data_type, flow_id)
            assert fname not in self.file_package
            self.file_package[fname] = fname
            row_metadata = [make_row_metadata(row) for row in rows]

            bpa_sample_ids = ", ".join([t.bpa_sample_id for t in rows])
            bpa_dataset_ids = ", ".join([t.bpa_dataset_id for t in rows])
            bpa_library_ids = ", ".join([t.bpa_library_id for t in rows])

            obj = {
                "name": name,
                "id": name,
                "flow_id": flow_id,
                "bpa_sample_ids": bpa_sample_ids,
                "bpa_library_ids": bpa_library_ids,
                "bpa_dataset_ids": bpa_dataset_ids,
                "title": "OMG 10x Illumina Raw %s %s" % (bpa_sample_ids, flow_id),
                "archive_name": fname,
                "type": self.ckan_data_type,
                "sequence_data_type": self.sequence_data_type,
                "license_id": apply_cc_by_license(),
            }
            # there must be only one ticket
            assert len(set(t.ticket for t in rows)) == 1

            ticket = rows[0].ticket
            track_meta = self.track_meta.get(ticket)

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            obj.update(
                {
                    "ticket": ticket,
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
                 }
            )
            mapped_rows = ". ".join(self.build_string_from_map_without_blanks(self.row_mapping, t) for t in row_metadata)

            self.build_notes_into_object(obj, {"library_ids": bpa_library_ids,
                                               "mapped_rows": mapped_rows})
            ingest_utils.add_spatial_extra(self._logger, obj)
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "archive_ingestion_date",
                self.embargo_days,
                CONSORTIUM_ORG_NAME,
            )
            ingest_utils.apply_access_control(self._logger, self, obj)
            obj.update(common_values([make_row_metadata(row) for row in rows]))
            obj["tags"] = [{"name": t} for t in self.tag_names]
            self.track_xlsx_resource(obj, xlsx_fname)
            packages.append(obj)

        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        del resource["basename"]

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            (resource["name"],)  # this is the archive name
                )


class OMG10XRawMetadata(OMGBaseMetadata):
    """
    this data conforms to the BPA 10X raw workflow. future data
    will use this ingest class.
    """

    organization = "bpa-omg"
    ckan_data_type = "omg-10x-raw"
    technology = "10xraw"
    sequence_data_type = "illumina-10x"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_raw/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_sample_id", "flow_id")
    spreadsheet = {
        "fields": [
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
            fld("genus", "genus", optional=True),
            fld("species", "species", optional=True),
            fld("voucher_id", "voucher_id", optional=True),
        ],
        "options": {
            "sheet_name": "OMG_library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.tenxfastq_filename_re],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    tag_names = ["10x-raw"]
    notes_mapping = [
        {"key": "bpa_library_id", "separator": "\n"},
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "voucher_or_tissue_number", "separator": " "},
        {"key": "country", "separator": " "},
        {"key": "state_or_region"},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)
        self.flow_lookup = {}
        self.library_to_sample = {}

    def _get_packages(self):
        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
            )

            # for this tech, each spreadsheet will only have a single BPA ID and flow cell
            # we grab the common values in the spreadsheet, then apply the flow cell ID
            # from the filename
            obj = common_values(
                [t._asdict() for t in self.parse_spreadsheet(fname, self.metadata_info)]
            )
            file_info = files.tenx_raw_xlsx_filename_re.match(
                os.path.basename(fname)
            ).groupdict()
            obj["flow_id"] = file_info["flow_id"]

            bpa_sample_id = obj["bpa_sample_id"]
            bpa_library_id = obj["bpa_library_id"]
            flow_id = obj["flow_id"]
            self.flow_lookup[obj["ticket"]] = flow_id

            name = sample_id_to_ckan_name(bpa_sample_id, self.ckan_data_type, flow_id)
            context = {}
            for contextual_source in self.contextual_metadata:
                context.update(contextual_source.get(bpa_sample_id, bpa_library_id))

            track_meta = self.track_meta.get(obj["ticket"])

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "bpa_sample_id": bpa_sample_id,
                    "title": "OMG 10x Raw %s %s" % (bpa_sample_id, flow_id),
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
                    "license_id": apply_cc_by_license(),
                }
            )
            self.library_to_sample[obj["bpa_library_id"]] = obj["bpa_sample_id"]
            obj.update(context)
            self.build_notes_into_object(obj)
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "archive_ingestion_date",
                self.embargo_days,
                CONSORTIUM_ORG_NAME,
            )
            ingest_utils.apply_access_control(self._logger, self, obj)
            ingest_utils.add_spatial_extra(self._logger, obj)
            obj["tags"] = [{"name": t} for t in self.tag_names]
            self.track_xlsx_resource(obj, fname)
            packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # no additional fields for OMG 10x Raw
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        ticket = xlsx_info["ticket"]
        flow_id = self.flow_lookup[ticket]
        # FIXME: we have inconsistently named files, raise with Anna M after
        # urgent ingest complete.
        bpa_sample_id = ingest_utils.extract_ands_id(
            self._logger, file_info["bpa_sample_id"]
        )

        if bpa_sample_id.split("/", 1)[1].startswith("5"):
            # actually a library ID, map back
            bpa_sample_id = self.library_to_sample[
                bpa_sample_id
            ]
            resource["bpa_sample_id"] = bpa_sample_id
        return (bpa_sample_id,
                flow_id)


class OMG10XProcessedIlluminaMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-10x-processed-illumina"
    technology = "10xprocessed"
    sequence_data_type = "illumina-10x"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_processed.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_processed/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_sample_id", "flow_id")
    spreadsheet = {
        "fields": [
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld("library_prep_date", "library_prep_date"),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
            fld("genus", "genus", optional=True),
            fld("species", "species", optional=True),
            fld("voucher_id", "voucher_id", optional=True),
        ],
        "options": {
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.tenxtar_filename_re],
        "skip": [
            re.compile(r"^.*_processed\.xlsx$"),
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    tag_names = ["10x-processed"]
    notes_mapping = [
        {"key": "bpa_library_id", "separator": "\n"},
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "voucher_or_tissue_number", "separator": " "},
        {"key": "country", "separator": " "},
        {"key": "state_or_region"},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)
        # each row in the spreadsheet maps through to a single tar file
        self.file_package = {}

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_processed.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)
                flow_id = get_flow_id(fname)

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                bpa_sample_id = row.bpa_sample_id
                bpa_library_id = row.bpa_library_id
                if bpa_sample_id is None:
                    continue
                obj = {}
                name = sample_id_to_ckan_name(
                    bpa_sample_id, self.ckan_data_type, flow_id
                )
                assert row.file not in self.file_package
                self.file_package[row.file] = bpa_sample_id, flow_id
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(bpa_sample_id, bpa_library_id))
                obj.update(row._asdict())
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "flow_id": flow_id,
                        "title": "OMG 10x Illumina Processed %s %s"
                        % (bpa_sample_id, flow_id),
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
                        "ticket": row.ticket,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                    }
                )
                obj.update(context)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # no additional fields for OMG 10x Raw
        del resource["basename"]
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        bpa_sample_id, flow_id = self.file_package[resource["name"]]
        return (bpa_sample_id,
                flow_id)


class OMGExonCaptureMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-exon-capture"
    technology = "exoncapture"
    sequence_data_type = "illumina-exoncapture"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_[mM]etadata.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/exon_capture/",
    ]
    metadata_url_components = (
        "facility",
        "ticket",
    )
    resource_linkage = ("bpa_library_id", "flowcell_id", "p7_library_index_sequence")
    spreadsheet = {
        "fields": [
            fld("genus", "genus", optional=True),
            fld("species", "species", optional=True),
            fld("voucher_id", "voucher_id", optional=True),
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_sample_id",
                re.compile(r"^(bpa_sample_id|bpasampleid)$"),
                coerce=ingest_utils.extract_ands_id,
            ),
            skp("plate_name"),
            skp("plate_well"),
            skp("voucher_number"),
            skp("tissue_number"),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
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
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
            fld("voucher_or_tissue_number", "voucher_or_tissue_number", optional=True),
        ],
        "options": {
            "sheet_name": "OMG_library_metadata",
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
    tag_names = ["exon-capture", "raw"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)
        self.linkage_xlsx = {}

    @classmethod
    def flow_cell_index_linkage(cls, flow_id, index):
        return flow_id + "_" + index.replace("-", "").replace("_", "")

    def _get_packages(self):
        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                library_id = row.bpa_library_id
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
                migrate_field("library_index_sequence", "p7_library_index_sequence"),
                migrate_field("library_oligo_sequence", "p7_library_oligo_sequence"),

                linkage = self.flow_cell_index_linkage(
                    row.flowcell_id, obj["p7_library_index_sequence"]
                )
                name = sample_id_to_ckan_name(library_id, self.ckan_data_type, linkage)

                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(
                        contextual_source.get(row.bpa_sample_id, row.bpa_library_id)
                    )

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
                            "OMG Exon Capture Raw %s %s %s"
                            % (library_id, row.flowcell_id, index_sequence)
                        ).rstrip(),
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
                        "license_id": apply_cc_by_license(),
                    }
                )
                obj.update(context)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)

                # remove obsoleted fields
                obj.pop("library_index_id", False)
                obj.pop("library_index_sequence", False)
                obj.pop("library_oligo_sequence", False)

                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]

                self.track_xlsx_resource(obj, fname)

                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # no additional fields for OMG Exon Capture
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        library_id = ingest_utils.extract_ands_id(
            self._logger, resource["bpa_library_id"])

        return(
            (library_id,
               resource["flow_cell_id"],
               resource["index"])
        )

class OMGWholeGenomeMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-novaseq-whole-genome"
    technology = "novaseq-whole-genome"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/whole_genome/",
    ]
    metadata_url_components = (
        "facility",
        "ticket",
    )
    resource_linkage = ("bpa_library_id", "flowcell_id", "p7_library_index_sequence")
    spreadsheet = {
        "fields": [
            fld("genus", "genus", optional=True),
            fld("species", "species", optional=True),
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_sample_id",
                re.compile(r"^(bpa_sample_id|bpasampleid)$"),
                coerce=ingest_utils.extract_ands_id,
            ),
            skp("plate_name"),
            skp("plate_well"),
            fld("voucher_number", "voucher_number"),
            skp("tissue_number"),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
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
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
            fld(
                "voucher_or_tissue_number",
                "voucher_or_tissue_number",
                optional=True,
                coerce=ingest_utils.int_or_comment,
            ),
        ],
        "options": {
            "sheet_name": "OMG_library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.whole_genome_filename_re],
        "skip": [
            re.compile(r"^.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    tag_names = ["whole-genome-resequence", "genomics"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)
        self.linkage_xlsx = {}

    @classmethod
    def flow_cell_index_linkage(cls, flow_id, index):
        return flow_id + "_" + index.replace("-", "").replace("_", "")

    def _get_packages(self):
        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                library_id = row.bpa_library_id
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
                migrate_field("library_index_sequence", "p7_library_index_sequence"),
                migrate_field("library_oligo_sequence", "p7_library_oligo_sequence"),

                linkage = self.flow_cell_index_linkage(
                    row.flowcell_id, obj["p7_library_index_sequence"]
                )
                name = sample_id_to_ckan_name(library_id, self.ckan_data_type, linkage)

                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(
                        contextual_source.get(row.bpa_sample_id, row.bpa_library_id)
                    )

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
                            "OMG Whole Genome %s %s %s"
                            % (library_id, row.flowcell_id, index_sequence)
                        ).rstrip(),
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
                        "license_id": apply_cc_by_license(),
                    }
                )
                obj.update(context)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)

                # remove obsoleted fields
                obj.pop("library_index_id", False)
                obj.pop("library_index_sequence", False)
                obj.pop("library_oligo_sequence", False)

                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]

                self.track_xlsx_resource(obj, fname)

                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # no additional fields for OMG Whole Genome Metadata
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        library_id = ingest_utils.extract_ands_id(
            self._logger, resource["bpa_library_id"])

        return (
            (library_id,
             resource["flow_cell_id"],
             resource["index"])
        )

class OMGGenomicsNovaseqMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-novaseq"
    technology = "novaseq"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/genomics-novaseq/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_library_id", "flowcell_id", "library_index_sequence")
    spreadsheet = {
        "fields": [
            fld("genus", "genus"),
            fld("species", "species"),
            fld("voucher_id", "voucher_id"),
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
        ],
        "options": {
            "sheet_name": "OMG_library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [
            files.novaseq_filename_re,
            files.novaseq_filename_2_re,
        ],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    tag_names = ["novaseq", "genomics", "raw"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)

    def _get_packages(self):
        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                library_id = row.bpa_library_id
                if library_id is None:
                    continue

                obj = row._asdict()
                name = sample_id_to_ckan_name(
                    library_id, self.ckan_data_type, row.flowcell_id
                )

                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(
                        contextual_source.get(row.bpa_sample_id, row.bpa_library_id)
                    )

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "title": "OMG Novaseq Raw %s %s %s"
                        % (library_id, row.flowcell_id, row.library_index_sequence),
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
                        "license_id": apply_cc_by_license(),
                    }
                )
                obj.update(context)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)

                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)

                packages.append(obj)
        return self.apply_location_generalisation(packages)


    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # no additional fields for OMG Genomics Novaseq Metadata
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        library_id = ingest_utils.extract_ands_id(
            self._logger, resource["bpa_library_id"])

        return (
            (library_id,
             resource["flow_cell_id"],
             resource["index"])
        )

class OMGGenomicsHiSeqMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-genomics-hiseq"
    omics = "genomics"
    technology = "hiseq"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/genomics/raw/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_sample_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld("library_prep_date", "library_prep_date"),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
        ],
        "options": {
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.hiseq_filename_re],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    notes_mapping = [
        {"key": "bpa_library_id", "separator": "\n"},
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "voucher_or_tissue_number", "separator": " "},
        {"key": "country", "separator": " "},
        {"key": "state_or_region"},
    ]
    tag_names = ["genomics-hiseq"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
            )
            flow_id = get_flow_id(fname)

            objs = defaultdict(list)
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                obj = row._asdict()
                obj.pop("file")
                objs[obj["bpa_sample_id"]].append(obj)

            for bpa_sample_id, row_objs in list(objs.items()):
                obj = common_values(row_objs)
                track_meta = self.track_meta.get(obj["ticket"])

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                bpa_sample_id = obj["bpa_sample_id"]
                bpa_library_id = obj["bpa_library_id"]
                if bpa_sample_id is None:
                    continue
                name = sample_id_to_ckan_name(
                    bpa_sample_id, self.ckan_data_type, flow_id
                )
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(bpa_sample_id, bpa_library_id))
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "flow_id": flow_id,
                        "title": "OMG Genomics HiSeq Raw %s %s"
                        % (bpa_sample_id, flow_id),
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
                        "license_id": apply_cc_by_license(),
                    }
                )
                obj.update(context)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # no additional fields for OMG Whole Genome Metadata
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            (ingest_utils.extract_ands_id(
                            self._logger, resource["bpa_sample_id"]
                        ),
             resource["flow_cell_id"])
        )


class OMGGenomicsDDRADMetadata(OMGBaseMetadata):
    """
    This data conforms to the BPA Genomics ddRAD workflow. future data
    will use this ingest class.
    Issue: bpa-archive-ops#699
    """

    organization = "bpa-omg"
    ckan_data_type = "omg-genomics-ddrad"
    omics = "genomics"
    technology = "ddrad"
    sequence_data_type = "illumina-ddrad"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/ddrad/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_dataset_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld("genus", "genus"),
            fld("species", "species"),
            fld("voucher_id", "voucher_id", optional=True),
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("plate_name", "plate_name"),
            fld("plate_well", "plate_well"),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
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
        ],
        "options": {
            "sheet_name": "OMG_library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [
            files.ddrad_fastq_filename_re,
            files.ddrad_metadata_sheet_re,
            files.ddrad_metadata_sheet_2_re,
        ],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^err$"),
            re.compile(r"^out$"),
            re.compile(r"^.*DataValidation\.pdf.*"),
        ],
    }
    tag_names = ["genomics-ddrad"]
    notes_mapping = [
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": "\n"},
        {"key": "additional_notes"},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)
        self.flow_lookup = {}

    def generate_notes_field(self, row_object):
        notes = "%s\nddRAD dataset not demultiplexed" % (
            row_object.get(
                "scientific_name",
                "%s %s" % (row_object.get("genus", ""), row_object.get("species", "")),
            ),
        )
        return notes

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
            )
            flow_id = get_flow_id(fname)
            objs = defaultdict(list)
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                obj = row._asdict()
                obj.pop("file")
                if not obj["bpa_dataset_id"] or not obj["flowcell_id"]:
                    continue
                objs[(obj["bpa_dataset_id"], obj["flowcell_id"])].append(obj)

            for (bpa_dataset_id, flowcell_id), row_objs in list(objs.items()):

                if bpa_dataset_id is None or flowcell_id is None:
                    continue

                context_objs = []
                for row in row_objs:
                    context = {}
                    for contextual_source in self.contextual_metadata:
                        context.update(
                            contextual_source.get(
                                row.get("bpa_sample_id"), row.get("bpa_libary_id")
                            )
                        )
                    context_objs.append(context)

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
                        "title": "OMG Genomics ddRAD %s %s" % (bpa_dataset_id, flow_id),
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
                        "license_id": apply_cc_by_license(),
                    }
                )
                obj.update(common_values(context_objs))
                obj.update(merge_values("scientific_name", " , ", context_objs))
                additional_notes = "ddRAD dataset not demultiplexed"
                self.build_notes_into_object(obj, {"additional_notes": additional_notes})
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # no additional fields for OMG ddrad Metadata
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            (ingest_utils.extract_ands_id(
                            self._logger, resource["bpa_dataset_id"]
                        ),
             resource["flowcell_id"])
        )


class OMGGenomicsPacbioMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-pacbio"
    technology = "pacbio"
    sequence_data_type = "pacbio-clr"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/pacbio/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_library_id", "run_date")
    spreadsheet = {
        "fields": [
            fld("genus", "genus"),
            fld("species", "species"),
            fld("voucher_id", "voucher_id"),
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
        ],
        "options": {
            "sheet_name": "OMG_library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.pacbio_filename_re],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    tag_names = ["pacbio", "genomics", "raw"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)

    def _get_packages(self):
        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []

        filename_re = re.compile(r"^OMG_.*_(\d{8})_metadata\.xlsx")
        objs = []
        # this is a folder-oriented ingest, so we crush each xlsx down into a single row
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
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

            library_id = obj["bpa_library_id"]
            if library_id is None:
                continue

            name = sample_id_to_ckan_name(
                library_id, self.ckan_data_type, obj["run_date"]
            )

            context = {}
            for contextual_source in self.contextual_metadata:
                context.update(
                    contextual_source.get(obj["bpa_sample_id"], obj["bpa_library_id"])
                )

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "title": "OMG Pacbio Raw {} {}".format(library_id, obj["run_date"]),
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
                    "license_id": apply_cc_by_license(),
                }
            )
            obj.update(context)
            self.build_notes_into_object(obj)
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "archive_ingestion_date",
                self.embargo_days,
                CONSORTIUM_ORG_NAME,
            )
            ingest_utils.apply_access_control(self._logger, self, obj)

            ingest_utils.add_spatial_extra(self._logger, obj)
            obj["tags"] = [{"name": t} for t in self.tag_names]
            self.track_xlsx_resource(obj, fname)
            packages.append(obj)

        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # no additional fields for OMG pacbio Metadata
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        library_id = ingest_utils.extract_ands_id(
            self._logger, resource["bpa_library_id"]
        )

        return (
                (  ingest_utils.extract_ands_id(self._logger, library_id),
                   resource["run_date"],)
        )

class OMGONTPromethionMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-ont-promethion"
    technology = "ont-promethion"
    sequence_data_type = "ont-promethion"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/ont-promethion/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld("genus", "genus"),
            fld("species", "species"),
            fld("voucher_id", "voucher_id", optional=True),
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id", optional=True),
            fld("library_index_sequence", "library_index_sequence", optional=True),
            fld("library_oligo_sequence", "library_oligo_sequence", optional=True),
            fld("library_pcr_reps", "library_pcr_reps", optional=True),
            fld("library_pcr_cycles", "library_pcr_cycles", optional=True),
            fld("library_ng_ul", "library_ng_ul", optional=True),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled",
                "n_libraries_pooled",
                optional=True,
                coerce=ingest_utils.get_int,
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length", optional=True),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file", optional=True),
            fld("insert_size_range", "insert_size_range", optional=True),
            fld("flowcell_type", "flowcell_type", optional=True),
            fld("cell_position", "cell_position", optional=True),
            fld("voucher_number", "voucher_number", optional=True),
            fld("tissue_number", "tissue_number", optional=True),
            fld("voucher_or_tissue_number", "voucher_or_tissue_number", optional=True),
        ],
        "options": {
            "sheet_name": "OMG_library_metadata",
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
    tag_names = ["ont-promethion"]
    notes_mapping = [
        {"key": "bpa_library_id", "separator": "\n"},
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "voucher_or_tissue_number", "separator": " "},
        {"key": "country", "separator": " "},
        {"key": "state_or_region"},
    ]


    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)

    def _get_packages(self):
        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing OMG metadata file {0}".format(fname))
            rows = self.parse_spreadsheet(fname, self.metadata_info)

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            for row in rows:
                track_meta = self.track_meta.get(row.ticket)
                bpa_library_id = row.bpa_library_id
                flowcell_id = row.flowcell_id
                obj = row._asdict()
                name = sample_id_to_ckan_name(
                    bpa_library_id.split("/")[-1], self.ckan_data_type, flowcell_id
                )

                for contextual_source in self.contextual_metadata:
                    obj.update(
                        contextual_source.get(
                            obj["bpa_sample_id"], obj["bpa_library_id"]
                        )
                    )

                obj.update(
                    {
                        "title": "OMG ONT PromethION {} {}".format(
                            obj["bpa_sample_id"], row.flowcell_id
                        ),
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
                        "license_id": apply_cc_by_license(),
                    }
                )
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["bpa_library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["bpa_library_id"]
        )
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return resource["bpa_library_id"], resource["flowcell_id"],


class OMGTranscriptomicsNextseq(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-transcriptomics-nextseq"
    omics = "transcriptomics"
    technology = "nextseq"
    sequence_data_type = "illumina-transcriptomics"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/transcriptomics_nextseq/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_library_id", "flowcell_id")
    spreadsheet = {
        "fields": [
            fld("genus", "genus"),
            fld("species", "species"),
            fld("voucher_number", "voucher_number"),
            fld("tissue_number", "tissue_number"),
            fld("voucher_or_tissue_number", "voucher_or_tissue_number"),
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            # each row has multiple dataset_ids separated by |
            fld("bpa_sample_id", "bpa_sample_id"),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
        ],
        "options": {
            "sheet_name": "OMG_library_metadata",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.transcriptomics_nextseq_fastq_filename_re],
        "skip": None,
    }
    tag_names = ["transcriptomics-nextseq"]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)
        self.flow_lookup = {}

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
            )
            flow_id = get_flow_id(fname)
            objs = defaultdict(list)
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                obj = row._asdict()
                obj.pop("file")
                objs[(obj["bpa_library_id"], obj["flowcell_id"])].append(obj)

            for (bpa_library_id, flowcell_id), row_objs in list(objs.items()):

                if bpa_library_id is None:
                    continue

                obj = common_values(row_objs)
                track_meta = self.track_meta.get(obj["ticket"])

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                name = sample_id_to_ckan_name(
                    bpa_library_id, self.ckan_data_type, flowcell_id
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "bpa_library_id": bpa_library_id,
                        "title": "OMG Transcriptomics NextSeq %s %s"
                        % (bpa_library_id, flow_id),
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
                        "license_id": apply_cc_by_license(),
                    }
                )
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        # no additional fields for OMG transcriptomicsMetadata
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return ((
                        ingest_utils.extract_ands_id(
                            self._logger, resource["bpa_library_id"]
                        ),
                        resource["flowcell_id"],
                    ),)


class OMGGenomicsPacBioGenomeAssemblyMetadata(SecondaryMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-pacbio-genome-assembly"
    technology = "pacbio-genome-assembly"
    sequence_data_type = "genome-assembly"
    embargo_days = 365
    contextual_classes = []
    metadata_patterns = [
        r"^.*\.md5$",
        r"^.*_metadata.*\.xlsx$",
        r"^.*raw_resources.json$",
    ]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/pacbio-secondary/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_library_id",)
    raw_resource_linkage = ("bpa_library_id", "run_date")
    spreadsheet = {
        "fields": [
            fld("filename", "filename", optional=True),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("seq_technology", "seq_technology"),
            fld("analysis_aim", "analysis_aim"),
            fld("assembly_method", "assembly method"),
            fld("hybrid", "hybrid"),
            fld("polishing_scaffolding_method", "polishing scaffolding method"),
            fld("polishing_scaffolding_data", "polishing scaffolding data"),
            fld(
                "assembly_method_version_or_date",
                "assembly method version or date",
                coerce=ingest_utils.date_or_str,
            ),
            fld("genome_coverage", "genome coverage"),
            fld("sequencing_technology", "sequencing technology"),
            fld("assembly_date", "assembly date", coerce=ingest_utils.date_or_str),
            fld("assembly_name", "assembly name"),
            fld(
                "full_or_partial_genome_in_the_sample",
                "full or partial genome in the sample :",
            ),
            fld("reference_genome", "reference genome"),
            fld("update", "update", coerce=ingest_utils.get_date_isoformat),
            fld("bacteria_available_from", "bacteria_available_from"),
            fld("computational_infrastructure", "computational_infrastructure"),
            fld("main_analysis_output", "main_analysis_output"),
            fld("no_scaffolds", "no. scaffolds"),
            fld("n50", "n50 (mb)", units="mb", coerce=ingest_utils.get_clean_number),
            fld("version_release_link", "version_release_link"),
            fld("contact_person", "contact_person"),
            fld("raw_resources", "raw resources"),
        ],
        "options": {
            "sheet_name": "Data_Genomes",
            "header_length": 2,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.pacbio_secondary_filename_re],
        "skip": [
            re.compile(r"^.*_metadata\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
        ],
    }
    tag_names = [
        "pacbio",
        "genomics",
        "genome assembly",
    ]
    raw = {"match": [files.pacbio_secondary_raw_filename_re], "skip": []}
    notes_mapping = [
        {"key": "name"},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=[], metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = OMGTrackGenomeAssemblyMetadata(logger)
        # self.create_metadata_info_for_raw_resources()

    def _get_packages(self):
        self._logger.info("Ingesting secondary OMG metadata from {0}".format(self.path))
        packages = []

        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Secondary (Genome assembly) metadata file {0}".format(
                    os.path.basename(fname)
                )
            )
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                obj = row._asdict()
                for key_identifier in [
                    "bpa_library_id",
                    "assembly_method_version_or_date",
                ]:
                    if not obj[key_identifier]:
                        raise Exception(
                            "A row does not contain {}, which is mandatory.".format(
                                key_identifier
                            )
                        )
                if track_meta is not None:

                    def track_get(k):
                        if track_meta is None:
                            return None
                        return getattr(track_meta, k)

                name = sample_id_to_ckan_name(
                    "{}".format(obj["bpa_library_id"].split("/")[-1]),
                    self.ckan_data_type,
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "title": "OMG Pacbio Genome Assembly {} {}".format(
                            obj["bpa_library_id"],
                            obj["assembly_method_version_or_date"],
                        ),
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer")
                        ),
                        "folder_name": track_get("folder_name"),
                        "description": track_get("description"),
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer")
                        ),
                        "contextual_data_submission_date": None,
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer_to_archive")
                        ),
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                    }
                )
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                self._logger.info(
                    "No context metadata for this data type, so no object merge....Continuing"
                )
                obj["tags"] = [{"name": "{:.100}".format(t)} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)

        return packages

    def _get_resources(self):
        """
        Note: This get_resources has not been refactored as the original code
        is broken. This datatype has not been used, and needs to be revisited
        before being used for ingests in the future. BCG 18/08/2022
        """
        self._logger.info(
            "Ingesting OMG md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = file_info.copy()
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            resource["resource_type"] = self.ckan_data_type
            library_id = ingest_utils.extract_ands_id(
                self._logger, resource["bpa_library_id"]
            )
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info["base_url"], filename)
            resources.append(
                (
                    (ingest_utils.extract_ands_id(self._logger, library_id),),
                    legacy_url,
                    resource,
                )
            )
        return (
            resources + self.generate_xlsx_resources() + self.generate_raw_resources()
        )


class OMGAnalysedDataMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-analysed-data"
    technology = "analysed-data"
    sequence_data_type = "analysed-data"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/analysed/",
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
            fld("sample_id", "sample_id", coerce=ingest_utils.int_or_comment),
            fld("sample_id_description", "sample_id_description"),
            fld("library_id", "library_id", coerce=ingest_utils.int_or_comment),
            fld("library_id_description", "library_id_description"),
            fld("dataset_id", "dataset_id", coerce=ingest_utils.get_int),
            fld("dataset_id_description", "dataset_id_description"),
            fld("bioplatforms_project", "bioplatforms_project"),
            fld("contact_person", "contact_person"),
            fld("scientific_name", "scientific_name"),
            fld("common_name", "common_name"),
            fld("dataset_context", "dataset_context"),
            fld("analysis_name", "analysis_name"),
            fld(
                "analysis_date", "analysis_date", coerce=ingest_utils.get_date_isoformat
            ),
            fld("reference_genome", "reference_genome"),
            fld("reference_genome_link", "reference_genome_link"),
            fld("sequencing_technology", "sequencing_technology"),
            fld("genome_coverage", "genome_coverage"),
            fld("analysis_method", "analysis_method"),
            fld("analysis_method_version", "analysis_method_version"),
            fld("version_method_version_link", "version_method_version_link"),
            fld("analysis_qc", "analysis_qc"),
            fld("computational_infrastructure", "computational_infrastructure"),
            fld("system_used", "system_used"),
            fld("analysis_description", "analysis_description"),
        ],
        "options": {
            "sheet_name": "fields",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }
    md5 = {
        "match": [files.analysed_data_filename_re],
        "skip": [
            re.compile(r"^.*\.xlsx$"),
        ],
    }
    tag_names = ["omg-analysed-data"]
    notes_mapping = [
        {"key": "common_name", "separator": " "},
        {"key": "left-paren", "separator": ""},
        {"key": "scientific_name", "separator": ""},
        {"key": "right-paren", "separator": ", "},
        {"key": "dataset_context"},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)

    def _get_packages(self):
        self._logger.info(
            "Ingesting OMG Analysed Data metadata from {0}".format(self.path)
        )
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing OMG metadata file {0}".format(fname))
            rows = self.parse_spreadsheet(fname, self.metadata_info)

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            for row in rows:
                track_meta = self.track_meta.get(row.ticket)
                bioplatforms_secondarydata_id = row.bioplatforms_secondarydata_id
                scientific_name = row.scientific_name
                obj = row._asdict()
                name = sample_id_to_ckan_name(
                    bioplatforms_secondarydata_id.split("/")[-1], self.ckan_data_type
                )

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
                                ingest_utils.extract_ands_id(
                                    self._logger, library_ids[i]
                                ),
                            )
                        )
                    obj.update(common_values(context))

                obj.update(
                    {
                        "title": "OMG Analysed Data {}".format(
                            obj["bioplatforms_secondarydata_id"]
                        ),
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
                        "license_id": apply_cc_by_license(),
                    }
                )

                self.build_notes_into_object(obj, {"left-paren": "(",
                                                   "right-paren": ")",
                                                   }
                                             )
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["bioplatforms_secondarydata_id"
        ] = ingest_utils.extract_ands_id(
            self._logger, resource["bioplatforms_secondarydata_id"]
        )
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):

        return (resource["bioplatforms_secondarydata_id"],
         )


class OMGGenomicsDArTMetadata(OMGBaseMetadata):
    """
    This data conforms to the BPA Genomics DArT workflow. future data
    will use this ingest class.
    """

    organization = "bpa-omg"
    ckan_data_type = "omg-genomics-dart"
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
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/dart/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_dataset_id",)
    spreadsheet = {
        "fields": [
            fld("genus", "genus"),
            fld("species", "species"),
            fld(
                "bpa_dataset_id", "bpa_dataset_id", coerce=ingest_utils.extract_ands_id
            ),
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("plate_name", "plate_name"),
            fld("plate_well", "plate_well"),
            fld("facility_sample_id", "facility_sample_id"),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
            fld("sequencing_facility", "sequencing_facility"),
            fld(
                "n_libraries_pooled", "n_libraries_pooled", coerce=ingest_utils.get_int
            ),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
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
        ],
        "options": {
            "sheet_name": "OMG_library_metadata",
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
    tag_names = ["genomics-dart"]
    notes_mapping = [
        {"key": "organism_scientific_name", "separator": "\n"},
        {"key": "additional_notes"},
    ]

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(logger)
        self.flow_lookup = {}

    def _get_packages(self):
        self._logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []

        # this is a folder-oriented ingest, so we crush each xlsx down into a single row
        filename_re = re.compile(r"^OMG_.*_(\d{5,6})_librarymetadata\.xlsx")

        objs = []
        flattened_objs = defaultdict(list)
        for fname in glob(self.path + "/*librarymetadata.xlsx"):
            row_objs = []
            self._logger.info(
                "Processing OMG metadata file {0}".format(os.path.basename(fname))
            )

            file_dataset_id = ingest_utils.extract_ands_id(
                self._logger, filename_re.match(os.path.basename(fname)).groups()[0]
            )

            for row in self.parse_spreadsheet(fname, self.metadata_info):
                obj = row._asdict()

                if not obj["bpa_dataset_id"]:
                    continue
                if file_dataset_id != obj["bpa_dataset_id"]:
                    self._logger.warn(
                        "Skipping metadata row related to unrelated dataset {0} (should be {1})".format(
                            obj["bpa_dataset_id"], file_dataset_id
                        )
                    )
                    continue

                # Add sample contextual metadata
                for contextual_source in self.contextual_metadata:
                    obj.update(
                        contextual_source.get(
                            obj.get("bpa_sample_id"), obj.get("bpa_libary_id")
                        )
                    )

                row_objs.append(obj)

            combined_obj = common_values(row_objs)
            combined_obj.update(merge_values("scientific_name", " , ", row_objs))

            objs.append((fname, combined_obj))

        for (fname, obj) in objs:
            track_meta = self.track_meta.get(obj["ticket"])

            def track_get(k):
                if track_meta is None:
                    self._logger.warn("Tracking data missing")
                    return None
                return getattr(track_meta, k)

            name = sample_id_to_ckan_name(
                obj["bpa_dataset_id"], self.ckan_data_type, obj["ticket"]
            )
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "title": "OMG DArT %s" % (obj["bpa_dataset_id"],),
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
                    "license_id": apply_cc_by_license(),
                }
            )
            organism_scientific_name = obj.get(
                "scientific_name",
                "%s %s" % (obj.get("genus", ""), obj.get("species", "")))
            additional_notes = "DArT dataset not demultiplexed"
            self.build_notes_into_object(obj, {"organism_scientific_name": organism_scientific_name,
                                               "additional_notes": additional_notes})
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "archive_ingestion_date",
                self.embargo_days,
                CONSORTIUM_ORG_NAME,
            )
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
                + ingest_utils.short_ands_id(self._logger, obj["bpa_dataset_id"])
                + "_samplemetadata_ingest.xlsx"
            ):
                self.track_xlsx_resource(obj, sample_metadata_file)
            packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        return self._get_common_resources() + self.generate_xlsx_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        def __dataset_id_from_md5_file(fname):
            fname = os.path.basename(fname)
            assert files.dart_md5_filename_re.match(fname) is not None
            md5match = files.dart_md5_filename_re.match(fname)
            assert "bpa_dataset_id" in md5match.groupdict()
            return md5match.groupdict()["bpa_dataset_id"]

        resource["bpa_dataset_id"] = __dataset_id_from_md5_file(md5_file)

        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):

        return (
                   (ingest_utils.extract_ands_id(
                    self._logger, resource["bpa_dataset_id"]
                        ),
                   )

         )
