from unipath import Path
from collections import defaultdict

from ...abstract import BaseMetadata

from ...util import sample_id_to_ckan_name, common_values
from urllib.parse import urljoin

from glob import glob

from ...libs import ingest_utils
from bpasslh.handler import SensitiveDataGeneraliser
from ...libs.excel_wrapper import make_field_definition as fld, make_skip_column as skp
from . import files
from .tracking import OMGTrackMetadata
from .contextual import OMGSampleContextual, OMGLibraryContextual
from ...libs.ingest_utils import get_clean_number

import os
import re

common_context = [OMGSampleContextual, OMGLibraryContextual]


class OMGBaseMetadata(BaseMetadata):
    def __init__(self, *args, **kwargs):
        self.generaliser = SensitiveDataGeneraliser()
        super().__init__(*args, **kwargs)

    def apply_location_generalisation(self, packages):
        "Apply location generalisation for sensitive species found from ALA"

        def species_name(package):
            return "{} {}".format(package.get("genus", ""), package.get("species", ""))

        # prime the cache of responses
        names = sorted(set(species_name(p) for p in packages))
        self.generaliser.ala_lookup.get_bulk(names)

        cache = {}
        for package in packages:
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
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
        ],
        "options": {"header_length": 1, "column_name_row_index": 0,},
    }
    md5 = {
        "match": [files.tenxtar_filename_re],
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
        self.track_meta = OMGTrackMetadata()
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
                "private": True,
            }
            # there must be only one ticket
            assert len(set(t.ticket for t in rows)) == 1

            ticket = rows[0].ticket
            track_meta = self.track_meta.get(ticket)

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            notes = "\n".join(
                "%s. %s." % (t.get("common_name", ""), t.get("institution_name", ""))
                for t in row_metadata
            )

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
                    "notes": notes,
                }
            )

            ingest_utils.add_spatial_extra(self._logger, obj)
            obj.update(common_values([make_row_metadata(row) for row in rows]))
            tag_names = ["10x-raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            self.track_xlsx_resource(obj, xlsx_fname)
            packages.append(obj)

        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info(
            "Ingesting OMG md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            for filename, md5, file_info in self.parse_md5file(md5_file):
                archive_name = self.file_package[filename]
                resource = file_info.copy()
                # waiting on filename convention from AGRF
                del resource["basename"]
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((archive_name,), legacy_url, resource))

        return resources + self.generate_xlsx_resources()


class OMG10XRawMetadata(OMGBaseMetadata):
    """
    this data conforms to the BPA 10X raw workflow. future data
    will use this ingest class.
    """

    organization = "bpa-omg"
    ckan_data_type = "omg-10x-raw"
    technology = "10xraw"
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
            fld("n_libraries_pooled", "n_libraries_pooled"),
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
        "match": [files.tenxfastq_filename_re],
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
        self.track_meta = OMGTrackMetadata()
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
                    "notes": "%s. %s."
                    % (
                        context.get("common_name", ""),
                        context.get("institution_name", ""),
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
                    "type": self.ckan_data_type,
                    "private": True,
                }
            )
            self.library_to_sample[obj["bpa_library_id"]] = obj["bpa_sample_id"]
            obj.update(context)
            ingest_utils.add_spatial_extra(self._logger, obj)
            tag_names = ["10x-raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            self.track_xlsx_resource(obj, fname)
            packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info(
            "Ingesting OMG md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                ticket = xlsx_info["ticket"]
                flow_id = self.flow_lookup[ticket]

                # FIXME: we have inconsistently named files, raise with Anna M after
                # urgent ingest complete.
                bpa_sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info["bpa_sample_id"]
                )
                if bpa_sample_id.split("/", 1)[1].startswith("5"):
                    # actually a library ID, map back
                    bpa_sample_id = file_info["bpa_sample_id"] = self.library_to_sample[
                        bpa_sample_id
                    ]

                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((bpa_sample_id, flow_id), legacy_url, resource))

        return resources + self.generate_xlsx_resources()


class OMG10XProcessedIlluminaMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-10x-processed-illumina"
    technology = "10xprocessed"
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
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
        ],
        "options": {"header_length": 1, "column_name_row_index": 0,},
    }
    md5 = {
        "match": [files.tenxtar_filename_re],
        "skip": [
            re.compile(r"^.*_processed\.xlsx$"),
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
        self.track_meta = OMGTrackMetadata()
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
                        "notes": "%s. %s."
                        % (
                            context.get("common_name", ""),
                            context.get("institution_name", ""),
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
                        "ticket": row.ticket,
                        "type": self.ckan_data_type,
                        "private": True,
                    }
                )
                obj.update(context)
                ingest_utils.add_spatial_extra(self._logger, obj)
                tag_names = ["10x-processed"]
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info(
            "Ingesting OMG md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                bpa_sample_id, flow_id = self.file_package[filename]
                resource = file_info.copy()
                # waiting on filename convention from AGRF
                del resource["basename"]
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((bpa_sample_id, flow_id), legacy_url, resource))

        return resources + self.generate_xlsx_resources()


class OMGExonCaptureMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-exon-capture"
    technology = "exoncapture"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
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
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
        ],
        "options": {"header_length": 1, "column_name_row_index": 0,},
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
        self.track_meta = OMGTrackMetadata()
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

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "title": "OMG Exon Capture Raw %s %s %s"
                        % (library_id, row.flowcell_id, row.library_index_sequence),
                        "notes": "%s. %s."
                        % (
                            context.get("common_name", ""),
                            context.get("institution_name", ""),
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
                        "type": self.ckan_data_type,
                        "private": True,
                    }
                )
                obj.update(context)

                # remove obsoleted fields
                obj.pop("library_index_id", False)
                obj.pop("library_index_sequence", False)
                obj.pop("library_oligo_sequence", False)

                ingest_utils.add_spatial_extra(self._logger, obj)
                tag_names = ["exon-capture", "raw"]
                obj["tags"] = [{"name": t} for t in tag_names]

                self.track_xlsx_resource(obj, fname)

                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info(
            "Ingesting OMG md5 file information from {0}".format(self.path)
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
                    self._logger, resource["bpa_library_id"]
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (library_id, resource["flow_cell_id"], resource["index"]),
                        legacy_url,
                        resource,
                    )
                )

        return resources + self.generate_xlsx_resources()


class OMGGenomicsNovaseqMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-novaseq"
    technology = "novaseq"
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
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("bpa_work_order", "bpa_work_order"),
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
        self.track_meta = OMGTrackMetadata()

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
                        "notes": "%s. %s."
                        % (
                            context.get("common_name", ""),
                            context.get("institution_name", ""),
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
                        "type": self.ckan_data_type,
                        "private": True,
                    }
                )
                obj.update(context)

                ingest_utils.add_spatial_extra(self._logger, obj)
                tag_names = ["novaseq", "genomics", "raw"]
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_xlsx_resource(obj, fname)

                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info(
            "Ingesting OMG md5 file information from {0}".format(self.path)
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
                    self._logger, resource["bpa_library_id"]
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (library_id, resource["flow_cell_id"], resource["index"]),
                        legacy_url,
                        resource,
                    )
                )
        return resources + self.generate_xlsx_resources()


class OMGGenomicsHiSeqMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-genomics-hiseq"
    omics = "genomics"
    technology = "hiseq"
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
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
        ],
        "options": {"header_length": 1, "column_name_row_index": 0,},
    }
    md5 = {
        "match": [files.hiseq_filename_re],
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
        self.track_meta = OMGTrackMetadata()

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
                        "notes": "%s. %s."
                        % (
                            context.get("common_name", ""),
                            context.get("institution_name", ""),
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
                        "type": self.ckan_data_type,
                        "private": True,
                    }
                )
                obj.update(context)
                ingest_utils.add_spatial_extra(self._logger, obj)
                tag_names = ["genomics-hiseq"]
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info(
            "Ingesting OMG md5 file information from {0}".format(self.path)
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
                                self._logger, resource["bpa_sample_id"]
                            ),
                            resource["flow_cell_id"],
                        ),
                        legacy_url,
                        resource,
                    )
                )
        return resources + self.generate_xlsx_resources()


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
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/nextseq_ddrad/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bpa_dataset_id", "flowcell_id")
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
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("bpa_work_order", "bpa_work_order", coerce=ingest_utils.get_int),
            fld("sequencing_platform", "sequencing_platform"),
            fld("sequence_length", "sequence_length"),
            fld("flowcell_id", "flowcell_id"),
            fld("software_version", "software_version"),
            fld("file", "file"),
            fld("library_pool_index_id", "library_pool_index_id"),
            fld("library_pool_index_sequence", "library_pool_index_sequence"),
            fld("library_pool_oligo_sequence", "library_pool_oligo_sequence"),
        ],
        "options": {
            "sheet_name": "OMG_library_metadata",
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
        self.track_meta = OMGTrackMetadata()
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
                objs[(obj["bpa_dataset_id"], obj["flowcell_id"])].append(obj)

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
                        "private": True,
                    }
                )
                ingest_utils.add_spatial_extra(self._logger, obj)
                tag_names = ["genomics-ddrad"]
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)
        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info(
            "Ingesting OMG md5 file information from {0}".format(self.path)
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


class OMGGenomicsPacbioMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-pacbio"
    technology = "pacbio"
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
            fld("n_libraries_pooled", "n_libraries_pooled"),
            fld("bpa_work_order", "bpa_work_order"),
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

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()

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
                    "notes": "{}. {}.".format(
                        context.get("common_name", ""),
                        context.get("institution_name", ""),
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
                    "type": self.ckan_data_type,
                    "private": True,
                }
            )
            obj.update(context)

            ingest_utils.add_spatial_extra(self._logger, obj)
            tag_names = ["pacbio", "genomics", "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            self.track_xlsx_resource(obj, fname)
            packages.append(obj)

        return self.apply_location_generalisation(packages)

    def _get_resources(self):
        self._logger.info(
            "Ingesting OMG md5 file information from {0}".format(self.path)
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
                    self._logger, resource["bpa_library_id"]
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


class OMGONTPromethionMetadata(OMGBaseMetadata):
    organization = "bpa-omg"
    ckan_data_type = "omg-ont-promethion"
    technology = "ont-promethion"
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
            fld("n_libraries_pooled", "n_libraries_pooled"),
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
        self.track_meta = OMGTrackMetadata()

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
                        "notes": "%s. %s."
                        % (obj.get("common_name", ""), obj.get("institution_name", "")),
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
                        "private": True,
                    }
                )
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
                resource["bpa_library_id"] = ingest_utils.extract_ands_id(
                    self._logger, resource["bpa_library_id"]
                )
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (resource["bpa_library_id"], resource["flowcell_id"]),
                        legacy_url,
                        resource,
                    )
                )
        return resources + self.generate_xlsx_resources()
