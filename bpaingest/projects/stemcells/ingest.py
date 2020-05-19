from unipath import Path
from urllib.parse import urljoin
from collections import defaultdict
from hashlib import md5 as md5hash

from ...libs import ingest_utils
from ...util import sample_id_to_ckan_name, common_values, clean_tag_name
from ...abstract import BaseMetadata
from ...libs.excel_wrapper import (
    ExcelWrapper,
    make_field_definition as fld,
    make_skip_column as skp,
)
from .tracking import StemcellsTrackMetadata
from .contextual import (
    StemcellsTranscriptomeContextual,
    StemcellsSmallRNAContextual,
    StemcellsSingleCellRNASeq,
    StemcellsMetabolomicsContextual,
    StemcellsProteomicsContextual,
)
from . import files
from .util import fix_analytical_platform
from glob import glob

import os
import re

common_skip = [
    re.compile(r"^._metadata\.xlsx$"),
    re.compile(r"^.*_Report\.pdf"),
]


def parse_sample_id_range(logger, s):
    return s.strip().split("/")[-1]


class StemcellsTranscriptomeMetadata(BaseMetadata):
    contextual_classes = [StemcellsTranscriptomeContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/stemcell/raw/transcriptome/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata\.xlsx"]
    omics = "transcriptomics"
    organization = "bpa-stemcells"
    ckan_data_type = "stemcells-transcriptomics"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                re.compile(r"^.*sample unique id$"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample_extaction_id", "Sample extraction ID"),
            fld("insert_size_range", "Insert size range"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld("analysis_software_version", "CASAVA version"),
        ],
        "options": {"header_length": 2, "column_name_row_index": 1,},
    }
    md5 = {"match": [files.transcriptome_filename_re], "skip": common_skip}

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellsTrackMetadata()

    def _get_packages(self):
        self._logger.info(
            "Ingesting Stemcells Transcriptomics metadata from {0}".format(self.path)
        )
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and sample_id is the primary key
        all_rows = set()
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Stemcells Transcriptomics metadata file {0}".format(fname)
            )
            all_rows.update(self.parse_spreadsheet(fname, self.metadata_info))
        for row in all_rows:
            sample_id = row.sample_id
            if sample_id is None:
                continue
            obj = {}
            name = sample_id_to_ckan_name(sample_id.split("/")[-1], self.ckan_data_type)
            track_meta = self.track_meta.get(row.ticket)
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "sample_id": sample_id,
                    "notes": "Stemcell Transcriptomics %s" % (sample_id),
                    "title": "Stemcell Transcriptomics %s" % (sample_id),
                    "omics": "transcriptomics",
                    "insert_size_range": row.insert_size_range,
                    "library_construction_protocol": row.library_construction_protocol,
                    "sequencer": row.sequencer,
                    "analysis_software_version": row.analysis_software_version,
                    "ticket": row.ticket,
                    "facility": row.facility_code.upper(),
                    "type": self.ckan_data_type,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "private": True,
                }
            )
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(sample_id))
            tag_names = ["transcriptome", "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((sample_id,), legacy_url, resource))
        return resources


class StemcellsSmallRNAMetadata(BaseMetadata):
    contextual_classes = [StemcellsSmallRNAContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/stemcell/raw/small_rna/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata\.xlsx"]
    organization = "bpa-stemcells"
    technology = "smallrna"
    ckan_data_type = "stemcells-smallrna"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                re.compile(r"^.*sample unique id$"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample_extaction_id", "Sample extraction ID"),
            fld("insert_size_range", "Insert size range"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld("analysis_software_version", "CASAVA version"),
        ],
        "options": {"header_length": 2, "column_name_row_index": 1,},
    }
    md5 = {"match": [files.smallrna_filename_re], "skip": common_skip}

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellsTrackMetadata()

    def _get_packages(self):
        self._logger.info(
            "Ingesting Stemcells SmallRNA metadata from {0}".format(self.path)
        )
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and sample_id is the primary key
        all_rows = set()
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Stemcells SmallRNA metadata file {0}".format(fname)
            )
            all_rows.update(self.parse_spreadsheet(fname, self.metadata_info))
        for row in all_rows:
            sample_id = row.sample_id
            if sample_id is None:
                continue
            obj = {}
            name = sample_id_to_ckan_name(sample_id.split("/")[-1], self.ckan_data_type)
            track_meta = self.track_meta.get(row.ticket)
            self._logger.info("track_meta is {}".format(track_meta))
            if not track_meta:
                self._logger.warning("No track meta data available. Continuing...")
            else:
                self._logger.debug(
                    "Tracking meta for ticket: {} with track_meta: {}".format(
                        row.ticket, track_meta
                    )
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_id": sample_id,
                        "notes": "Stemcell SmallRNA %s" % (sample_id),
                        "title": "Stemcell SmallRNA %s" % (sample_id),
                        "omics": "transcriptomics",
                        "insert_size_range": row.insert_size_range,
                        "library_construction_protocol": row.library_construction_protocol,
                        "sequencer": row.sequencer,
                        "analysis_software_version": row.analysis_software_version,
                        "ticket": row.ticket,
                        "facility": row.facility_code.upper(),
                        "type": self.ckan_data_type,
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, track_meta.date_of_transfer
                        ),
                        "data_type": track_meta.data_type,
                        "description": track_meta.description,
                        "folder_name": track_meta.folder_name,
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            self._logger, track_meta.date_of_transfer
                        ),
                        "contextual_data_submission_date": None,
                        "data_generated": ingest_utils.get_date_isoformat(
                            self._logger, track_meta.date_of_transfer_to_archive
                        ),
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, track_meta.date_of_transfer_to_archive
                        ),
                        "dataset_url": track_meta.download,
                        "private": True,
                    }
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id))
                tag_names = ["small-rna", "raw"]
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((sample_id,), legacy_url, resource))
        return resources


class StemcellsSingleCellRNASeqMetadata(BaseMetadata):
    contextual_classes = [StemcellsSingleCellRNASeq]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/stemcell/raw/single_cell_rnaseq/"
    ]
    # //this is based on splitting of subdirs ( so cannot be used for anything else e.g., flow_id) as they appear in downloads
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata\.xlsx"]
    organization = "bpa-stemcells"
    technology = "singlecellrna"
    ckan_data_type = "stemcells-singlecellrnaseq"
    resource_linkage = ("sample_id_range", "flow_id")
    spreadsheet = {
        "fields": [
            fld(
                "sample_id_range",
                re.compile(r"^.*sample unique id$"),
                coerce=parse_sample_id_range,
            ),
            fld("sample_extaction_id", "Sample extraction ID"),
            fld("insert_size_range", "Insert size range"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld("fastq_generation", "Fastq generation", optional=True),
            fld("bcl_to_fastq_generation", "Bcl to Fastq generation", optional=True),
            fld("casava_version", "casava version", optional=True),
        ],
        "options": {"header_length": 2, "column_name_row_index": 1,},
    }
    md5 = {
        "match": [
            files.singlecell_filename_re,
            files.singlecell_filename2_re,
            files.singlecell_index_info_filename_re,
        ],
        "skip": common_skip,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellsTrackMetadata()
        self.flow_lookup = {}

    def _get_packages(self):
        self._logger.info(
            "Ingesting Stemcells SingleCellRNASeq metadata from {0}".format(self.path)
        )
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and sample_id is the primary key
        all_rows = set()
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Stemcells SingleCellRNASeq metadata file {0}".format(fname)
            )
            next_rows = self.parse_spreadsheet(fname, self.metadata_info)
            file_info = files.singlecell_raw_xlsx_filename_re.match(
                os.path.basename(fname)
            ).groupdict()
            self.flow_lookup[next_rows[0].ticket] = file_info["flow_id"]
            all_rows.update(next_rows)
        for row in all_rows:
            sample_id_range = row.sample_id_range
            if sample_id_range is None:
                continue
            obj = {}
            flow_id = self.flow_lookup.get(row.ticket)
            if not flow_id:
                raise Exception("Unable to find flow id for: %s" % row.ticket)
            name = sample_id_to_ckan_name(sample_id_range, self.ckan_data_type, flow_id)
            track_meta = self.track_meta.get(row.ticket)
            # check that it really is a range
            if "-" not in sample_id_range:
                self._logger.error(
                    "Skipping row with BPA ID Range `%s'" % (sample_id_range)
                )
                continue
            # NB: this isn't really the BPA ID, it's the first BPA ID
            sample_id = ingest_utils.extract_ands_id(
                self._logger, sample_id_range.split("-", 1)[0]
            )
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "sample_id": sample_id,
                    "sample_id_range": sample_id_range,
                    "flow_id": flow_id,
                    "notes": "Stemcell SingleCellRNASeq %s" % (sample_id_range),
                    "title": "Stemcell SingleCellRNASeq %s" % (sample_id_range),
                    "insert_size_range": row.insert_size_range,
                    "library_construction_protocol": row.library_construction_protocol,
                    "sequencer": row.sequencer,
                    "fastq_generation": row.fastq_generation,
                    "ticket": row.ticket,
                    "facility": row.facility_code.upper(),
                    "type": self.ckan_data_type,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "omics": "transcriptomics",
                    "private": True,
                }
            )
            for contextual_source in self.contextual_metadata:
                # NB: the rows in the contextual metadata are all identical across the range, so this works
                obj.update(contextual_source.get(sample_id))
            tag_names = ["single-cell-rnaseq", "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                if file_info is None:
                    raise Exception("cannot parse filename: %s" % filename)
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                linked_resources = (file_info.get("id"), file_info.get("flow_id"))
                # resources object should align itself to class' resource_linkage property
                if len(linked_resources) != len(self.resource_linkage):
                    raise Exception(
                        "Linked resources length should be: %i",
                        len(self.resource_linkage),
                    )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append((linked_resources, legacy_url, resource))
        return resources


class StemcellsMetabolomicsMetadata(BaseMetadata):
    contextual_classes = [StemcellsMetabolomicsContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/stemcell/raw/metabolomic/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata.*\.xlsx"]
    organization = "bpa-stemcells"
    omics = "metabolomics"
    resource_linkage = ("sample_id", "analytical_platform")
    ckan_data_type = "stemcells-metabolomic"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                re.compile(r"^.*(sample unique id|bpa unique[\s]+identifier \*\*)$"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_fractionation_extraction_solvent",
                "sample fractionation / extraction solvent",
            ),
            fld("analytical_platform", "platform", coerce=fix_analytical_platform),
            fld("instrument_column_type", "instrument/column type"),
            fld("method", "Method"),
            fld("mass_spectrometer", "Mass Spectrometer"),
            fld("acquisition_mode", "acquisition mode"),
            fld(
                "sample_submission_date",
                re.compile(r"^(sample submission date|date submission)$"),
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "raw_file_name",
                re.compile(
                    r"^raw file name \(available in (.d and .mzml|.qgd) format\)"
                ),
                units="available in .d,.qgd and .mzml format",
                coerce=ingest_utils.get_clean_number,
            ),
            skp("sample name **"),
        ],
        "options": {"header_length": 2, "column_name_row_index": 1,},
    }
    md5 = {"match": [files.metabolomics_filename_re], "skip": common_skip}

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellsTrackMetadata()

    def _get_packages(self):
        self._logger.info(
            "Ingesting Stemcells Metabolomics metadata from {0}".format(self.path)
        )
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and sample_id is the primary key
        all_rows = set()
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Stemcells Metabolomics metadata file {0}".format(fname)
            )
            all_rows.update(self.parse_spreadsheet(fname, self.metadata_info))
        for row in all_rows:
            sample_id = row.sample_id
            if sample_id is None:
                continue
            obj = {}
            name = sample_id_to_ckan_name(
                sample_id.split("/")[-1] + "-" + row.analytical_platform,
                self.ckan_data_type,
            )
            track_meta = self.track_meta.get(row.ticket)
            analytical_platform = fix_analytical_platform(
                self._logger, row.analytical_platform
            )
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "sample_id": sample_id,
                    "notes": "Stemcell Metabolomics %s %s"
                    % (sample_id, analytical_platform),
                    "title": "Stemcell Metabolomics %s %s"
                    % (sample_id, analytical_platform),
                    "omics": "metabolomics",
                    "sample_fractionation_extraction_solvent": row.sample_fractionation_extraction_solvent,
                    "analytical_platform": analytical_platform,
                    "instrument_column_type": row.instrument_column_type,
                    "method": row.method,
                    "mass_spectrometer": row.mass_spectrometer,
                    "acquisition_mode": row.acquisition_mode,
                    "ticket": row.ticket,
                    "facility": row.facility_code.upper(),
                    "type": self.ckan_data_type,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "private": True,
                }
            )
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(sample_id, analytical_platform))
            tag_names = ["metabolomic", clean_tag_name(analytical_platform), "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["analytical_platform"] = fix_analytical_platform(
                    self._logger, resource["analytical_platform"]
                )
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    ((sample_id, resource["analytical_platform"]), legacy_url, resource)
                )
        return resources


class StemcellsProteomicsBaseMetadata(BaseMetadata):
    contextual_classes = [StemcellsProteomicsContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/stemcell/raw/proteomic/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata\.xlsx"]
    omics = "proteomics"
    organization = "bpa-stemcells"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filename_metadata = {}

    def read_all_rows(self, mode):
        all_rows = set()
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Stemcells Proteomics metadata file {0}".format(fname)
            )
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            all_rows.update(self.parse_spreadsheet(fname, xlsx_info, mode))
        self.filename_metadata.update(dict((t.raw_filename, t) for t in all_rows))
        self.filename_metadata.update(
            dict((t.protein_result_filename, t) for t in all_rows)
        )
        self.filename_metadata.update(
            dict((t.peptide_result_filename, t) for t in all_rows)
        )
        return all_rows

    def parse_spreadsheet(self, fname, additional_context, mode):
        if mode == "1d":
            field_spec = [
                fld(
                    "sample_id",
                    re.compile(r"^.*sample unique id$"),
                    coerce=ingest_utils.extract_ands_id_silent,
                ),
            ]
        elif mode == "2d":
            field_spec = [
                fld(
                    "pool_id",
                    "raw file name",
                    coerce=files.proteomics_raw_extract_pool_id,
                ),
            ]

        field_spec += [
            fld("facility", "facility"),
            fld("sample_fractionation", "sample fractionation (none/number)"),
            fld("lc_column_type", "lc/column type"),
            fld("gradient_time", re.compile(r"gradient time \(min\).*")),
            # allow for various unit symbols or none to be used (in this case: micrograms symbol)
            fld(
                "sample_on_column",
                re.compile(
                    r"sample on column \("
                    + "["
                    + b"\xc2\xb5".decode("utf-8")
                    + "]?"
                    + re.escape("g)")
                ),
                optional=True,
            ),
            fld("mass_spectrometer", "mass spectrometer"),
            fld("acquisition_mode", "acquisition mode / fragmentation"),
            fld("raw_filename", "raw file name"),
            fld("protein_result_filename", "protein result filename", optional=True),
            fld("peptide_result_filename", "peptide result filename", optional=True),
            fld("database", "database", optional=True),
            fld("database_size", "database size", optional=True),
            fld("sample_unique_id", "sample unique id"),
        ]
        wrapper = ExcelWrapper(
            self._logger,
            field_spec,
            fname,
            header_length=2,
            column_name_row_index=1,
            additional_context=additional_context,
        )
        for error in wrapper.get_errors():
            self._logger.error(error)
        rows = list(wrapper.get_all())
        return rows


class StemcellsProteomicsMetadata(StemcellsProteomicsBaseMetadata):
    ckan_data_type = "stemcells-proteomic"
    md5 = {
        "match": [files.proteomics_filename_re, files.proteomics_filename2_re],
        "skip": common_skip,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellsTrackMetadata()

    def _get_packages(self):
        self._logger.info(
            "Ingesting Stemcells Proteomics metadata from {0}".format(self.path)
        )
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and sample_id is the primary key
        #
        # we also have rows relating to pooled data, and non-pooled data (this class
        # considers only non-pooled data)
        all_rows = self.read_all_rows("1d")
        sample_id_ticket_facility = dict(
            (t.sample_id, (t.ticket, t.facility_code)) for t in all_rows if t.sample_id
        )
        for sample_id, (ticket, facility_code) in sorted(
            sample_id_ticket_facility.items()
        ):
            obj = {}
            name = sample_id_to_ckan_name(sample_id.split("/")[-1], self.ckan_data_type)
            track_meta = self.track_meta.get(ticket)
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "sample_id": sample_id,
                    "notes": "Stemcell Proteomics %s" % (sample_id),
                    "title": "Stemcell Proteomics %s" % (sample_id),
                    "omics": "proteomics",
                    "type": self.ckan_data_type,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "ticket": ticket,
                    "facility": facility_code,
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "private": True,
                }
            )
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(sample_id))
            tag_names = ["proteomic", "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                if file_info is None:
                    if not files.proteomics_pool_filename_re.match(filename):
                        raise Exception("unhandled file: %s" % (filename))
                    continue
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource_meta = self.filename_metadata.get(filename, {})
                for k in (
                    "sample_fractionation",
                    "lc_column_type",
                    "gradient_time",
                    "sample_on_column",
                    "mass_spectrometer",
                    "acquisition_mode",
                    "database",
                    "database_size",
                ):
                    resource[k] = getattr(resource_meta, k)
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((sample_id,), legacy_url, resource))
        return resources


class StemcellsProteomicsPoolMetadata(StemcellsProteomicsBaseMetadata):
    ckan_data_type = "stemcells-proteomic-pool"
    resource_linkage = ("pool_id",)
    pool = True
    md5 = {"match": [files.proteomics_pool_filename_re], "skip": common_skip}

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellsTrackMetadata()

    def _get_packages(self):
        self._logger.info(
            "Ingesting Stemcells Proteomics Pool metadata from {0}".format(self.path)
        )
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and sample_id is the primary key
        #
        # we also have rows relating to pooled data, and non-pooled data (this class
        # considers only non-pooled data)
        all_rows = self.read_all_rows("2d")
        pool_id_ticket_facility = dict(
            (t.pool_id, (t.ticket, t.facility_code)) for t in all_rows if t.pool_id
        )
        for pool_id, (ticket, facility_code) in sorted(pool_id_ticket_facility.items()):
            obj = {}
            name = sample_id_to_ckan_name(pool_id, self.ckan_data_type)
            track_meta = self.track_meta.get(ticket)
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "pool_id": pool_id,
                    "notes": "Stemcell Proteomics Pool %s" % (pool_id),
                    "title": "Stemcell Proteomics Pool %s" % (pool_id),
                    "omics": "proteomics",
                    "type": self.ckan_data_type,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "ticket": ticket,
                    "facility": facility_code,
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "private": True,
                }
            )
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(pool_id))
            tag_names = ["proteomic", "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                if file_info is None:
                    if not files.proteomics_filename_re.match(filename):
                        raise Exception("unhandled file: %s" % (filename))
                    continue
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource_meta = self.filename_metadata.get(filename, {})
                for k in (
                    "sample_fractionation",
                    "lc_column_type",
                    "gradient_time",
                    "sample_on_column",
                    "mass_spectrometer",
                    "acquisition_mode",
                    "database",
                    "database_size",
                ):
                    resource[k] = getattr(resource_meta, k)
                pool_id = file_info["pool_id"]
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((pool_id,), legacy_url, resource))
        return resources


class StemcellsProteomicsAnalysedMetadata(BaseMetadata):
    """
    we one zip file per ticket, a metadata spreadsheet, and an MD5 file
    we use the ticket as linkage between the package and the resource
    """

    contextual_classes = []
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/stemcell/analysed/proteomic/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata\.xlsx$"]
    organization = "bpa-stemcells"
    omics = "proteomics"
    analysed = True
    ckan_data_type = "stemcells-proteomics-analysed"
    resource_linkage = ("ticket",)
    spreadsheet = {
        "fields": [
            fld(
                "date_submission",
                "date submission yy/mm/dd",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "facility_project_code_experiment_code",
                "facility project_code _facility experiment code",
            ),
            fld(
                "sample_id",
                "bpa unique  identifier",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample_name", "sample name"),
            fld("replicate_group_id", "replicate group id"),
            fld("species", "species"),
            fld("sample_description", "sample_description"),
            fld("sample_type", "sample type"),
            fld("tissue", "tissue"),
            fld("cell_type", "cell type"),
            fld("growth_protocol", "growth protocol"),
            fld("extract_protocol", "extract protocol"),
            fld("omics", "omics"),
            fld(
                "analytical_platform",
                "analytical plaform",
                coerce=fix_analytical_platform,
            ),
            fld("facility", "facility"),
            fld("date_type", "data type"),
            fld("zip_file_name", "file name of analysed data (folder or zip file)"),
            fld("version", "version (genome or database)"),
            fld("translation", "translation (3 frame or 6 frame)"),
            fld("proteome_size", "proteome size"),
        ],
        "options": {"header_length": 8, "column_name_row_index": 7,},
    }
    md5 = {
        "match": [
            files.proteomics_analysed_filename_re,
            files.xlsx_filename_re,
            files.pdf_filename_re,
        ],
        "skip": common_skip,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellsTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting Stemcells metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        ticket_rows = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing Stemcells metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            if not ticket:
                continue
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                ticket_rows[ticket].append(row)
        packages = []
        for ticket, rows in list(ticket_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            track_meta = self.track_meta.get(ticket)
            name = sample_id_to_ckan_name(track_meta.folder_name, self.ckan_data_type)
            # folder names can be quite long, truncate
            name = name[:100]
            sample_ids = sorted(
                set(
                    [
                        ingest_utils.extract_ands_id(self._logger, t.sample_id)
                        for t in rows
                    ]
                )
            )
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "notes": "Stemcell Proteomics Analysed %s"
                    % (track_meta.folder_name),
                    "title": "Stemcell Proteomics Analysed %s"
                    % (track_meta.folder_name),
                    "omics": "proteomics",
                    "sample_ids": ", ".join(sample_ids),
                    "type": self.ckan_data_type,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "private": True,
                }
            )
            # for contextual_source in self.contextual_metadata:
            #     obj.update(contextual_source.get(track_meta.folder_name))
            tag_names = ["proteomics", "analysed"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = {}
                resource["md5"] = md5
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                # analysed data has duplicate PNG images in it -- we need to keep the ID unique
                resource["id"] = (
                    "u-"
                    + md5hash(
                        (self.ckan_data_type + xlsx_info["ticket"] + md5).encode("utf8")
                    ).hexdigest()
                )
                resource["name"] = filename
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((xlsx_info["ticket"],), legacy_url, resource))
        return resources


class StemcellsMetabolomicsAnalysedMetadata(BaseMetadata):
    contextual_classes = []
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/stemcell/analysed/metabolomic/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata\.xlsx$"]
    organization = "bpa-stemcells"
    ckan_data_type = "stemcells-metabolomics-analysed"
    omics = "metabolomics"
    analysed = True
    resource_linkage = ("folder_name",)
    spreadsheet = {
        "fields": [
            fld("data_analysis_date", "data analysis date"),
            fld("sample_id_range", "bpa unique  identifier **"),
            fld("sample_name", "sample name **"),
            fld("replicate_group_id", "replicate group id**"),
            fld("species", "species**"),
            fld("sample_description", "sample_description**"),
            fld("tissue", "tissue**"),
            fld("cell_type", "cell type**"),
            fld("disease_state", "disease state"),
            fld("growth_protocol", "growth protocol"),
            fld("omics", "omics"),
            fld(
                "analytical_platform",
                "analytical platform",
                coerce=fix_analytical_platform,
            ),
            fld("facility", "facility"),
            fld("method_type", "method type"),
            fld("data_type", "data type"),
            fld(
                "analysis_file_name",
                "file name of analysed data (folder or zip file) file name",
            ),
            fld("additional_comments", "additional comments"),
            skp(re.compile(r"^x{4,6}$"), skip_all=True),
        ],
        "options": {"header_length": 8, "column_name_row_index": 7,},
    }
    md5 = {"match": [re.compile(r"^.*$"),], "skip": common_skip}

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellsTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting Stemcells metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing Stemcells metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            if not ticket:
                continue
            folder_name = self.track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            name = sample_id_to_ckan_name(folder_name, self.ckan_data_type)
            track_meta = self.track_meta.get(ticket)
            sample_ids = sorted(set([str(t.sample_id_range).strip() for t in rows]))
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "notes": "%s" % (folder_name),
                    "title": "%s" % (folder_name),
                    "omics": "metabolomics",
                    "sample_ids": ", ".join(sample_ids),
                    "type": self.ckan_data_type,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "private": True,
                }
            )
            # for contextual_source in self.contextual_metadata:
            #     obj.update(contextual_source.get(ticket))
            tag_names = ["metabolomics", "analysed"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        # one MD5 file per 'folder_name', so we just take every file and upload
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy() or {}
                resource["md5"] = md5
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                # analysed data has duplicate PNG images in it - we need to keep the id unique
                id_attributes = self.ckan_data_type + xlsx_info["base_url"] + md5
                if filename and filename.endswith(".png"):
                    id_attributes += filename
                resource["id"] = (
                    "u-" + md5hash((id_attributes).encode("utf8")).hexdigest()
                )
                resource["name"] = filename
                ticket_name = xlsx_info["ticket"]
                tracking_ticket_folder = self.track_meta.get(ticket_name)
                if not tracking_ticket_folder:
                    self._logger.warn(
                        "No tracking ticket folder found. Consider checking the tracking metadata to ensure it contains ticket_name: {0}.".format(
                            ticket_name
                        )
                    )
                else:
                    if ticket_name == next:
                        self._logger.debug(
                            "Tracking ticket folder is: {0}".format(
                                tracking_ticket_folder
                            )
                        )
                folder_name = (
                    tracking_ticket_folder.folder_name if tracking_ticket_folder else ""
                )
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((folder_name,), legacy_url, resource))
        return resources


class StemcellsTranscriptomeAnalysedMetadata(BaseMetadata):
    contextual_classes = []
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/stemcell/analysed/transcriptome/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata\.xlsx$"]
    organization = "bpa-stemcells"
    ckan_data_type = "stemcells-transcriptome-analysed"
    omics = "transcriptome"
    analysed = True
    resource_linkage = ("folder_name",)
    spreadsheet = {
        "fields": [
            fld("date", "date (yyyy-mm-dd)", coerce=ingest_utils.get_date_isoformat),
            fld("sample_id", "bpa identifier", coerce=ingest_utils.extract_ands_id),
            fld("plate_number", "plate number"),
            fld("well_no", "well no"),
            fld("sample_name", "sample name"),
            fld("replicate_group_id", "replicate group id"),
            fld("data_set", "data set"),
            fld("species", "species"),
            fld("sample_description", "sample_description"),
            fld("tissue", "tissue"),
            fld("cell_type", "cell type"),
            fld("disease_state", "disease state"),
            fld("labelling", "labelling"),
            fld("organism_part", "organism part"),
            fld("molecule", "molecule"),
            fld("growth_protocol", "growth protocol"),
            fld("treatment_protocol", "treatment protocol"),
            fld("extract_protocol", "extract protocol"),
            fld("library_construction_protocol", "library construction protocol"),
            fld("library_strategy", "library strategy"),
            fld("omics", "omics"),
            fld("analytical_plaform", "analytical plaform"),
            fld("facility", "facility"),
            fld("data_type", "data type"),
            fld("folder_name", "file name of analysed data (folder or zip file)"),
            skp(re.compile(r"relevant heading\??$"), skip_all=True),
        ],
        "options": {"header_length": 9, "column_name_row_index": 8,},
    }
    md5 = {"match": [re.compile(r"^.*$"),], "skip": common_skip}

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellsTrackMetadata()

    def _get_packages(self):
        self._logger.info("Ingesting Stemcells metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing Stemcells metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            if not ticket:
                continue
            folder_name = self.track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            name = sample_id_to_ckan_name(folder_name, self.ckan_data_type)
            track_meta = self.track_meta.get(ticket)
            sample_ids = sorted(set([t.sample_id.strip() for t in rows]))
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "notes": "%s" % (folder_name),
                    "title": "%s" % (folder_name),
                    "omics": "transcriptomics",
                    "sample_ids": ", ".join(sample_ids),
                    "type": self.ckan_data_type,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "private": True,
                }
            )
            # for contextual_source in self.contextual_metadata:
            #     obj.update(contextual_source.get(ticket))
            tag_names = ["transcriptomics", "analysed"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        # one MD5 file per 'folder_name', so we just take every file and upload
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource = {}
                resource["md5"] = md5
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                # analysed data has duplicate PNG images in it - we need to keep the id unique
                resource["id"] = (
                    "u-"
                    + md5hash(
                        (self.ckan_data_type + xlsx_info["base_url"] + md5).encode(
                            "utf8"
                        )
                    ).hexdigest()
                )
                resource["name"] = filename
                folder_name = self.track_meta.get(xlsx_info["ticket"]).folder_name
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((folder_name,), legacy_url, resource))
        return resources
