# -*- coding: utf-8 -*-

from unipath import Path
from collections import defaultdict
from urllib.parse import urljoin, quote
from hashlib import md5 as md5hash
from .strains import get_taxon_strain, map_taxon_strain, map_taxon_strain_rows
from ...libs import ingest_utils
from ...util import (
    sample_id_to_ckan_name,
    csv_to_named_tuple,
    common_values,
    clean_tag_name,
    apply_cc_by_license,
)
from ...abstract import BaseMetadata
from ...libs.excel_wrapper import make_field_definition as fld
from glob import glob
from .tracking import (
    SepsisTrackMetadata,
    SepsisGenomicsTrackMetadata,
    SepsisGoogleTrackMetadata,
)
from .contextual import (
    SepsisBacterialContextual,
    SepsisGenomicsContextual,
    SepsisMetabolomicsLCMSContextual,
    SepsisProteomicsSwathMSContextual,
    SepsisProteomicsMS1QuantificationContextual,
    SepsisTranscriptomicsHiseqContextual,
)
from . import files
from datetime import datetime
import os
import re


def fix_version(logger, s):
    if isinstance(s, datetime):
        return ingest_utils.get_date_isoformat(logger, s)
    return str(s)


def parse_pooled_sample_id(logger, s):
    if isinstance(s, str) and "," in s:
        return tuple(
            [ingest_utils.extract_ands_id(logger, t.strip()) for t in s.split(",")]
        )
    else:
        return ingest_utils.extract_ands_id(logger, s)


def make_sample_id_list(logger, s):
    return tuple(
        [ingest_utils.extract_ands_id(logger, t.strip()) for t in s.split(",")]
    )


expanded_names = {
    "ms1quantification": "MS1 quantification",
    "2dlibrary": "2D Library",
    "proteindatabase": "protein database",
}


def expanded_tag_name(tag_name):
    """
    This function will return unique tag name.
    """
    return clean_tag_name(expanded_names.get(tag_name, tag_name))


def add_taxons_strains_tags(taxons, strains, tag_names):
    """
    This function generates taxons and strains tag names,
    removes duplicates and adds them to the list.
    """
    for taxon, strain in zip(taxons, strains):
        tag_names.append(clean_tag_name(("%s_%s" % (taxon, strain)).replace(" ", "_")))
    return sorted(set(tag_names))


def add_taxons_strains_meta(cls, obj):
    """
    Build package level package and strains metadata (noting that package might
    contain data for more than one taxon/strain pair) from the Google Drive
    tracking metadata.
    """
    taxons, strains = cls.google_track_meta.get_taxons_strains(obj["ticket"])
    mapped = [map_taxon_strain(*t) for t in zip(taxons, strains)]
    taxons = [t[0] for t in mapped]
    strains = [t[1] for t in mapped]
    obj.update(
        {
            "taxon_or_organism": ", ".join(list(sorted(set(taxons)))),
            "strain_or_isolate": ", ".join(list(sorted(set(strains)))),
        }
    )
    return taxons, strains


def sepsis_contextual_tags(cls, obj):
    tags = [cls.omics, cls.technology]
    taxon_tpl = get_taxon_strain(obj)
    if None not in taxon_tpl:
        tags.append(clean_tag_name(("{}_{}".format(*taxon_tpl).replace(" ", "_"))))
    data_type = obj.get("data_type")
    if data_type:
        tags.append(clean_tag_name(data_type))
    growth_media = obj.get("growth_media")
    growth_media_names = [
        tag.strip() for tag in growth_media.split(",") if tag not in "N/A"
    ]
    tags += [clean_tag_name(name) for name in growth_media_names]
    return tags


class BaseSepsisMetadata(BaseMetadata):
    # printable names used when generating CKAN schemas
    # package_field: printable name
    package_field_names = {"growth_condition_time": "growth_condition_time_(h)"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.google_track_meta = SepsisGoogleTrackMetadata(self._logger)

    def parse_spreadsheet(self, *args, **kwargs):
        return map_taxon_strain_rows(super().parse_spreadsheet(*args, **kwargs))


class SepsisGenomicsMiseqMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisGenomicsContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/raw/miseq/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    organization = "bpa-sepsis"
    ckan_data_type = "arp-genomics-miseq"
    omics = "genomics"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    technology = "miseq"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "Bacterial sample unique ID",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("insert_size_range", "Insert size range"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld("analysis_software_version", "AnalysisSoftwareVersion"),
        ],
        "options": {
            "header_length": 2,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [files.miseq_filename_re],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisGenomicsTrackMetadata(
            self._logger, "GenomicsMiSeq"
        )
        self.metadata_info = metadata_info

    def _get_packages(self):
        self._logger.info(
            "Ingesting Sepsis Genomics Miseq metadata from {0}".format(self.path)
        )
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Sepsis Genomics metadata file {0}".format(fname)
            )
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                track_meta = self.bpam_track_meta.get(sample_id)
                taxon, strain = get_taxon_strain(track_meta)
                obj = track_meta.copy()
                name = sample_id_to_ckan_name(
                    sample_id.split("/")[-1], self.ckan_data_type
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_id": sample_id,
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer_to_archive
                        ),
                        "notes": "ARP Genomics Miseq Raw Data: %s %s %s Replicate %s"
                        % (taxon, strain, obj["growth_media"], obj["replicate"]),
                        "title": "Sepsis Genomics Miseq %s"
                        % (sample_id.split("/")[-1]),
                        "ticket": row.ticket,
                        "facility": row.facility_code.upper(),
                        "insert_size_range": row.insert_size_range,
                        "library_construction_protocol": row.library_construction_protocol,
                        "sequencer": row.sequencer,
                        "analysis_software_version": row.analysis_software_version,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "data_generated": True,
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id, track_meta))
                tag_names = sepsis_contextual_tags(self, obj)
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = dict(
                (t, file_info.get(t))
                for t in (
                    "index",
                    "lane",
                    "vendor",
                    "read",
                    "flow_cell_id",
                    "library",
                    "extraction",
                    "runsamplenum",
                )
            )
            resource["seq_size"] = file_info.get("size")
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            resource["resource_path"] = ""
            resource["resource_type"] = self.ckan_data_type
            # these are set to False by default
            # methods to a) determine if they should be set to True and
            #            b) set other values within the resource (eg adjusted id, file location etc) as required
            # are still to be developed.
            resource["shared_file"] = False
            resource["optional_file"] = False
            sample_id = ingest_utils.extract_ands_id(self._logger, file_info.get("id"))
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((sample_id,), legacy_url, resource))
        return resources


class SepsisGenomicsPacbioMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisGenomicsContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/raw/pacbio/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    organization = "bpa-sepsis"
    ckan_data_type = "arp-genomics-pacbio"
    omics = "genomics"
    sequence_data_type = "pacbio-clr"
    embargo_days = 365
    technology = "pacbio"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "Bacterial sample unique ID",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("insert_size_range", "Insert size range"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld("sequencer_run_id", "Run ID"),
            fld("smrt_cell_id", "SMRT Cell ID"),
            fld("cell_position", ("Cell Postion", "Cell Position")),
            fld("rs_version", "RS Version"),
        ],
        "options": {
            "header_length": 2,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [files.pacbio_filename_re],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisGenomicsTrackMetadata(
            self._logger, "GenomicsPacBio"
        )
        self.metadata_info = metadata_info

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple("SepsisGenomicsPacbioTrack", fname)
        return dict(
            (ingest_utils.extract_ands_id(self._logger, t.five_digit_bpa_id), t)
            for t in rows
        )

    def _get_packages(self):
        self._logger.info(
            "Ingesting Sepsis Genomics Pacbio metadata from {0}".format(self.path)
        )
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Sepsis Genomics metadata file {0}".format(fname)
            )
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                track_meta = self.bpam_track_meta.get(sample_id)
                taxon, strain = get_taxon_strain(track_meta)
                obj = track_meta.copy()
                name = sample_id_to_ckan_name(
                    sample_id.split("/")[-1], self.ckan_data_type
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_id": sample_id,
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer_to_archive
                        ),
                        "title": "Sepsis Genomics Pacbio %s"
                        % (sample_id.split("/")[-1]),
                        "ticket": row.ticket,
                        "facility": row.facility_code.upper(),
                        "notes": "ARP Genomics Pacbio Raw Data: %s %s %s Replicate %s"
                        % (taxon, strain, obj["growth_media"], obj["replicate"]),
                        "insert_size_range": row.insert_size_range,
                        "library_construction_protocol": row.library_construction_protocol,
                        "sequencer": row.sequencer,
                        "sequencer_run_id": row.sequencer_run_id,
                        "smrt_cell_id": row.smrt_cell_id,
                        "cell_position": row.cell_position,
                        "rs_version": row.rs_version,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "data_generated": True,
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id, track_meta))
                tag_names = sepsis_contextual_tags(self, obj)
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = dict(
                (t, file_info.get(t))
                for t in ("run_id", "vendor", "data_type", "machine_data")
            )
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            resource["resource_path"] = ""
            resource["resource_type"] = self.ckan_data_type
            # these are set to False by default
            # methods to a) determine if they should be set to True and
            #            b) set other values within the resource (eg adjusted id, file location etc) as required
            # are still to be developed.
            resource["shared_file"] = False
            resource["optional_file"] = False
            sample_id = ingest_utils.extract_ands_id(self._logger, file_info.get("id"))
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((sample_id,), legacy_url, resource))
        return resources


class SepsisTranscriptomicsHiseqMetadata(BaseSepsisMetadata):
    contextual_classes = [
        SepsisBacterialContextual,
        SepsisTranscriptomicsHiseqContextual,
    ]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/transcriptomics/raw/hiseq/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    organization = "bpa-sepsis"
    ckan_data_type = "arp-transcriptomics-hiseq"
    omics = "transcriptomics"
    sequence_data_type = "illumina-transcriptomics"
    embargo_days = 365
    technology = "hiseq"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "Antibiotic Resistant Pathogen sample unique ID",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample", "Sample (MGR code)"),
            fld("sample_notes", "sample i.d"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("barcode_tag", "Barcode tag"),
            fld("sequencer", "Sequencer"),
            fld("casava_version", "CASAVA version"),
            fld("additional_notes", "additional notes", optional=True),
        ],
        "options": {
            "header_length": 2,
            "column_name_row_index": 1,
        },
    }
    md5 = {"match": [files.hiseq_filename_re], "skip": None}

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisTrackMetadata(self._logger, "TranscriptomicsHiSeq")
        self.metadata_info = metadata_info

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple("SepsisGenomicsHiseqTrack", fname)
        return dict(
            (ingest_utils.extract_ands_id(self._logger, t.five_digit_bpa_id), t)
            for t in rows
        )

    def _get_packages(self):
        self._logger.info(
            "Ingesting Sepsis Transcriptomics Hiseq metadata from {0}".format(self.path)
        )
        packages = []

        # we have some rows which are the same except for the flow-cell. BPA have asked
        # for these to be combined together, into a single package, with two flow-cells.
        # Should be an uncommon case, only in AGRF data.

        sample_id_info = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Sepsis Transcriptomics metadata file {0}".format(fname)
            )

            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            google_track_meta = self.google_track_meta.get(ticket)

            for row in rows:
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                sample_id_info[sample_id].append([row, xlsx_info, google_track_meta])

        # collate together the flow cell IDs
        sample_id_flowcells = defaultdict(set)
        for md5_file in glob(self.path + "/*.md5"):
            for filename, md5, _, file_info in self.md5_lines():
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                sample_id_flowcells[sample_id].add(file_info["flow_cell_id"])

        for sample_id, info in sample_id_info.items():
            tickets = ", ".join(
                sorted(set(xlsx_info["ticket"] for _, xlsx_info, _ in info))
            )
            archive_ingestion_dates = ", ".join(
                sorted(
                    set(
                        google_track_meta.date_of_transfer_to_archive
                        for _, _, google_track_meta in info
                    )
                )
            )
            name = sample_id_to_ckan_name(sample_id.split("/")[-1], self.ckan_data_type)
            track_meta = self.bpam_track_meta.get(sample_id)
            taxon, strain = get_taxon_strain(track_meta)
            obj = track_meta.copy()
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "sample_id": sample_id,
                    "flow_cell_ids": ", ".join(sorted(sample_id_flowcells[sample_id])),
                    "title": "ARP Transcriptomics Hiseq %s"
                    % (sample_id.split("/")[-1]),
                    "archive_ingestion_dates": archive_ingestion_dates,
                    "ticket": tickets,
                    "facility": row.facility_code.upper(),
                    "notes": "ARP Transcriptomics Hiseq Raw Data: %s %s %s Replicate %s"
                    % (taxon, strain, obj["growth_media"], obj["replicate"]),
                    "sample": row.sample,
                    "library_construction_protocol": row.library_construction_protocol,
                    "barcode_tag": row.barcode_tag,
                    "sequencer": row.sequencer,
                    "casava_version": row.casava_version,
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                    "data_generated": True,
                }
            )
            ingest_utils.permissions_organization_member(self._logger, obj)
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(sample_id, track_meta))
            tag_names = sepsis_contextual_tags(self, obj)
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = dict(
                (t, file_info.get(t))
                for t in (
                    "library",
                    "vendor",
                    "flow_cell_id",
                    "index",
                    "lane",
                    "read",
                )
            )
            resource["seq_size"] = file_info.get("size")
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            resource["resource_path"] = ""
            resource["resource_type"] = self.ckan_data_type
            # these are set to False by default
            # methods to a) determine if they should be set to True and
            #            b) set other values within the resource (eg adjusted id, file location etc) as required
            # are still to be developed.
            resource["shared_file"] = False
            resource["optional_file"] = False
            sample_id = ingest_utils.extract_ands_id(self._logger, file_info.get("id"))
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((sample_id,), legacy_url, resource))
        return resources


class SepsisMetabolomicsGCMSMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/metabolomics/raw/gcms/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    organization = "bpa-sepsis"
    ckan_data_type = "arp-metabolomics-gcms"
    omics = "metabolomics"
    sequence_data_type = "metabolomics"
    embargo_days = 365
    technology = "gcms"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "bacterial sample unique id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_fractionation_extract_solvent",
                "sample fractionation / extraction solvent",
            ),
            fld("gc_column_type", "gc/column type"),
            fld("gradient_time_min_flow", "gradient time (min) / flow"),
            fld("mass_spectrometer", "mass spectrometer"),
            fld("acquisition_mode", "acquisition mode"),
            fld("raw_file_name", "raw file name"),
        ],
        "options": {
            "header_length": 1,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [files.metabolomics_lcms_gcms_filename_re],
        "skip": [re.compile(r"^.*_metadata\.xlsx$")],
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisTrackMetadata(self._logger, "MetabolomicsGCMS")
        self.metadata_info = metadata_info

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple("SepsisMetabolomicsGCMSTrack", fname)
        return dict(
            (ingest_utils.extract_ands_id(self._logger, t.five_digit_bpa_id), t)
            for t in rows
        )

    def _get_packages(self):
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Sepsis Metabolomics GCMS metadata file {0}".format(fname)
            )
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                track_meta = self.bpam_track_meta.get(sample_id)
                taxon, strain = get_taxon_strain(track_meta)
                obj = track_meta.copy()
                name = sample_id_to_ckan_name(
                    sample_id.split("/")[-1], self.ckan_data_type
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_id": sample_id,
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer_to_archive
                        ),
                        "title": "ARP Metabolomics GCMS %s"
                        % (sample_id.split("/")[-1]),
                        "ticket": row.ticket,
                        "facility": row.facility_code.upper(),
                        "notes": "ARP Metabolomics GCMS Raw Data: %s %s %s Replicate %s"
                        % (taxon, strain, obj["growth_media"], obj["replicate"]),
                        "sample_fractionation_extract_solvent": row.sample_fractionation_extract_solvent,
                        "gc_column_type": row.gc_column_type,
                        "gradient_time_min_flow": row.gradient_time_min_flow,
                        "mass_spectrometer": row.mass_spectrometer,
                        "acquisition_mode": row.acquisition_mode,
                        "raw_file_name": row.raw_file_name,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "data_generated": True,
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id, track_meta))
                tag_names = sepsis_contextual_tags(self, obj)
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = dict(
                (t, file_info.get(t))
                for t in ("vendor", "platform", "mastr_ms_id", "machine_data")
            )
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            resource["resource_path"] = ""
            resource["resource_type"] = self.ckan_data_type
            # these are set to False by default
            # methods to a) determine if they should be set to True and
            #            b) set other values within the resource (eg adjusted id, file location etc) as required
            # are still to be developed.
            resource["shared_file"] = False
            resource["optional_file"] = False
            sample_id = ingest_utils.extract_ands_id(self._logger, file_info.get("id"))
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((sample_id,), legacy_url, resource))
        return resources


class SepsisMetabolomicsLCMSMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisMetabolomicsLCMSContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/metabolomics/raw/lcms/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    organization = "bpa-sepsis"
    ckan_data_type = "arp-metabolomics-lcms"
    omics = "metabolomics"
    sequence_data_type = "metabolomics"
    embargo_days = 365
    technology = "lcms"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "Bacterial sample unique ID",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_fractionation_extract_solvent",
                "Sample fractionation / Extraction Solvent",
            ),
            fld("lc_column_type", "LC/column type"),
            fld("gradient_time_min_flow", "Gradient time (min) / flow"),
            fld("mass_spectrometer", "Mass Spectrometer"),
            fld("acquisition_mode", "Acquisition Mode"),
            fld("raw_file_name", "Raw file name"),
        ],
        "options": {
            "header_length": 1,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [files.metabolomics_lcms_gcms_filename_re],
        "skip": [re.compile(r"^.*_metadata\.xlsx$")],
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisTrackMetadata(self._logger, "MetabolomicsLCMS")
        self.metadata_info = metadata_info

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple("SepsisMetabolomicsLCMSTrack", fname)
        return dict(
            (ingest_utils.extract_ands_id(self._logger, t.five_digit_bpa_id), t)
            for t in rows
        )

    def _get_packages(self):
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Sepsis Metabolomics LCMS metadata file {0}".format(fname)
            )
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                track_meta = self.bpam_track_meta.get(sample_id)
                taxon, strain = get_taxon_strain(track_meta)
                obj = track_meta.copy()
                name = sample_id_to_ckan_name(
                    sample_id.split("/")[-1], self.ckan_data_type
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_id": sample_id,
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer_to_archive
                        ),
                        "title": "ARP Metabolomics LCMS %s"
                        % (sample_id.split("/")[-1]),
                        "ticket": row.ticket,
                        "facility": row.facility_code.upper(),
                        "notes": "ARP Metabolomics LCMS Raw Data: %s %s %s Replicate %s"
                        % (
                            taxon,
                            strain,
                            obj.get("growth_media"),
                            obj.get("replicate"),
                        ),
                        "sample_fractionation_extract_solvent": row.sample_fractionation_extract_solvent,
                        "lc_column_type": row.lc_column_type,
                        "gradient_time_min_flow": row.gradient_time_min_flow,
                        "mass_spectrometer": row.mass_spectrometer,
                        "acquisition_mode": row.acquisition_mode,
                        "raw_file_name": row.raw_file_name,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "data_generated": True,
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id, track_meta))
                tag_names = sepsis_contextual_tags(self, obj)
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = dict(
                (t, file_info.get(t))
                for t in ("vendor", "platform", "mastr_ms_id", "machine_data")
            )
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            resource["resource_path"] = ""
            resource["resource_type"] = self.ckan_data_type
            # these are set to False by default
            # methods to a) determine if they should be set to True and
            #            b) set other values within the resource (eg adjusted id, file location etc) as required
            # are still to be developed.
            resource["shared_file"] = False
            resource["optional_file"] = False
            sample_id = ingest_utils.extract_ands_id(self._logger, file_info.get("id"))
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((sample_id,), legacy_url, resource))
        return resources


class SepsisProteomicsMS1QuantificationMetadata(BaseSepsisMetadata):
    contextual_classes = [
        SepsisBacterialContextual,
        SepsisProteomicsMS1QuantificationContextual,
    ]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/raw/ms1quantification/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    organization = "bpa-sepsis"
    ckan_data_type = "arp-proteomics-ms1quantification"
    omics = "proteomics"
    sequence_data_type = "proteomics"
    embargo_days = 365
    technology = "ms1quantification"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                "bacterial sample unique id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("facility", "facility", coerce=ingest_utils.to_uppercase),
            fld(
                "sample_fractionation",
                "sample fractionation (none/number)",
                units="none/number",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("lc_column_type", "lc/column type"),
            fld(
                "gradient_time",
                "gradient time (min)  /  % acn (start-finish main gradient) / flow",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "sample_on_column",
                "sample on column (µg)",
                units="\u00B5" + "g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("mass_spectrometer", "mass spectrometer"),
            fld("acquisition_mode_fragmentation", "acquisition mode / fragmentation"),
            fld("raw_file_name", "raw file name"),
        ],
        "options": {
            "header_length": 1,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [files.proteomics_ms1quantification_filename_re],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisTrackMetadata(
            self._logger, "ProteomicsMS1Quantification"
        )
        self.metadata_info = metadata_info

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple(
            "SepsisProteomicsMS1QuantificationTrack", fname
        )
        return dict(
            (ingest_utils.extract_ands_id(self._logger, t.five_digit_bpa_id), t)
            for t in rows
        )

    def _get_packages(self):
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Sepsis Proteomics MS1Quantification metadata file {0}".format(
                    fname
                )
            )
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                bpam_track_meta = self.bpam_track_meta.get(sample_id)
                taxon, strain = get_taxon_strain(bpam_track_meta)
                if "taxon_or_organism" not in bpam_track_meta:
                    self._logger.error(
                        "package for {} excluded due to missing BPAM metadata".format(
                            sample_id
                        )
                    )
                    continue
                obj = bpam_track_meta.copy()
                obj.update(row._asdict())
                name = sample_id_to_ckan_name(
                    sample_id.split("/")[-1], self.ckan_data_type
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id, bpam_track_meta))
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_id": sample_id,
                        "title": "ARP Proteomics MS1Quantification %s"
                        % (sample_id.split("/")[-1]),
                        "archive_ingestion_date": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer_to_archive
                        ),
                        "notes": "ARP Proteomics MS1Quantification Raw Data: %s %s %s Replicate %s"
                        % (taxon, strain, obj["growth_media"], obj["replicate"]),
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "data_generated": True,
                    }
                )
                ingest_utils.permissions_organization_member(self._logger, obj)
                tag_names = sepsis_contextual_tags(self, obj)
                obj["tags"] = [{"name": expanded_tag_name(t)} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = dict((t, file_info.get(t)) for t in ("vendor", "machine_data"))
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            resource["resource_path"] = ""
            resource["resource_type"] = self.ckan_data_type
            # these are set to False by default
            # methods to a) determine if they should be set to True and
            #            b) set other values within the resource (eg adjusted id, file location etc) as required
            # are still to be developed.
            resource["shared_file"] = False
            resource["optional_file"] = False
            sample_id = ingest_utils.extract_ands_id(self._logger, file_info.get("id"))
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((sample_id,), legacy_url, resource))
        return resources


class SepsisProteomicsSwathMSBaseSepsisMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisProteomicsSwathMSContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/raw/swathms/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata\.xlsx"]
    organization = "bpa-sepsis"
    omics = "proteomics"
    technology = "swathms"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id", "Bacterial sample unique ID", coerce=parse_pooled_sample_id
            ),
            fld("facility", "Facility"),
            fld(
                "sample_fractionation_none_number", "Sample fractionation (none/number)"
            ),
            fld("lc_column_type", "LC/column type"),
            fld(
                "gradient_time_per_acn",
                "Gradient time (min)  /  % ACN (start-finish main gradient) / flow",
            ),
            fld(
                "sample_on_column",
                "sample on column (µg)",
                units="\u00B5" + "g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("mass_spectrometer", "Mass Spectrometer"),
            fld("acquisition_mode_fragmentation", "Acquisition Mode / fragmentation"),
            fld("raw_file_name", "Raw file name"),
        ],
        "options": {
            "sheet_name": " Metadata sheet (raw data)",
            "header_length": 1,
            "column_name_row_index": 1,
        },
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisTrackMetadata(self._logger, "ProteomicsSwathMS")
        self.package_data, self.file_data = self.get_spreadsheet_data()

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple("SepsisProteomicsSwathMSTrack", fname)
        return dict(
            (ingest_utils.extract_ands_id(self._logger, t.five_digit_bpa_id), t)
            for t in rows
        )

    def get_spreadsheet_data(self):
        """
        proteomics SWATH is a bit different, the spreadsheets might have dupes by `bpa id`,
        so some data in the sheet is per-file and some is per-ID. the only way to go from
        filename back to the pool/sample_id is via the spreadsheet, so we also need to build
        that mapping
        """
        package_data = {}
        file_data = {}
        for fname in glob(self.path + "/*_metadata.xlsx"):
            self._logger.info(
                "Processing Sepsis Proteomics SwathMS metadata file {0}".format(fname)
            )
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                contextual_meta = {}
                # if `sample_id` is a tuple, we've got a pooled sample
                if isinstance(sample_id, tuple):
                    data_type = "2d"
                    printable_sample_id = "_".join(
                        [t.split("/")[-1] for t in sorted(sample_id)]
                    )
                    track_meta = common_values(
                        [self.bpam_track_meta.get(t) for t in sample_id]
                    )
                    for contextual_source in self.contextual_metadata:
                        contextual_meta.update(
                            common_values(
                                [
                                    contextual_source.get(t, track_meta)
                                    for t in sample_id
                                ]
                            )
                        )
                else:
                    data_type = "1d"
                    printable_sample_id = sample_id
                    track_meta = self.bpam_track_meta.get(sample_id)
                    for contextual_source in self.contextual_metadata:
                        contextual_meta.update(
                            contextual_source.get(sample_id, track_meta)
                        )
                name = sample_id_to_ckan_name(
                    printable_sample_id.split("/")[-1], self.ckan_data_type
                )
                package_meta = {
                    "facility": row.facility_code.upper(),
                    "ticket": row.ticket,
                    "lc_column_type": row.lc_column_type,
                    "gradient_time_per_acn": row.gradient_time_per_acn,
                    "mass_spectrometer": row.mass_spectrometer,
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer_to_archive
                    ),
                }
                package_meta.update(contextual_meta)
                package_data[name] = (
                    name,
                    data_type,
                    printable_sample_id,
                    track_meta,
                    package_meta,
                )
                file_data[row.raw_file_name] = {
                    "package_name": printable_sample_id,
                    "sample_fractionation_none_number": row.sample_fractionation_none_number,
                    "sample_on_column": row.sample_on_column,
                    "acquisition_mode_fragmentation": row.acquisition_mode_fragmentation,
                }
        return package_data, file_data

    def get_swath_packages(self, data_type):
        packages = []
        for (
            package_name,
            (name, package_data_type, printable_sample_id, track_meta, submission_meta),
        ) in list(self.package_data.items()):
            if package_data_type != data_type:
                continue
            obj = track_meta.copy()
            obj.update(submission_meta)
            pool = ""
            if data_type == "1d":
                obj.update(
                    {
                        "sample_id": printable_sample_id,
                    }
                )
            if data_type == "2d":
                pool = "Pool "
                obj.update(
                    {
                        "pool_sample_ids": printable_sample_id,
                    }
                )
            # package won't get replicate number if datatype is 2d, because it's getting common values. see code above.
            if "replicate" in obj:
                replicate = " Replicate %s" % obj["replicate"]
            else:
                replicate = ""
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "title": "ARP Proteomics SwathMS %s%s"
                    % (pool, printable_sample_id.split("/")[-1]),
                    "notes": "ARP Proteomics SwathMS %sRaw Data: %s %s %s%s"
                    % (
                        pool,
                        track_meta["taxon_or_organism"],
                        track_meta["strain_or_isolate"],
                        obj["growth_media"],
                        replicate,
                    ),
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                    "data_generated": True,
                }
            )
            ingest_utils.permissions_organization_member(self._logger, obj)
            tag_names = sepsis_contextual_tags(self, obj)
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_swath_resources(self, data_type):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []

        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = dict((t, file_info.get(t)) for t in ("vendor", "machine_data"))
            if filename not in self.file_data:
                self._logger.warning("no submission metadata for `%s'" % (filename))
            file_meta = self.file_data.get(filename, {})
            resource["md5"] = resource["id"] = md5
            resource["data_type"] = file_info.get("type")
            resource["vendor"] = file_info.get("vendor")
            resource["resource_type"] = data_type
            package_name = file_meta.pop("package_name", None)
            resource.update(file_meta)
            resource["name"] = filename
            resource["resource_path"] = ""
            if data_type == "1d":
                package_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
            elif data_type == "2d":
                package_id = package_name

            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((package_id,), legacy_url, resource))
        return resources


class SepsisProteomicsSwathMSCombinedSampleMetadata(BaseSepsisMetadata):
    """
    see https://github.com/BioplatformsAustralia/bpa-archive-ops/issues/163
    this is a one-off, for a single dataset that was inadvertantly produced in this way
    as such, this ingest is deliberately minimal
    """

    contextual_classes = []
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/raw/swathms-combined-sample/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata\.xlsx$"]
    organization = "bpa-sepsis"
    ckan_data_type = "arp-proteomics-swathms-combined-sample"
    resource_linkage = ("folder_name",)
    omics = "proteomics"
    sequence_data_type = "proteomics"
    embargo_days = 365
    technology = "swathms-combined-sample"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id_list",
                "bacterial sample unique id",
                coerce=make_sample_id_list,
            ),
            fld("facility", "facility"),
            fld(
                "sample_fractionation_none_number", "sample fractionation (none/number)"
            ),
            fld("lc_column_type", "lc/column type"),
            fld(
                "gradient_time_min",
                "gradient time (min)  /  % acn (start-finish main gradient) / flow",
            ),
            fld(
                "sample_on_column",
                "sample on column (µg)",
                units="\u00B5" + "g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("mass_spectrometer", "mass spectrometer"),
            fld("acquisition_mode_fragmentation", "acquisition mode / fragmentation"),
            fld("raw_file_name", "raw file name"),
        ],
        "options": {
            "header_length": 2,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [re.compile(r"^.*$")],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata(self._logger)

    def _get_packages(self):
        self._logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode("utf8")).hexdigest()
            name = sample_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            taxons, strains = self.google_track_meta.get_taxons_strains(ticket)
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "notes": "%s" % (folder_name),
                    "title": "%s" % (folder_name),
                    "omics": "proteomics",
                    "data_generated": "True",
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "growth_media": track_meta.growth_media,
                }
            )
            ingest_utils.permissions_organization_member(self._logger, obj)
            tag_names = sepsis_contextual_tags(self, obj)
            taxons, strains = add_taxons_strains_meta(self, obj)
            tag_names = add_taxons_strains_tags(taxons, strains, tag_names)
            obj.update(
                {
                    "notes": "ARP Proteomics Swath-MS Combined Sample Raw Data: %s, %s"
                    % (
                        ", ".join(
                            [
                                taxons + " " + strains
                                for taxons, strains in zip(taxons, strains)
                            ]
                        ),
                        obj["growth_media"],
                    ),
                }
            )
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = {}
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            folder_name = self.google_track_meta.get(xlsx_info["ticket"]).folder_name
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((folder_name,), legacy_url, resource))
        return resources


class SepsisProteomics2DLibraryMetadata(BaseSepsisMetadata):
    contextual_classes = []
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/raw/2dlibrary/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata\.xlsx$"]
    organization = "bpa-sepsis"
    ckan_data_type = "arp-proteomics-2dlibrary"
    resource_linkage = ("folder_name",)
    omics = "proteomics"
    sequence_data_type = "proteomics"
    embargo_days = 365
    technology = "2dlibrary"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id_list",
                "bacterial sample unique id",
                coerce=make_sample_id_list,
            ),
            fld("facility", "facility"),
            fld(
                "sample_fractionation_none_number", "sample fractionation (none/number)"
            ),
            fld("lc_column_type", "lc/column type"),
            fld(
                "gradient_time_min",
                "gradient time (min)  /  % acn (start-finish main gradient) / flow",
            ),
            fld(
                "sample_on_column",
                "sample on column (µg)",
                units="\u00B5" + "g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("mass_spectrometer", "mass spectrometer"),
            fld("acquisition_mode_fragmentation", "acquisition mode / fragmentation"),
            fld("raw_file_name", "raw file name"),
        ],
        "options": {
            "header_length": 2,
            "column_name_row_index": 1,
        },
    }
    md5 = {
        "match": [files.proteomics_2dlibrary_filename_re],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata(self._logger)

    def _get_packages(self):
        self._logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode("utf8")).hexdigest()
            name = sample_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "notes": "%s" % (folder_name),
                    "title": "%s" % (folder_name),
                    "omics": "proteomics",
                    "data_generated": "True",
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "growth_media": track_meta.growth_media,
                }
            )
            ingest_utils.permissions_organization_member(self._logger, obj)
            tag_names = sepsis_contextual_tags(self, obj)
            taxons, strains = add_taxons_strains_meta(self, obj)
            tag_names = add_taxons_strains_tags(taxons, strains, tag_names)
            obj.update(
                {
                    "notes": "ARP Proteomics 2D Library Raw Data: %s, %s"
                    % (
                        ", ".join(
                            [
                                taxons + " " + strains
                                for taxons, strains in zip(taxons, strains)
                            ]
                        ),
                        obj["growth_media"],
                    ),
                }
            )
            obj["tags"] = [{"name": expanded_tag_name(t)} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = {}
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            resource.update(file_info)
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            folder_name = self.google_track_meta.get(xlsx_info["ticket"]).folder_name
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((folder_name,), legacy_url, resource))
        return resources


class SepsisProteomicsSwathMSMetadata(SepsisProteomicsSwathMSBaseSepsisMetadata):
    ckan_data_type = "arp-proteomics-swathms"
    sequence_data_type = "proteomics"
    embargo_days = 365
    md5 = {
        "match": [
            files.proteomics_swathms_1d_ida_filename_re,
            files.proteomics_swathms_lib_filename_re,
            files.proteomics_swathms_swath_raw_filename_re,
        ],
        "skip": [
            files.proteomics_swathms_lib_filename_re,
            files.proteomics_swathms_2d_ida_filename_re,
            files.proteomics_swathms_mspeak_filename_re,
            files.proteomics_swathms_msresult_filename_re,
        ],
    }

    def _get_packages(self):
        return self.get_swath_packages("1d")

    def _get_resources(self):
        return self.get_swath_resources("1d")


class SepsisProteomicsSwathMSPoolMetadata(SepsisProteomicsSwathMSBaseSepsisMetadata):
    ckan_data_type = "arp-proteomics-swathms-pool"
    sequence_data_type = "proteomics"
    embargo_days = 365
    pool = True
    resource_linkage = ("pool_sample_ids",)
    md5 = {
        "match": [
            files.proteomics_swathms_lib_filename_re,
            files.proteomics_swathms_2d_ida_filename_re,
            files.proteomics_swathms_mspeak_filename_re,
            files.proteomics_swathms_msresult_filename_re,
        ],
        "skip": [
            files.proteomics_swathms_1d_ida_filename_re,
            files.proteomics_swathms_lib_filename_re,
            files.proteomics_swathms_swath_raw_filename_re,
        ],
    }

    def _get_packages(self):
        return self.get_swath_packages("2d")

    def _get_resources(self):
        return self.get_swath_resources("2d")


class BaseSepsisAnalysedMetadata(BaseSepsisMetadata):
    def apply_common_context(self, obj, sample_ids):
        # find the contextual metadata in common between these BPA IDs
        context_objs = []
        for sample_id in sample_ids:
            context_obj = {}
            for contextual_source in self.contextual_metadata:
                context_obj.update(contextual_source.get(sample_id, obj))
            context_objs.append(context_obj)
        obj.update(common_values(context_objs))
        # find the tracking metadata in common between these BPA IDs
        tracking_objs = []
        for sample_id in sample_ids:
            tracking_obj = {}
            for bpam_source in self.bpam_track_meta:
                tracking_obj.update(bpam_source.get(sample_id))
            tracking_objs.append(tracking_obj)
        obj.update(common_values(tracking_objs))
        return obj

    @classmethod
    def google_drive_track_to_object(cls, trk, exclude=[]):
        "copy over the relevant bits of a sepsis google drive track object, to a package object"
        obj = {
            "facility": trk.facility,
            "data_type": trk.data_type_pre_pilot_pilot_or_main_dataset,
            "ticket": trk.ccg_jira_ticket,
        }
        for field in (
            "date_of_transfer",
            "taxon_or_organism",
            "strain_or_isolate",
            "growth_media",
            "folder_name",
            "date_of_transfer_to_archive",
            "file_count",
        ):
            if field in exclude:
                continue
            if hasattr(trk, field):
                obj[field] = getattr(trk, field)
        return obj


class SepsisProteomicsAnalysedMetadata(BaseSepsisAnalysedMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisProteomicsSwathMSContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/analysed/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata\.xlsx$"]
    organization = "bpa-sepsis"
    ckan_data_type = "arp-proteomics-analysed"
    resource_linkage = ("folder_name",)
    omics = "proteomics"
    sequence_data_type = "proteomics"
    embargo_days = 365
    technology = "analysed"
    spreadsheet = {
        "fields": [
            fld(
                "data_analysis_date",
                "data analysis date (yyyy-mm-dd)",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "facility_project_code_experiment_code",
                "facility project code_facility experiment code",
            ),
            fld(
                "sample_id",
                "sample name (5 digit bpa id)",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("taxon_or_organism", "taxon_or_organism"),
            fld("strain_or_isolate", "strain_or_isolate"),
            fld("serovar", "serovar"),
            fld("growth_media", "growth media"),
            fld("replicate", "replicate", coerce=ingest_utils.get_int),
            fld("growth_condition_time", "growth_condition_time"),
            fld("growth_condition_growth", "growth_condition_growth phase"),
            fld("growth_condition_od600", "growth_condition_od600 reading"),
            fld("growth_condition_temperature", "growth_condition_temperature"),
            fld("growth_condition_media", "growth_condition_media"),
            fld("omics", "omics"),
            fld("analytical_platform", "analytical platform"),
            fld("facility", "facility"),
            fld("data_type", "data type"),
            fld("zip_file_name", "file name of analysed data (folder or zip file)"),
            fld("genome_used", "genome used (file name if used annoted bpa genome)"),
            fld("database", "database (if used publicly available genome)"),
            fld("version", "version (genome or database)", coerce=fix_version),
            fld("translation", "translation (3 frame or 6 frame)"),
            fld("proteome_size", "proteome size"),
        ],
        "options": {
            "header_length": 8,
            "column_name_row_index": 7,
        },
    }
    md5 = {
        "match": [re.compile(r"^.*$")],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata(self._logger)
        self.bpam_track_meta = [
            SepsisTrackMetadata(self._logger, "ProteomicsMS1Quantification"),
            SepsisTrackMetadata(self._logger, "ProteomicsSwathMS"),
        ]

    def _get_packages(self):
        self._logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode("utf8")).hexdigest()
            name = sample_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            sample_ids = list(sorted(set([t.sample_id for t in rows if t.sample_id])))
            obj.update(self.google_drive_track_to_object(track_meta))
            self.apply_common_context(obj, sample_ids)
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "notes": "%s" % (folder_name),
                    "title": "%s" % (folder_name),
                    "omics": "proteomics",
                    "sample_ids": ", ".join(sample_ids),
                    "data_generated": "True",
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                }
            )
            ingest_utils.permissions_organization_member(self._logger, obj)
            tag_names = sepsis_contextual_tags(self, obj)
            taxons, strains = add_taxons_strains_meta(self, obj)
            tag_names = add_taxons_strains_tags(taxons, strains, tag_names)
            # Correction with analytical platform and generate tag
            analytical_platform = sorted(
                set([t.analytical_platform for t in rows if t.analytical_platform])
            )
            obj.update(
                {
                    "analytical_platform": ", ".join(analytical_platform),
                }
            )
            tag_names.extend([",".join(analytical_platform)])
            obj["tags"] = [{"name": expanded_tag_name(t)} for t in tag_names]
            # Update analysed package notes(showings as description)
            obj.update(
                {
                    "notes": "ARP %s %s analysed data: %s, %s"
                    % (
                        obj["omics"],
                        obj["analytical_platform"],
                        ", ".join(
                            [
                                taxons + " " + strains
                                for taxons, strains in zip(taxons, strains)
                            ]
                        ),
                        obj["growth_media"],
                    )
                }
            )
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        # one MD5 file per 'folder_name', so we just take every file and upload
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = {}
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            folder_name = self.google_track_meta.get(xlsx_info["ticket"]).folder_name
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((folder_name,), legacy_url, resource))
        return resources


class SepsisTranscriptomicsAnalysedMetadata(BaseSepsisAnalysedMetadata):
    contextual_classes = [SepsisBacterialContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/transcriptomics/analysed/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata\.xlsx$"]
    organization = "bpa-sepsis"
    ckan_data_type = "arp-transcriptomics-analysed"
    resource_linkage = ("ticket",)
    omics = "transcriptomics"
    sequence_data_type = "transcriptomics-analysed"
    embargo_days = 365
    technology = "analysed"
    spreadsheet = {
        "fields": [
            fld(
                "data_analysis_date",
                "data analysis date (yyyy-mm-dd)",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "sample_id",
                "sample name (5 digit bpa id)",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("taxon_or_organism", "taxon_or_organism"),
            fld("strain_or_isolate", "strain_or_isolate"),
            fld("serovar", "serovar"),
            fld("growth_media", "growth media"),
            fld("replicate", "replicate", coerce=ingest_utils.get_int),
            fld("growth_condition_time", "growth_condition_time"),
            fld("growth_condition_growth_phase", "growth_condition_growth phase"),
            fld("growth_condition_od600_reading", "growth_condition_od600 reading"),
            fld("growth_condition_temperature", "growth_condition_temperature"),
            fld("growth_condition_media", "growth_condition_media"),
            fld("omics", "omics"),
            fld("analytical_platform", "analytical platform"),
            fld("facility", "facility"),
            fld("alignment_file_name", "alignment file name"),
            fld("file_name_of_gene_list", "file name of gene list (raw counts)"),
            fld("file_name_of_annotated_rna", "file name of annotated rna"),
            fld(
                "file_name_of_assembled_genome",
                "file name of assembled genome used for analysis",
            ),
            fld(
                "file_name_of_annotated_genes",
                "file name of annotated genes used for analysis",
            ),
            fld("approach_used", "approach used", optional=True),
        ],
        "options": {
            "header_length": 8,
            "column_name_row_index": 7,
        },
    }
    md5 = {
        "match": [re.compile(r"^.*$")],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata(self._logger)
        self.bpam_track_meta = [
            SepsisTrackMetadata(self._logger, "TranscriptomicsHiSeq")
        ]

    def _get_packages(self):
        self._logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        ticket_rows = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            if not ticket:
                continue
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                ticket_rows[ticket].append(row)
        packages = []
        for ticket, rows in list(ticket_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            ticket_md5 = md5hash(ticket.encode("utf8")).hexdigest()
            name = sample_id_to_ckan_name(ticket_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            folder_name = track_meta.folder_name
            sample_ids = list(
                sorted(set([t.sample_id for t in rows if t.sample_id is not None]))
            )
            obj.update(self.google_drive_track_to_object(track_meta))
            self.apply_common_context(obj, sample_ids)
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "notes": "%s" % (folder_name),
                    "title": "%s" % (folder_name),
                    "folder_name": "%s" % (folder_name),
                    "omics": "transcriptomics",
                    "sample_ids": ", ".join(sample_ids),
                    "data_generated": "True",
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "ticket": ticket,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                }
            )
            ingest_utils.permissions_organization_member(self._logger, obj)
            tag_names = sepsis_contextual_tags(self, obj)
            taxons, strains = add_taxons_strains_meta(self, obj)
            tag_names = add_taxons_strains_tags(taxons, strains, tag_names)
            obj["tags"] = [{"name": t} for t in tag_names]
            # Update analysed package notes(showings as description)
            obj.update(
                {
                    "notes": "ARP %s %s analysed data: %s, %s"
                    % (
                        obj["omics"],
                        obj.get("analytical_platform", ""),
                        ", ".join(
                            [
                                taxons + " " + strains
                                for taxons, strains in zip(taxons, strains)
                            ]
                        ),
                        obj["growth_media"],
                    )
                }
            )
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = {}
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            ticket = xlsx_info["ticket"]
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((ticket,), legacy_url, resource))
        return resources


class SepsisMetabolomicsAnalysedMetadata(BaseSepsisAnalysedMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisProteomicsSwathMSContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/metabolomics/analysed/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata\.xlsx$"]
    organization = "bpa-sepsis"
    ckan_data_type = "arp-metabolomics-analysed"
    resource_linkage = ("folder_name",)
    omics = "metabolomics"
    sequence_data_type = "metabolomics-analysed"
    embargo_days = 365
    technology = "analysed"
    spreadsheet = {
        "fields": [
            fld(
                "data_analysis_date",
                "data analysis date (yyyy-mm-dd)",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "sample_id",
                re.compile(r"sample name (i.e. )?\(?5 digit bpa id\)?"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("taxon_or_organism", "taxon_or_organism"),
            fld("strain_or_isolate", "strain_or_isolate"),
            fld("serovar", "serovar"),
            fld("growth_media", "growth media"),
            fld("replicate", "replicate", coerce=ingest_utils.get_int),
            fld("growth_condition_time", "growth_condition_time"),
            fld("growth_condition_growth_phase", "growth_condition_growth phase"),
            fld("growth_condition_od600_reading", "growth_condition_od600 reading"),
            fld("growth_condition_temperature", "growth_condition_temperature"),
            fld("growth_condition_media", "growth_condition_media"),
            fld("omics", "omics"),
            fld("analytical_platform", "analytical platform"),
            fld("facility", "facility"),
            fld("data_type", "data type"),
            fld("approach_used", "approach used", optional=True),
        ],
        "options": {
            "header_length": 8,
            "column_name_row_index": 7,
        },
    }
    md5 = {
        "match": [re.compile(r"^.*$")],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata(self._logger)
        self.bpam_track_meta = [SepsisTrackMetadata(self._logger, "MetabolomicsLCMS")]

    def _get_packages(self):
        self._logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode("utf8")).hexdigest()
            name = sample_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            sample_ids = list(sorted(set([t.sample_id for t in rows if t.sample_id])))
            analytical_platform = list(
                sorted(
                    set([t.analytical_platform for t in rows if t.analytical_platform])
                )
            )
            obj.update(self.google_drive_track_to_object(track_meta))
            self.apply_common_context(obj, sample_ids)
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "notes": "%s" % (folder_name),
                    "title": "%s" % (folder_name),
                    "omics": "metabolomics",
                    "sample_ids": ", ".join(sample_ids),
                    "analytical_platform": ", ".join(analytical_platform),
                    "data_generated": "True",
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                }
            )
            ingest_utils.permissions_organization_member(self._logger, obj)
            tag_names = sepsis_contextual_tags(self, obj)
            tag_names.append("Analysed metabolomics")
            taxons, strains = add_taxons_strains_meta(self, obj)
            tag_names = add_taxons_strains_tags(taxons, strains, tag_names)
            obj["tags"] = [{"name": t} for t in tag_names]
            # Update analysed package notes(showings as description)
            obj.update(
                {
                    "notes": "ARP %s %s analysed data: %s, %s"
                    % (
                        obj["omics"],
                        obj["analytical_platform"],
                        ", ".join(
                            [
                                taxons + " " + strains
                                for taxons, strains in zip(taxons, strains)
                            ]
                        ),
                        obj["growth_media"],
                    )
                }
            )
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = {}
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            folder_name = self.google_track_meta.get(xlsx_info["ticket"]).folder_name
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((folder_name,), legacy_url, resource))
        return resources


class SepsisGenomicsAnalysedMetadata(BaseSepsisAnalysedMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisProteomicsSwathMSContextual]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/analysed/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata\.xlsx$"]
    organization = "bpa-sepsis"
    ckan_data_type = "arp-genomics-analysed"
    resource_linkage = ("folder_name",)
    omics = "genomics"
    technology = "analysed"
    spreadsheet = {
        "fields": [
            fld(
                "data_analysis_date",
                "data analysis date (yyyy-mm-dd)",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "sample_id",
                "sample name (5 digit bpa id)",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("taxon_or_organism", "taxon_or_organism"),
            fld("strain_or_isolate", "strain_or_isolate"),
            fld("serovar", "serovar"),
            fld("growth_condition_time", "growth_condition_time"),
            fld("growth_condition_temperature", "growth_condition_temperature"),
            fld("growth_condition_media", "growth_condition_media"),
            fld("growth_condition_notes", "growth_condition_notes"),
            fld(
                "experimental_replicate",
                "experimental_replicate",
                coerce=ingest_utils.get_int,
            ),
            fld("analytical_platform", "analytical_platform"),
            fld("analytical_facility", "analytical_facility"),
            fld(
                "experimental_sample_preparation_method",
                "experimental_sample_preparation_method",
            ),
            fld("data_type", "data type"),
            fld(
                "sample_folder",
                "folder for each sample (individual files are listed on the next sheet)",
            ),
        ],
        "options": {
            "header_length": 8,
            "column_name_row_index": 7,
        },
    }
    md5 = {
        "match": [re.compile(r"^.*$")],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata(self._logger)
        self.bpam_track_meta = [
            SepsisGenomicsTrackMetadata(self._logger, "MetabolomicsLCMS")
        ]

    def _get_packages(self):
        self._logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode("utf8")).hexdigest()
            name = sample_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            sample_ids = list(sorted(set([t.sample_id for t in rows if t.sample_id])))
            obj.update(self.google_drive_track_to_object(track_meta))
            self.apply_common_context(obj, sample_ids)
            analytical_platform = list(
                sorted(
                    set([t.analytical_platform for t in rows if t.analytical_platform])
                )
            )
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "notes": "%s" % (folder_name),
                    "title": "%s" % (folder_name),
                    "omics": "genomics",
                    "sample_ids": ", ".join(sample_ids),
                    "data_generated": "True",
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                    "analytical_platform": ", ".join(analytical_platform),
                }
            )
            ingest_utils.permissions_organization_member(self._logger, obj)
            tag_names = sepsis_contextual_tags(self, obj)
            taxons, strains = add_taxons_strains_meta(self, obj)
            tag_names = add_taxons_strains_tags(taxons, strains, tag_names)
            obj["tags"] = [{"name": t} for t in tag_names]
            # Update analysed package notes(showings as description)
            obj.update(
                {
                    "notes": "ARP %s %s analysed data: %s, %s"
                    % (
                        obj["omics"],
                        obj["analytical_platform"],
                        ", ".join(
                            [
                                taxons + " " + strains
                                for taxons, strains in zip(taxons, strains)
                            ]
                        ),
                        obj["growth_media"],
                    )
                }
            )
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = {}
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            folder_name = self.google_track_meta.get(xlsx_info["ticket"]).folder_name
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((folder_name,), legacy_url, resource))
        return resources


class SepsisProteomicsProteinDatabaseMetadata(BaseSepsisAnalysedMetadata):
    contextual_classes = []
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/proteindatabase/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata\.xlsx$"]
    organization = "bpa-sepsis"
    ckan_data_type = "arp-proteomics-database"
    resource_linkage = ("folder_name",)
    omics = "proteomics"
    technology = "proteindatabase"
    analysed = True
    spreadsheet = {
        "fields": [
            fld(
                "database_generation_date",
                "database generation date (yyyy-mm-dd)",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "sample_id",
                "sample name (5 digit bpa id)",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("taxon_or_organism", "taxon_or_organism"),
            fld("strain_or_isolate", "strain_or_isolate"),
            fld("serovar", "serovar"),
            fld("file_name", "file name of database that is generated"),
            fld("bacterial_database_used", "bacterial database used (ccg jira ticket)"),
            fld(
                "version", "version (bacterial genome or database)", coerce=fix_version
            ),
            fld("human_database_used", "human database used (ccg jira ticket)"),
            fld(
                "decription_of_how_the_database_is_generated",
                "decription of how the database is generated",
            ),
            fld("translation", "translation (3 frame or 6 frame)"),
            fld("proteome_size", "proteome size"),
        ],
        "options": {
            "header_length": 8,
            "column_name_row_index": 7,
        },
    }
    md5 = {
        "match": [re.compile(r"^.*$")],
        "skip": None,
    }

    def __init__(
        self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata(self._logger)
        self.bpam_track_meta = []

    def _get_packages(self):
        self._logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info["ticket"]
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode("utf8")).hexdigest()
            name = sample_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            sample_ids = list(sorted(set([t.sample_id for t in rows if t.sample_id])))
            obj.update(self.google_drive_track_to_object(track_meta))
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "notes": "%s" % (folder_name),
                    "title": "%s" % (folder_name),
                    "omics": "proteomics",
                    "sample_ids": ", ".join(sample_ids),
                    "data_generated": "True",
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "data_type": track_meta.data_type,
                    "description": track_meta.description,
                    "folder_name": track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer
                    ),
                    "archive_ingestion_date": ingest_utils.get_date_isoformat(
                        self._logger, track_meta.date_of_transfer_to_archive
                    ),
                    "dataset_url": track_meta.download,
                }
            )
            ingest_utils.permissions_organization_member(self._logger, obj)
            tag_names = sepsis_contextual_tags(self, obj)
            taxons, strains = add_taxons_strains_meta(self, obj)
            tag_names = add_taxons_strains_tags(taxons, strains, tag_names)
            obj["tags"] = [{"name": expanded_tag_name(t)} for t in tag_names]
            # Update analysed package notes(showings as description)
            obj.update(
                {
                    "notes": "ARP %s protein database analysed data: %s, %s"
                    % (
                        obj["omics"],
                        ", ".join(
                            [
                                taxons + " " + strains
                                for taxons, strains in zip(taxons, strains)
                            ]
                        ),
                        obj["growth_media"],
                    )
                }
            )
            packages.append(obj)
        return packages

    def _get_resources(self):
        rows = []
        for fname in glob(self.path + "/*.xlsx"):
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            rows += self.parse_spreadsheet(fname, self.metadata_info)
        by_filename = dict((t.file_name.strip(), t) for t in rows)
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = {}
            extra_data = by_filename.get(filename)
            if extra_data:
                extra_data = extra_data._asdict()
                extra_data.pop("file_name")
                resource.update(extra_data)
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            folder_name = self.google_track_meta.get(xlsx_info["ticket"]).folder_name
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            resources.append(((folder_name,), legacy_url, resource))
        return resources
