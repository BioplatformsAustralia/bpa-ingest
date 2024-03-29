import os
import re
from glob import glob
from unipath import Path

from .contextual import PlantProteinAtlasLibraryContextual, PlantProteinAtlasDatasetControlContextual
from collections import defaultdict
from .tracking import PlantProteinAtlasGoogleTrackMetadata
from ...abstract import BaseMetadata
from . import files
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import (
    sample_id_to_ckan_name,
    apply_cc_by_license,
    common_values,
)

common_context = [PlantProteinAtlasLibraryContextual, PlantProteinAtlasDatasetControlContextual]
CONSORTIUM_ORG_NAME = "ppa-consortium-members"


class PlantProteinAtlasBaseMetadata(BaseMetadata):
    initiative = "Plant Protein Atlas"
    organization = "ppa"
    initiative_code = "PPA"

    notes_mapping = [
        {"key": "common_name", "separator": ", "},
        {"key": "specimen_custodian"},
    ]
    title_mapping = [
        {"key": "initiative_code", "separator": ", "},
        {"key": "omics", "separator": ", "},
        {"key": "data_context", "separator": ", Dataset ID: "},
        {"key": "dataset_id", "separator": ", "},
        {"key": "raw_or_analysed_data", "separator": " "},

    ]

    def __init__(self, logger, metadata_path, contextual_metadata=None, metadata_info=None):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info

        self.google_track_meta = PlantProteinAtlasGoogleTrackMetadata(logger)

    def _get_packages(self):
        packages = []

        # this is a folder-oriented ingest, so we crush each xlsx down into a single row
        filename_re = re.compile(r"^PPA.*_(\d{5,6})_librarymetadata\.xlsx")

        objs = []
        for fname in glob(self.path + "/*librarymetadata.xlsx"):
            row_objs = []
            self._logger.info("-Processing {} metadata file {}".format(self.initiative, os.path.basename(fname)))
            file_dataset_id = filename_re.match(os.path.basename(fname)).groups()[0]

            full_dataset_id = ingest_utils.extract_ands_id(
                self._logger, file_dataset_id
            )
            # this is the library metadata. It contains both dataset_id and sample_id info.
            for row in self.parse_spreadsheet(fname, self.metadata_info):

                obj = row._asdict()
                obj.update({
                    "dataset_id": file_dataset_id,
                })

                if full_dataset_id != obj["bioplatforms_dataset_id"]:
                    self._logger.warn(
                        "Skipping metadata row related to unrelated dataset {0} (should be {1})".format(
                            obj["bioplatforms_dataset_id"], full_dataset_id
                        )
                    )
                    continue
                else:
                    self._logger.info(
                        "Found Sample Metadata for {0} ".format(
                            obj["bioplatforms_dataset_id"], file_dataset_id
                        )
                    )
                # Add data control  contextual metadata by linking with dataset id
                for contextual_source in self.contextual_metadata:
                    if isinstance(contextual_source, PlantProteinAtlasDatasetControlContextual):
                        obj.update(contextual_source.get(obj.get("bioplatforms_dataset_id")))
                    else:
                        if isinstance(contextual_source, PlantProteinAtlasLibraryContextual):  # its the sampel metadata, match up on sample_id
                            sample_id = obj.get("bioplatforms_sample_id")
                            obj.update(contextual_source.get(sample_id))

                row_objs.append(obj)

            combined_obj = common_values(row_objs)
            objs.append((fname, combined_obj))

        for (fname, obj) in objs:
            ticket = obj["ticket"]

            name = sample_id_to_ckan_name(
                file_dataset_id, self.ckan_data_type
            )
            tracking_info = self.get_tracking_info(ticket)
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, tracking_info.date_of_transfer
                    ),
                    "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                        self._logger, tracking_info.date_of_transfer_to_archive
                    ),
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "initiative_code": self.initiative_code,
                    "license_id": apply_cc_by_license(),
                    "facility": tracking_info.facility,
                    "scientific_name": tracking_info.scientific_name,
                    "experiment_type": tracking_info.experiment_type,
                    "omics": tracking_info.omics,
                    "raw_or_analysed_data": tracking_info.raw_or_analysed_data,
                    "data_type": tracking_info.data_type,
                    "description": tracking_info.description,
                }
            )


            package_embargo_days = self.embargo_days
            if obj["access_control_date"] is not None:
                package_embargo_days = obj["access_control_date"]
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "date_of_transfer_to_archive",
                package_embargo_days,
                CONSORTIUM_ORG_NAME,
            )
            ingest_utils.apply_access_control(self._logger, self, obj)
            obj["tags"] = [{"name": "{:.100}".format(t)} for t in self.tag_names]

            self.track_xlsx_resource(obj, fname)
            for sample_metadata_file in glob(
                self.path
                + "/*_"
                + ingest_utils.short_ands_id(self._logger, obj["bioplatforms_dataset_id"])
                + "_samplemetadata_ingest.xlsx"
            ):
                self.track_xlsx_resource(obj, sample_metadata_file)
            self.build_title_into_object(obj)
            self.build_notes_into_object(obj)
            del obj["dataset_id"]  # only used for populating the title
            packages.append(obj)
        return packages

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        ticket = xlsx_info["ticket"]
        dataset_id = self.get_tracking_info(ticket, "bioplatforms_dataset_id")
        return (ticket, ingest_utils.extract_ands_id(self._logger, dataset_id), )

class PlantProteinAtlasPhenoCTXrayRawMetadata(PlantProteinAtlasBaseMetadata):
    ckan_data_type = "ppa-xray-raw"
    technology = "phenoct-xray"
    sequence_data_type = "xray-raw"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*librarymetadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/xray-raw/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "bioplatforms_dataset_id")

    spreadsheet = {
        "fields": [
                fld('bioplatforms_project', 'bioplatforms_project'),
                fld('bioplatforms_sample_id', 'bioplatforms_sample_id', coerce=ingest_utils.extract_ands_id,),
                fld('bioplatforms_library_id', 'bioplatforms_library_id', coerce=ingest_utils.extract_ands_id,),
                fld('bioplatforms_dataset_id', 'bioplatforms_dataset_id', coerce=ingest_utils.extract_ands_id,),
                fld('planting_season', 'planting_season'),
                fld('planting_site', 'planting_site'),
                fld('planting_code', 'planting_code'),
                fld('planting_block', 'planting_block'),
                fld('planting_row', 'planting_row'),
                fld('planting_bay', 'planting_bay'),
                fld('variety_commercial', 'variety_commercial'),
                fld('variety_name', 'variety_name'),
                fld('plant_replicate', 'plant_replicate'),
                fld('data_type', 'data_type'),
                fld('omics', 'omics'),
                fld('data_context', 'data_context'),
                fld('facility_project_code', 'facility_project_code'),
                fld('facility_sample_id', 'facility_sample_id'),
                fld('phenomics_facility', 'phenomics_facility'),
                fld('analytical_platform', 'analytical_platform'),
                fld('x_ray_voltage', 'x_ray_voltage'),
                fld('x_ray_current', 'x_ray_current'),
                fld('x_ray_scanning_time', 'x_ray_scanning_time', coerce=ingest_utils.get_time),
                fld('x_ray_filter', 'x_ray_filter'),
                fld('x_ray_dosage', 'x_ray_dosage'),
                fld('x_ray_voxel_resolution', 'x_ray_voxel_resolution'),
                fld('x_ray_exposure_time', 'x_ray_exposure time', coerce=ingest_utils.get_time),
                fld('file_description', 'file_description'),
            ],
            "options": {
            "sheet_name": "1. APPF_PhenoCT Xray_raw",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }

    md5 = {
        "match": [files.phenoct_xray_raw_re,
                  files.xlsx_filename_re, ],

        "skip": [
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }

    tag_names = ["phenomics", "PhenoCT-xray-raw"]


class PlantProteinAtlasPhenoCTXrayAnalysedMetadata(PlantProteinAtlasBaseMetadata):
    ckan_data_type = "ppa-xray-analysed"
    technology = "phenoct-xray-analysed"
    sequence_data_type = "xray-analysed"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*librarymetadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/xray-analysed/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "bioplatforms_dataset_id")
    spreadsheet = {
        "fields": [
                fld('bioplatforms_project', 'bioplatforms_project'),
                fld('bioplatforms_sample_id', 'bioplatforms_sample_id', coerce=ingest_utils.extract_ands_id,),
                fld('bioplatforms_library_id', 'bioplatforms_library_id', coerce=ingest_utils.extract_ands_id,),
                fld('bioplatforms_dataset_id', 'bioplatforms_dataset_id', coerce=ingest_utils.extract_ands_id,),
                fld('planting_season', 'planting_season'),
                fld('planting_site', 'planting_site'),
                fld('planting_code', 'planting_code'),
                fld('planting_block', 'planting_block'),
                fld('planting_row', 'planting_row'),
                fld('planting_bay', 'planting_bay'),
                fld('variety_commercial', 'variety_commercial'),
                fld('variety_name', 'variety_name'),
                fld('plant_replicate', 'plant_replicate'),
                fld('data_type', 'data_type'),
                fld('omics', 'omics'),
                fld('data_context', 'data_context'),
                fld('facility_project_code', 'facility_project_code'),
                fld('facility_sample_id', 'facility_sample_id'),
                fld('phenomics_facility', 'phenomics_facility'),
                fld('data_analysis_date', 'data_analysis_date' ),
                fld('contact_person', 'contact_person'),
                fld('file_description', 'file_description'),
            ],
            "options": {
            "sheet_name": "1. APPF_PhenoCT Xray_analysed",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }

    md5 = {
        "match": [files.phenoct_xray_analysed_re,
                  files.xlsx_filename_re,
                  ],
        "skip": [
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }
    # description = "Illumina Shortread" this should come from teh tracking sheet
    tag_names = ["phenomics", "PhenoCT-xray-analysed"]

class PlantProteinAtlasHyperspectralMetadata(PlantProteinAtlasBaseMetadata):
    ckan_data_type = "ppa-hyperspectral"
    technology = "hyperspectral"
    sequence_data_type = "hyperspectral"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*librarymetadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/hyperspect/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "bioplatforms_dataset_id")
    spreadsheet = {
        "fields": [
                fld('bioplatforms_project', 'bioplatforms_project'),
                fld('bioplatforms_sample_id', 'bioplatforms_sample_id', coerce=ingest_utils.extract_ands_id,),
                fld('bioplatforms_library_id', 'bioplatforms_library_id', coerce=ingest_utils.extract_ands_id,),
                fld('bioplatforms_dataset_id', 'bioplatforms_dataset_id', coerce=ingest_utils.extract_ands_id,),
                fld('planting_season', 'planting_season'),
                fld('planting_site', 'planting_site'),
                fld('planting_code', 'planting_code'),
                fld('planting_block', 'planting_block'),
                fld('planting_row', 'planting_row'),
                fld('planting_bay', 'planting_bay'),
                fld('variety_commercial', 'variety_commercial'),
                fld('variety_name', 'variety_name'),
                fld('plant_replicate', 'plant_replicate'),
                fld('data_type', 'data_type'),
                fld('omics', 'omics'),
                fld('data_context', 'data_context'),
                fld('facility_project_code', 'facility_project_code'),
                fld('facility_sample_id', 'facility_sample_id'),
                fld('phenomics_facility', 'phenomics_facility'),
                fld('analytical_platform', 'analytical_platform'),
                fld('file_description', 'file_description'),
            ],
            "options": {
            "sheet_name": "2. APPF_Hyperspectral_raw",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }

    md5 = {
        "match": [files.hyperspect_re,
                  files.xlsx_filename_re,
                  ],
        "skip": [
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }
    # description = "Illumina Shortread" this should come from teh tracking sheet
    tag_names = ["phenomics", "Hyperspectral Raw"]


class PlantProteinAtlasASDSpectroMetadata(PlantProteinAtlasBaseMetadata):
    ckan_data_type = "ppa-asd-spectro"
    technology = "asd-spectro"
    sequence_data_type = "asd-spectro"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*librarymetadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/asd-spectro/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "bioplatforms_dataset_id")
    spreadsheet = {
        "fields": [
                fld('bioplatforms_project', 'bioplatforms_project'),
                fld('bioplatforms_sample_id', 'bioplatforms_sample_id', coerce=ingest_utils.extract_ands_id,),
                fld('bioplatforms_library_id', 'bioplatforms_library_id', coerce=ingest_utils.extract_ands_id,),
                fld('bioplatforms_dataset_id', 'bioplatforms_dataset_id', coerce=ingest_utils.extract_ands_id,),
                fld('planting_season', 'planting_season'),
                fld('planting_site', 'planting_site'),
                fld('planting_code', 'planting_code'),
                fld('planting_block', 'planting_block'),
                fld('planting_row', 'planting_row'),
                fld('planting_bay', 'planting_bay'),
                fld('variety_commercial', 'variety_commercial'),
                fld('variety_name', 'variety_name'),
                fld('plant_replicate', 'plant_replicate'),
                fld('data_type', 'data_type'),
                fld('omics', 'omics'),
                fld('data_context', 'data_context'),
                fld('facility_project_code', 'facility_project_code'),
                fld('facility_sample_id', 'facility_sample_id'),
                fld('phenomics_facility', 'phenomics_facility'),
                fld('analytical_platform', 'analytical_platform'),
            ],

            "options": {
            "sheet_name": "3. APPF_ASD FieldSpec_raw",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }

    md5 = {
        "match": [files.asd_spectro_re,
                  files.xlsx_filename_re, ],
        "skip": [
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }

    tag_names = ["phenomics", "ASD FieldSpec Spectroradiometer"]

