import os
import re
from glob import glob
from unipath import Path

from .contextual import PlantProteinAtlasLibraryContextual, PlantProteinAtlasDatasetControlContextual
from .tracking import PlantProteinAtlasGoogleTrackMetadata
from ...abstract import BaseMetadata
from . import files
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import (
    sample_id_to_ckan_name,
    apply_cc_by_license,
)

common_context = [PlantProteinAtlasLibraryContextual, PlantProteinAtlasDatasetControlContextual]
CONSORTIUM_ORG_NAME = "ppa-consortium-members"


class PlantProteinAtlasBaseMetadata(BaseMetadata):
    initiative = "Plant Protein Atlas"
    organization = "ppa"

    notes_mapping = [
        {"key": "common_name", "separator": ", "},
        {"key": "specimen_custodian"},
    ]
    title_mapping = [
        {"key": "initiative", "separator": ", "},
        {"key": "omics", "separator": ", "},
        {"key": "data_context", "separator": ", Sample ID: "},
        {"key": "sample_id", "separator": ", "},
        {"key": "type", "separator": " "},

    ]

    def __init__(self, logger, metadata_path, contextual_metadata=None, metadata_info=None):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info

        self.google_track_meta = PlantProteinAtlasGoogleTrackMetadata(logger)

    def _get_common_packages(self):
        self._logger.info("Ingesting {} metadata from {}".format(self.initiative, self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):

            self._logger.info("Processing {} metadata file {}".format(self.initiative, os.path.basename(fname)))
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            if self.method_exists('_set_metadata_vars'):
                self._set_metadata_vars(fname)

            for row in rows:
                if not row.bioplatforms_sample_id:
                    continue
                obj = row._asdict()

                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(row.bioplatforms_sample_id))
                obj.update(context)

                if not hasattr(row, "bioplatforms_sample_id"):
                    # name is populated by the subclass after the fact
                    name = "No sample ID - override in sublass"
                else:
                    sample_id = row.bioplatforms_sample_id.split("/")[-1]
                    name = sample_id_to_ckan_name(
                        "{}".format(sample_id),
                        self.ckan_data_type,
                    )

                obj.update(
                    {   "initiative": "PPA",
                        "name": name,
                        "id": name,
                        "sample_id": sample_id,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
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
                self.build_title_into_object(obj)
                self.build_notes_into_object(obj)

                # get the slug for the org that matches the Project Code.
                package_embargo_days = self.embargo_days
                if context["access_control_date"] is not None:
                    package_embargo_days = context["access_control_date"]

                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "date_of_transfer_to_archive",
                    package_embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": "{:.100}".format(t)} for t in self.tag_names]
                del obj["sample_id"] # only temporary for title generation
                packages.append(obj)

        return packages


class PlantProteinAtlasPhenoCTXrayRawMetadata(PlantProteinAtlasBaseMetadata):
    ckan_data_type = "ppa-xray-raw"
    technology = "phenoct-xray"
    sequence_data_type = "xray-raw"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/xray-raw/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "bioplatforms_sample_id")
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
        "match": [files.phenoct_xray_raw_re, ],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }
    # description = "Illumina Shortread" this should come from teh tracking sheet
    tag_names = ["phenomics", "PhenoCT-xray-raw"]

    def _get_packages(self):
        packages = self._get_common_packages()
        return packages

    def _add_datatype_specific_info_to_package(self, obj, row, filename):

        obj.update({"bioplatforms_sample_id":
                        ingest_utils.extract_ands_id(self._logger, obj["bioplatforms_sample_id"])})
        del obj["dataset_id"]
        del obj["library_id"]

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        """     resource["library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["library_id"]
        )
        """
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (xlsx_info["ticket"],ingest_utils.extract_ands_id(self._logger, resource["sample_id"]),)


class PlantProteinAtlasPhenoCTXrayAnalysedMetadata(PlantProteinAtlasBaseMetadata):
    ckan_data_type = "ppa-xray-analysed"
    technology = "phenoct-xray-analysed"
    sequence_data_type = "xray-analysed"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/xray-analysed/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "bioplatforms_sample_id")
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
            "sheet_name": "1. APPF_PhenoCT Xray_analysed",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }

    md5 = {
        "match": [files.phenoct_xray_analysed_re, ],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }
    # description = "Illumina Shortread" this should come from teh tracking sheet
    tag_names = ["phenomics", "PhenoCT-xray-analysed"]

    def _get_packages(self):
        packages = self._get_common_packages()
        return packages

    def _add_datatype_specific_info_to_package(self, obj, row, filename):

        obj.update({"bioplatforms_sample_id":
                        ingest_utils.extract_ands_id(self._logger, obj["bioplatforms_sample_id"])})
        del obj["dataset_id"]
        del obj["library_id"]

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        """     resource["library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["library_id"]
        )
        """
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (xlsx_info["ticket"], ingest_utils.extract_ands_id(self._logger, resource["sample_id"]),)


class PlantProteinAtlasHyperspectralMetadata(PlantProteinAtlasBaseMetadata):
    ckan_data_type = "ppa-hyperspectral"
    technology = "hyperspectral"
    sequence_data_type = "hyperspectral"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/hyperspect/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("ticket", "bioplatforms_sample_id")
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
            "sheet_name": "2. APPF_Hyperspectral_raw",
            "header_length": 1,
            "column_name_row_index": 0,
        },
    }

    md5 = {
        "match": [files.hyperspect_re, ],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }
    # description = "Illumina Shortread" this should come from teh tracking sheet
    tag_names = ["phenomics", "Hyperspectral Raw"]

    def _get_packages(self):
        packages = self._get_common_packages()
        return packages

    def _add_datatype_specific_info_to_package(self, obj, row, filename):

        obj.update({"bioplatforms_sample_id":
                        ingest_utils.extract_ands_id(self._logger, obj["bioplatforms_sample_id"])})
        del obj["dataset_id"]
        del obj["library_id"]

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        """     resource["library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["library_id"]
        )
        """
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (xlsx_info["ticket"], ingest_utils.extract_ands_id(self._logger, resource["sample_id"]),)


class PlantProteinAtlasASDSpectroMetadata(PlantProteinAtlasBaseMetadata):
    ckan_data_type = "ppa-asd-spectro"
    technology = "asd-spectro"
    sequence_data_type = "asd-spectro"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
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
        "match": [files.asd_spectro_re, ],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }
    # description = "Illumina Shortread" this should come from teh tracking sheet
    tag_names = ["phenomics", "ASD FieldSpec Spectroradiometer"]

    title_mapping = [
        {"key": "initiative", "separator": ", "},
        {"key": "omics", "separator": ", "},
        {"key": "data_context", "separator": ", Dataset ID: "},
        {"key": "dataset_id", "separator": ", "},
        {"key": "type", "separator": " "},

    ]

    def _get_packages(self):

        """
        Thios was stolen from GAP DDRAD as it uses the dataset id istead of sample id.

        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        self._logger.info("Ingesting Plant Protein Atlas metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing GAP metadata file {0}".format(os.path.basename(fname))
            )
            objs = defaultdict(list)
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                obj = row._asdict()
                if not obj["dataset_id"] or not obj["flowcell_id"]:
                    continue
                for contextual_source in self.contextual_metadata:
                    obj.update(
                        contextual_source.get(obj["library_id"], obj["dataset_id"])
                    )
                objs[(obj["dataset_id"], obj["flowcell_id"])].append(obj)

            for (dataset_id), row_objs in list(objs.items()):

                if dataset_id is None:
                    continue

                context_objs = []
                for row in row_objs:
                    context = {}
                    for contextual_source in self.contextual_metadata:
                        context.update(contextual_source.get(row.get("dataset_id")))
                    context_objs.append(context)

                obj = common_values(row_objs)
                ticket = obj["ticket"]
                track_meta = self.google_track_meta.get(ticket)

                name = sample_id_to_ckan_name(
                    dataset_id, self.ckan_data_type
                )
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "dataset_id": dataset_id,
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, track_meta.date_of_transfer
                        ),
                        "data_type": track_meta.data_type,
                        "description": track_meta.description,
                        "folder_name": track_meta.folder_name,
                        "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                            self._logger, track_meta.date_of_transfer_to_archive
                        ),
                        "dataset_url": track_meta.download,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                    }
                )
                # obj.update(common_values(context_objs))
                # obj.update(merge_values("scientific_name", " , ", context_objs))
                self._build_title_into_object(obj)
                self.build_notes_into_object(obj)
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                obj["tags"] = [{"name": t} for t in self.tag_names]
                self.track_xlsx_resource(obj, fname)
                packages.append(obj)

        return packages
        """
        return {}

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        """     resource["library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["library_id"]
        )
        """
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        print(xlsx_info)
        print(resource)
        print(file_info)
        return (xlsx_info["ticket"], ingest_utils.extract_ands_id(self._logger, resource["dataset_id"]),)