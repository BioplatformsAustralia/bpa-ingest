import os
from glob import glob

from .contextual import FishLibraryContextual, FishDatasetControlContextual
from .tracking import FishGoogleTrackMetadata
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import (
    sample_id_to_ckan_name,
    common_values,
    merge_values,
    apply_cc_by_license,
    clean_tag_name,
)

common_context = [FishLibraryContextual, FishDatasetControlContextual]


class FishBaseMetadata(BaseMetadata):
    initiative = "Fish"
    organization = "aus-fish"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _build_title_into_object(self, obj, field_value):
        if field_value is None:
            self.build_title_into_object(
                obj,
                {
                    "initiative": self.initiative,
                    "title_description": self.description,
                },
            )
        else:
            self.build_title_into_object(
                obj,
                {
                    "initiative": self.initiative,
                    "title_description": self.description,
                    "field_value": field_value,
                },
            )

    notes_mapping = [
        {"key": "genus", "separator": " "},
        {"key": "species", "separator": ", "},
        {"key": "voucher_or_tissue_number", "separator": " "},
        {"key": "country", "separator": " "},
        {"key": "state_or_region"},
    ]
    title_mapping = [
        {"key": "initiative", "separator": " "},
        {"key": "title_description", "separator": " "},
        {"key": "field_value", "separator": " "},
        {"key": "flowcell_id", "separator": ""},
    ]

    def _set_metadata_vars(self, filename):
        self.xlsx_info = self.metadata_info[os.path.basename(filename)]
        self.ticket = self.xlsx_info["ticket"]

    def _get_common_packages(self):
        self._logger.info(
            "Ingesting {} metadata from {}".format(self.initiative, self.path)
        )
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing {} metadata file {}".format(
                    self.initiative, os.path.basename(fname)
                )
            )
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            if self.method_exists("_set_metadata_vars"):
                self._set_metadata_vars(fname)
            for row in rows:
                if not row.bioplatforms_sample_id and not row.flowcell_id:
                    # skip empty rows
                    continue
                bioplatforms_sample_id = row.bioplatforms_sample_id
                bioplatforms_library_id = row.bioplatforms_library_id
                bioplatforms_dataset_id = row.bioplatforms_dataset_id
                obj = row._asdict()
                track_meta = self.get_tracking_info(row.ticket)
                if track_meta is not None:
                    obj.update(track_meta._asdict())

                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(row.bioplatforms_sample_id))
                obj.update(context)
                if not hasattr(row, "flowcell_id"):
                    # name is populated by the subclass after the fact
                    name = "No flowcell- override in sublass"
                else:
                    name = sample_id_to_ckan_name(
                        "{}".format(bioplatforms_library_id.split("/")[-1]),
                        self.ckan_data_type,
                        "{}".format(row.flowcell_id),
                    )

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "license_id": apply_cc_by_license(),
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger,
                            self.get_tracking_info(row.ticket, "date_of_transfer"),
                        ),
                        "date_of_transfer_to_archive": ingest_utils.get_date_isoformat(
                            self._logger,
                            self.get_tracking_info(
                                row.ticket, "date_of_transfer_to_archive"
                            ),
                        ),
                    }
                )

                self._build_title_into_object(obj, bioplatforms_library_id)
                self.build_notes_into_object(obj)
                self._add_datatype_specific_info_to_package(obj, row, fname)
                ingest_utils.permissions_organization_member(self._logger, obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                obj["tags"] = [{"name": "{:.100}".format(t)} for t in self.tag_names]
                packages.append(obj)
        return packages


"""
class FungiIlluminaShortreadMetadata(FungiBaseMetadata):
    ckan_data_type = "fungi-illumina-shortread"
    technology = "illumina-shortread"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/fungi_staging/illumina-shortread/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("bioplatforms_library_id", "flowcell_id")
    spreadsheet = {
            "fields": [  fld('bioplatforms_sample_id',
                             'bioplatforms_sample_id',
                             coerce=ingest_utils.extract_ands_id,),
                          fld("sample_id", "sample_id"),
                          fld(
                              "bioplatforms_library_id",
                              re.compile(r"bioplatforms_library_[Ii][Dd]"),
                              coerce=ingest_utils.extract_ands_id,
                          ),
                          fld(
                              "bioplatforms_dataset_id","bioplatforms_dataset_id",
                              coerce=ingest_utils.extract_ands_id,
                         ),
                         fld("library_construction_protocol", "library_construction_protocol"),
                         fld("run_format", "run format", optional=True),
                         fld("work_order", "work_order", coerce=ingest_utils.get_int),
                         fld("specimen_id", re.compile(r"specimen_[Ii][Dd]")),
                         fld("data_context", "data_context"),
                         fld("library_type", "library_type"),
                         fld("library_layout", "library_layout"),
                         fld("facility_sample_id","facility_sample_id"),
                         fld("sequencing_facility", "sequencing_facility"),
                         fld("sequencing_model", "sequencing_model"),
                         fld("library_strategy", "library_strategy"),
                         fld("library_selection", "library_selection"),
                         fld("library_source", "library_source"),
                         fld(
                              "library_prep_date",
                              "library_prep_date",
                              coerce=ingest_utils.get_date_isoformat,
                         ),
                         fld("library_prepared_by", "library_prepared_by"),
                         fld("library_location", "library_location"),
                         fld("library_status", "library_status", optional=True),
                         fld("library_comments", "library_comments"),
                         fld("dna_treatment", "dna_treatment"),
                         fld("library_index_id", "library_index_id"),
                         fld("library_index_sequence", "library_index_seq"),
                         fld("library_oligo_sequence", "library_oligo_sequence"),
                         fld("insert_size_range", "insert_size_range"),
                         fld("library_ng_ul", "library_ng_ul"),
                         fld("library_pcr_reps", "library_pcr_reps"),
                         fld("library_pcr_cycles", "library_pcr_cycles"),
                         fld("n_libraries_pooled", "n_libraries_pooled"),
                         fld("flowcell_type", "flowcell_type"),
                         fld("flowcell_id", "flowcell_id"),
                         fld("cell_postion", "cell_postion"),
                         fld("movie_length", "movie_length"),
                         fld("analysis_software", "analysis_software"),
                         fld("file_name", "file_name", optional=True),
                         fld("experimental_design", "experimental_design"),
                         fld("sequencing_platform", "sequencing_platform"),
                         fld("facility_project_code", "facility_project_code", optional=True),
                         fld('sequencing_kit_chemistry_version', 'sequencing_kit_chemistry_version', optional=True),
                         fld('bioplatforms_project', 'bioplatforms_project'),
                         fld('scientific_name', 'scientific_name'),
                         fld('project_lead', 'project_lead'),
                         fld('project_collaborators', 'project_collaborators'),
                         fld('bait_set_name', 'bait_set_name'),
                         fld('bait_set_reference', 'bait_set_reference'),
                         fld('library_index_id_dual', 'library_index_id_dual'),
                         fld('library_index_seq_dual', 'library_index_seq_dual'),
                         fld('library_oligo_sequence_dual', 'library_oligo_sequence_dual'),
                        fld('fast5_compression', 'fast5_compression'),
                         fld('model_base_caller', 'model_base_caller'),
                      ],
                      "options": {
        "sheet_name": "libmetadata",
        "header_length": 1,
        "column_name_row_index": 0,
        },
    }

    md5 = {
        "match": [files.illumina_shortread_re, ],
        "skip": [
            re.compile(r"^.*_metadata.*\.xlsx$"),
            re.compile(r"^.*SampleSheet.*"),
            re.compile(r"^.*TestFiles\.exe.*"),
            re.compile(r"^.*DataValidation\.pdf.*"),
            re.compile(r"^.*checksums\.(exf|md5)$"),
        ],
    }
    description = "Illumina Shortread"
    tag_names = ["genomics", "illumina-short-read"]

    def __init__(
            self, logger, metadata_path, contextual_metadata=None, metadata_info=None
    ):
        super().__init__(logger, metadata_path)
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = FungiGoogleTrackMetadata(logger)

    def _get_packages(self):
        packages = self._get_common_packages()
        return packages

    def _add_datatype_specific_info_to_package(self, obj, row, filename):

        flowcell_id = re.match(r"^.*_([^_]+)_metadata.*\.xlsx", filename).groups()[0]
        obj.update(
            { "data_generated": True,
              "flow_cell_id": flowcell_id,
              "library_id":  row.bioplatforms_library_id.split("/")[-1]
            }
        )

    def _get_resources(self):
        return self._get_common_resources()

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        resource["bioplatforms_library_id"] = ingest_utils.extract_ands_id(
            self._logger, resource["library_id"]
        )
        return

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        return (
            resource["bioplatforms_library_id"],
            resource["flow_cell_id"],
        )
        """
