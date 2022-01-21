import re
from glob import glob
from ...libs import ingest_utils
from ...libs.excel_wrapper import (
    ExcelWrapper,
    make_field_definition as fld,
    make_skip_column as skp,
)
from ...util import make_logger, one
from ...abstract import BaseDatasetControlContextual


def date_or_str(logger, v):
    d = ingest_utils.get_date_isoformat(logger, v, silent=True)
    if d is not None:
        return d
    return v


class GAPDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/dataset_control/2021-11-11/"
    ]
    name = "gap-dataset-contextual"
    contextual_linkage = ("library_id", "dataset_id")


class GAPLibraryContextual:
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/plants_staging/metadata/2021-12-14/"
    ]
    metadata_patterns = [re.compile(r"^.*\.xlsx$")]
    name = "gap-library-contextual"
    sheet_names = [
        "Ref_genomes",
        "Phylogenomics_pilot",
        "Phylo AATOL - Run 1",
        "Phylo AATOL - Run 2",
        "Conservation ",
    ]

    def __init__(self, logger, path):
        self._logger = logger
        self._logger.info("context path is: {}".format(path))
        self.library_metadata = self._read_metadata(one(glob(path + "/*.xlsx")))

    def get(self, library_id, dataset_id):
        if (library_id, dataset_id) in self.library_metadata:
            return self.library_metadata[(library_id, dataset_id)]
        self._logger.warning(
            "no %s metadata available for: (%s,%s)"
            % (type(self).__name__, repr(library_id), repr(dataset_id))
        )
        return {}

    def _read_metadata(self, fname):

        field_spec = [
            fld("data_type", "data_type"),
            fld("project_aim", "project_aim"),
            fld("sample_submitter_name", "sample_submitter_name"),
            fld("sample_submitter_email", "sample_submitter_email"),
            fld(
                "sample_submission_date",
                "sample_submission_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "sample_id",
                "bioplatforms_sample_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "library_id",
                "bioplatforms_library_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "dataset_id",
                "bioplatforms_dataset_id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("nagoya_protocol_compliance", "nagoya_protocol_compliance"),
            fld("nagoya_protocol_permit_number", "nagoya_protocol_permit_number"),
            fld("scientific_name", "scientific_name"),
            fld("scientific_name_authorship", "scientific_name_authorship"),
            fld("family", "family"),
            fld("id_vetting_by", "id_vetting_by"),
            fld("bait_set_name", "bait_set_name"),
            fld("bait_set_reference", "bait_set_reference"),
            fld(
                "living_collections_catalog_number",
                "living _collections_catalog_number",
            ),
            fld(
                "living_collections_record_number", "living _collections_record_number"
            ),
            fld("living_collections_recorded_by", "living _collections_recorded_by"),
            fld(
                "living_collections_event_date",
                "living _collections_event_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("herbarium_code", "herbarium_code"),
            fld("voucher_herbarium_collector_id", "voucher_herbarium_collector_id"),
            fld(
                "voucher_herbarium_catalog_number",
                "voucher_herbarium_catalog_number",
                coerce=ingest_utils.int_or_comment,
            ),
            fld(
                "voucher_herbarium_record_number",
                "voucher_herbarium_record_number",
                coerce=ingest_utils.date_or_int_or_comment,
            ),
            fld("voucher_herbarium_recorded_by", "voucher_herbarium_recorded_by"),
            fld(
                "voucher_herbarium_event_date",
                "voucher_herbarium_event_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("silica_gel", "silica_gel"),
            fld("silica_gel_pressed_sheet", "silica_gel_pressed_sheet"),
            fld("dna_extract", "dna_extract"),
            fld("dna_extract_pressed_sheet", "dna_extract_pressed_sheet"),
            fld("preservation_type", "preservation_type"),
            fld("preservation_temperature", "preservation_temperature"),
            fld(
                "preservation_date_begin",
                "preservation_date_begin",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "genomic_material_associated_references",
                "genomic_material_associated_references",
            ),
            fld(
                "genomic_material_preparation_type", "genomic_material_preparation_type"
            ),
            fld(
                "genomic_material_preparation_process",
                "genomic_material_preparation_process",
            ),
            fld(
                "genomic_material_preparation_materials",
                "genomic_material_preparation_materials",
            ),
            fld("genomic_material_prepared_by", "genomic_material_prepared_by"),
            fld(
                "genomic_material_preparation_date",
                "genomic_material_preparation_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("scientific_name_notes", "scientific_name_notes"),
            fld(
                "id_vetting_date",
                "id_vetting_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "living_collections_material_sample_rna",
                re.compile(r"^living[\s]*_collections_material_sample_[rR][nN][aA]$"),
            ),
            fld("silica_gel_id", "silica_gel_id"),
            fld("country", "country", optional=True),
            fld("state_or_territory", "state_or_territory", optional=True),
            fld("location_id", "location_id", optional=True),
            fld("location_notes", "location_notes", optional=True),
            fld("population_group", "population_group", optional=True),
            fld("species_complex", "species_complex", optional=True),
            skp("decimal_latitude (will not be made public)"),
            skp("decimal_longitude (will not be made public)"),
            skp("description_googledoc_(remove before sending to gb)"),
        ]

        library_metadata = {}
        for sheet_name in self.sheet_names:
            wrapper = ExcelWrapper(
                self._logger,
                field_spec,
                fname,
                sheet_name=sheet_name,
                header_length=1,
                column_name_row_index=0,
                suggest_template=True,
            )
            for error in wrapper.get_errors():
                self._logger.error(error)

            name_mapping = {
                "decimal_longitude": "longitude",
                "decimal_latitude": "latitude",
                "klass": "class",
            }

            for row in wrapper.get_all():
                if not row.library_id or not row.dataset_id:
                    continue
                if (row.library_id, row.dataset_id) in library_metadata:
                    raise Exception(
                        "duplicate library / dataset id: {} {}".format(
                            row.library_id, row.dataset_id
                        )
                    )
                library_id = ingest_utils.extract_ands_id(self._logger, row.library_id)
                dataset_id = ingest_utils.extract_ands_id(self._logger, row.dataset_id)
                library_metadata[(library_id, dataset_id)] = row_meta = {}
                for field in row._fields:
                    value = getattr(row, field)
                    if field == "library_id" or field == "dataset_id":
                        continue
                    row_meta[name_mapping.get(field, field)] = value
        return library_metadata
