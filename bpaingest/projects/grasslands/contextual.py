import re
from ...libs import ingest_utils
from ...libs.excel_wrapper import (
    make_field_definition as fld,
    make_skip_column as skp,
)
from ...abstract import BaseDatasetControlContextual
from ...abstract import BaseLibraryContextual



class AGDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/grasslands/dataset_control/2023-11-13/"
    ]
    name = "ag-dataset-contextual"
    contextual_linkage = ("library_id", "dataset_id")
    additional_fields = [
        fld('sample_id', 'sample_id', coerce=ingest_utils.extract_ands_id),
    ]


class AGLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/grasslands/metadata/2023-11-13/"
    ]
    name = "ag-library-contextual"
    sheet_names = [
        "Ref genomes",
    ]

    field_spec = [
            fld("data_type", "data_type"),
            fld("project_aim", "project_aim"),
            fld('team_lead_name', 'team_lead_name'),
            fld('team_lead_email', 'team_lead_email'),
            fld('sample_submitter_name', 'sample_submitter_name'),
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
            fld("nagoya_protocol_compliance", "nagoya_protocol_compliance", optional=True),
            fld("nagoya_protocol_permit_number", "nagoya_protocol_permit_number", optional=True),
            fld('ploidy', 'ploidy'),
            fld('common_name', 'common_name'),
            fld('taxon_id', 'taxon_id',
                coerce=ingest_utils.date_or_int_or_comment,),
            fld("scientific_name", "scientific_name"),
            fld("scientific_name_authorship", "scientific_name_authorship"),
            fld("family", "family"),
            fld("id_vetting_by", "id_vetting_by"),
            fld('id_vetting_date', 'id_vetting_date', coerce=ingest_utils.get_date_isoformat),
            fld('collected_by', 'collected_by'),
            fld('collection_date', 'collection_date', coerce=ingest_utils.get_date_isoformat),
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
            fld(
                "voucher_herbarium_event_date",
                "voucher_herbarium_event_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("preservation_type", "preservation_type"),
            fld("preservation_temperature", "preservation_temperature"),
            fld(
                "preservation_date_begin",
                "preservation_date_begin",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld('preservation_storage_location', 'preservation_storage_location'),
            fld("country", "country", optional=True),
            fld("state_or_territory", "state_or_territory", optional=True),
            fld("location_id", "location_id", optional=True),
            fld("location_notes", "location_notes", optional=True),
            fld("population_group", "population_group", optional=True),
            fld("species_complex", "species_complex", optional=True),
            # decimal_latitude
            fld("decimal_latitude", "decimal_latitude", optional=True),
            fld("decimal_latitude_public", "decimal_latitude", optional=True),
            # decimal_longitude
            fld("decimal_longitude", "decimal_longitude", optional=True),
            fld("decimal_longitude_public", "decimal_longitude", optional=True),
            fld('accession_number_seed', 'accession_number_seed'),
            fld('plant_structure', 'plant_structure'),
            fld('plant_developmental_stage', 'plant_developmental_stage'),
            fld('plant_growth_medium', 'plant_growth_medium'),
            fld('growth_facility', 'growth_facility'),
            fld('growth_condition', 'growth_condition'),
            fld('experimental_notes', 'experimental_notes'),
            fld('sample_replicate', 'sample_replicate'),
            fld('sample_material_associated_references', 'sample_material_associated_references'),
            fld('sample_material_preparation_type', 'sample_material_preparation_type'),
            fld('sample_material_preparation_process', 'sample_material_preparation_process'),
            fld('sample_material_prepared_by', 'sample_material_prepared_by'),
            fld('sample_material_preparation_date', 'sample_material_preparation_date',
                coerce=ingest_utils.get_date_isoformat),
            #
            skp("decimal_latitude (will not be made public)"),
            skp("decimal_longitude (will not be made public)"),
            skp("description_googledoc_(remove before sending to gb)"),
        ]

    def get(self, library_id, dataset_id):
        if (library_id, dataset_id) in self.library_metadata:
            return self.library_metadata[(library_id, dataset_id)]
        self._logger.warning(
            "no %s metadata available for: (%s,%s)"
            % (type(self).__name__, repr(library_id), repr(dataset_id))
        )
        return {}

    def process_row(self, row, library_metadata, metadata_filename, metadata_modified):
        # this is different to the base class because there are 2 fields used as the key. (library AND dataset ids)

        if not row.library_id or not row.dataset_id:
            return library_metadata
        if (row.library_id, row.dataset_id) in library_metadata:
            raise Exception("duplicate library / dataset id: {} {}".format(row.library_id, row.dataset_id))
        library_id = ingest_utils.extract_ands_id(self._logger, row.library_id)
        dataset_id = ingest_utils.extract_ands_id(self._logger, row.dataset_id)
        library_metadata[(library_id, dataset_id)] = row_meta = {}
        library_metadata[(library_id, dataset_id)]["metadata_revision_date"] = (
            ingest_utils.get_date_isoformat(self._logger, metadata_modified))
        library_metadata[(library_id, dataset_id)]["metadata_revision_filename"] = metadata_filename

        for field in row._fields:
            value = getattr(row, field)
            if field == "library_id" or field == "dataset_id":
                continue
            row_meta[self.name_mapping.get(field, field)] = value
        return library_metadata
