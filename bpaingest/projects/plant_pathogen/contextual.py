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


class PlantPathogenDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/pp_staging/dataset_control/2023-03-13/"
    ]
    name = "pp-dataset-contextual"
    sheet_names = [
        "Data control",
    ]
    contextual_linkage = ('bioplatforms_library_id',)
    additional_fields = [
        fld('bioplatforms_sample_id', 'bioplatforms_sample_id', coerce=ingest_utils.extract_ands_id,),
        fld('bioplatforms_dataset_id', 'bioplatforms_dataset_id', coerce=ingest_utils.extract_ands_id,),
        fld('bioplatforms_project_code', 'bioplatforms_project_code'),
        fld('data_type', 'data_type'),
    ]


class PlantPathogenLibraryContextual:
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/pp_staging/metadata/2023-02-28/"
    ]
    metadata_patterns = [re.compile(r"^.*\.xlsx$")]
    name = "pp-library-contextual"
    sheet_names = ["Virus",
                   "Bacteria",
                   "Fungi"
                   ]

    def __init__(self, logger, path):
        self._logger = logger
        self._logger.info("context path is: {}".format(path))
        self.library_metadata = self._read_metadata(one(glob(path + "/*.xlsx")))

    def get(self, identifier):
        if identifier in self.library_metadata:
            return self.library_metadata[identifier]
        self._logger.warning(
            "no %s metadata available for: %s" % (type(self).__name__, repr(identifier))
        )
        return {}

    def _read_metadata(self, fname):
        field_spec = [
            # sample_ID
            fld(
                "bioplatforms_sample_id",
                re.compile(r"(bioplatforms_)?sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_library_id",
                re.compile(r"(bioplatforms_)?library_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "bioplatforms_dataset_id",
                re.compile(r"(bioplatforms_)?dataset_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld('project_lead', 'project_lead'),
            fld('data_context', 'data_context'),
            fld('specimen_custodian', 'specimen_custodian'),
            fld('specimen_id', 'specimen_id', coerce=ingest_utils.int_or_comment,),
            fld('specimen_id_description', 'specimen_id_description'),
            fld('sample_custodian', 'sample_custodian'),
            fld('sample_collection_type', 'sample_collection_type'),
            fld('sample_type', 'sample_type'),
            fld('taxon_id', 'taxon_id',  coerce=ingest_utils.get_int),
            fld('phylum', 'phylum'),
            fld('klass', 'class'),
            fld('order', 'order'),
            fld('family', 'family'),
            fld('genus', 'genus'),
            fld('species', 'species'),
            fld('sub_species', 'sub_species'),
            fld('scientific_name', 'scientific_name'),
            fld('scientific_name_authorship', 'scientific_name_authorship'),
            fld('scientific_name_note', 'scientific_name_note'),
            fld('common_name', 'common_name'),
            fld('taxonomic_group', 'taxonomic_group'),
            fld('isolate', 'isolate'),
            fld('host_common_name', 'host_common_name'),
            fld('host_family', 'host_family'),
            fld('host_scientific_name', 'host_scientific_name'),
            fld('host_organ', 'host_organ'),
            fld('host_symptom', 'host_symptom'),
            fld('host_status', 'host_status'),
            fld('tissue', 'tissue'),
            fld('tissue_preservation', 'tissue_preservation'),
            fld('tissue_preservation_temperature', 'tissue_preservation_temperature'),
            fld('sample_quality', 'sample_quality'),
            fld('wild_captive', 'wild_captive'),
            fld('collection_date', 'collection_date', coerce=ingest_utils.get_date_isoformat),
            fld('collector', 'collector'),
            fld('collection_method', 'collection_method'),
            fld('collection_permit', 'collection_permit'),
            fld('collector_sample_id', 'collector_sample_id'),
            fld('source_population', 'source_population'),
            fld('country', 'country'),
            fld('state_or_region', 'state_or_region'),
            fld('location_text', 'location_text'),
            fld('location_info_restricted', 'location_info_restricted'),
            fld('coord_uncertainty_metres', 'coord_uncertainty_metres'),
            fld('decimal_latitude_public', 'decimal_latitude_public'),
            fld('decimal_longitude_public', 'decimal_longitude_public'),
            fld('life_stage', 'life_stage'),
            fld('health_state', 'health_state'),
            fld('associated_media', 'associated_media'),
            fld('ancillary_notes', 'ancillary_notes'),
            fld('type_status', 'type_status'),
            fld('material_extraction_type', 'material_extraction_type'),
            fld('material_extraction_date', 'material_extraction_date', coerce=ingest_utils.get_date_isoformat),
            fld('material_extracted_by', 'material_extracted_by'),
            fld('material_extraction_method', 'material_extraction_method'),
            fld('material_conc_ng_ul', 'material_conc_ng_ul'),
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
                # use sample_id as unique identifier (no library ID exists in context atm)
                if not row.bioplatforms_library_id:
                    continue
                if row.bioplatforms_library_id in library_metadata:
                    raise Exception("duplicate library id: {}".format(row.bioplatforms_library_id))
                bioplatforms_library_id =  row.bioplatforms_library_id
                library_metadata[row.bioplatforms_library_id] = row_meta = {}
                for field in row._fields:
                    value = getattr(row, field)
                    if field == "bioplatforms_library_id":
                        continue
                    row_meta[name_mapping.get(field, field)] = value
        return library_metadata
