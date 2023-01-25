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


class FungiDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/fungi_staging/dataset_control/2022-12-08/"
    ]
    name = "fungi-dataset-contextual"
    contextual_linkage = ("sample_id",)
    additional_fields = [
        fld('library_id', 'library_id'),
        fld('dataset_id', 'dataset_id'),
    ]



class FungiLibraryContextual:
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/fungi_staging/metadata/2022-12-08/"
    ]
    metadata_patterns = [re.compile(r"^.*\.xlsx$")]
    name = "fungi-library-contextual"
    sheet_names = ["Sample metadata"]

    def __init__(self, logger, path):
        self._logger = logger
        self._logger.info("context path is: {}".format(path))
        self.library_metadata = self._read_metadata(one(glob(path + "/*.xlsx")))

    def get(self, identifier):
        if identifier in self.library_metadata:
            return self.library_metadata[identifier]
        self._logger.warning(
            "no %s FUNGI metadata available for: %s" % (type(self).__name__, repr(identifier))
        )
        return {}

    def _read_metadata(self, fname):
        field_spec = [
            fld(
                "bioplatforms_sample_id",
                "bioplatforms_sample_id",
                coerce=ingest_utils.extract_ands_id,
            ),

            # sample_ID
            fld(
                "sample_id",
                "sample_id",
            ),
            fld("specimen_custodian", "specimen_custodian"),
            # specimen_ID
            fld("specimen_id", re.compile(r"specimen_?[Ii][Dd]")),
            # specimen_ID_description
            fld(
                "specimen_id_description", re.compile(r"specimen_?[Ii][Dd]_description")
            ),
            fld("sample_custodian", "sample_custodian"),
            fld('sample_type', 'sample_type'),
            # sample_ID_description
            fld(
                "sample_id_description", re.compile(r"sample_?[Ii][Dd]_description")
            ),
            fld('sample_collection_type', 'sample_collection_type'),
            fld('tissue', 'tissue'),
            # tissue_preservation
            fld("tissue_preservation", "tissue_preservation"),
            fld('tissue_preservation_temperature', 'tissue_preservation_temperature'),
            # sample_quality
            fld("sample_quality", "sample_quality"),
            fld('collection_permit', 'collection_permit'),
            fld('identified_by', 'identified_by'),
            fld('env_broad_scale', 'env_broad_scale'),
            fld('env_local_scale', 'env_local_scale'),
            fld('env_medium', 'env_medium'),
            fld('altitude', 'altitude'),
            fld('depth', 'depth', coerce=ingest_utils.get_clean_number),
            fld('temperature', 'temperature'),
            fld('location_info_restricted', 'location_info_restricted'),
            fld('genotypic_sex', 'genotypic_sex'),
            fld('phenotypic_sex', 'phenotypic_sex'),
            fld('method_sex_determination', 'method_sex_determination'),
            fld('sex_certainty', 'sex_certainty'),
            # taxon_id
            fld("taxon_id", re.compile(r"taxon_[Ii][Dd]")),
            # phylum
            fld("phylum", "phylum"),
            # class
            fld("klass", "class"),
            # order
            fld("order", "order"),
            # family
            fld("family", "family"),
            # genus
            fld("genus", "genus"),
            # species
            fld("species", "species"),
            fld("sub_species", "sub_species"),
            fld('scientific_name', 'scientific_name'),
            fld('scientific_name_note', 'scientific_name_note'),
            fld('scientific_name_authorship', 'scientific_name_authorship'),
            # common_name
            fld("common_name", "common_name"),
            # collection_date
            fld(
                "collection_date",
                "collection_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            # collector
            fld("collector", "collector"),
            # collection_method
            fld("collection_method", "collection_method"),
            # collector_sample_ID
            fld("collector_sample_id", re.compile(r"collector_sample_[Ii][Dd]")),
            # wild_captive
            fld("wild_captive", "wild_captive"),
            # source_population
            fld("source_population", "source_population"),
            # country
            fld("country", "country"),
            # state_or_region
            fld("state_or_region", "state_or_region"),
            # location_text
            fld("location_text", "location_text"),
            # habitat
            fld("habitat", "habitat"),
            # skip the private lat/long as this will contain data not to be shared
            skp("decimal_latitude_private"),
            skp("decimal_longitude_private"),
            # decimal_latitude
            fld("decimal_latitude", "decimal_latitude_public"),
            fld("decimal_latitude_public", "decimal_latitude_public"),
            # decimal_longitude
            fld("decimal_longitude", "decimal_longitude_public"),
            fld("decimal_longitude_public", "decimal_longitude_public"),
            # coord_uncertainty_metres
            fld("coord_uncertainty_metres", "coord_uncertainty_metres", optional=True),
            # life-stage
            fld("lifestage", re.compile("life[_-]stage")),
            # birth_date
            fld("birth_date", "birth_date", coerce=ingest_utils.get_date_isoformat),
            # death_date
            fld("death_date", "death_date", coerce=ingest_utils.get_date_isoformat),
            fld('health_state', 'health_state'),
            # associated_media
            fld("associated_media", "associated_media"),
            # ancillary_notes
            fld("ancillary_notes", "ancillary_notes"),
            # taxonomic_group
            fld("taxonomic_group", "taxonomic_group"),
            # type_status
            fld("type_status", "type_status"),
            # material_extraction_type
            fld("material_extraction_type", re.compile(r"[Mm]aterial_extraction_type")),
            # material_extraction_date
            fld(
                "material_extraction_date",
                re.compile(r"[Mm]aterial_extraction_date"),
                coerce=ingest_utils.get_date_isoformat,
            ),
            # material_extracted_by
            fld("material_extracted_by", re.compile(r"[Mm]aterial_extracted_by")),
            # material_extraction_method
            fld(
                "material_extraction_method",
                re.compile(r"[Mm]aterial_extraction_method"),
            ),
            # material_conc_ng_ul
            fld("material_conc_ng_ul", re.compile(r"[Mm]aterial_conc_ng_ul")),
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
                if not row.bioplatforms_sample_id:
                    continue
                if row.bioplatforms_sample_id in library_metadata:
                    raise Exception("duplicate sample id: {}".format(row.bioplatforms_sample_id))
                bioplatforms_sample_id = ingest_utils.extract_ands_id(self._logger, row.bioplatforms_sample_id)
                library_metadata[bioplatforms_sample_id] = row_meta = {}
                for field in row._fields:
                    value = getattr(row, field)
                    if field == "sample_id":
                        continue
                    row_meta[name_mapping.get(field, field)] = value
        return library_metadata
