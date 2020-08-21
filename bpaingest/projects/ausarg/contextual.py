import re
from glob import glob
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper, make_field_definition as fld
from ...util import make_logger, one


def date_or_str(logger, v):
    d = ingest_utils.get_date_isoformat(logger, v, silent=True)
    if d is not None:
        return d
    return v


class AusargLibraryContextual:
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/metadata/2020-08-19/"
    ]
    metadata_patterns = [re.compile(r"^.*\.xlsx$")]
    name = "ausarg-library-contextual"
    sheet_names = ["Sample_metadata"]

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
            fld("sample_id", "sample_id", coerce=ingest_utils.extract_ands_id),
            fld("specimenid", "specimenid"),
            fld("specimenid_description", "specimenid_description"),
            fld("tissue_number", "tissue_number"),
            fld("voucher_or_tissue_number", "voucher_or_tissue_number"),
            fld("institution_name", "institution_name"),
            fld("tissue_collection", "tissue_ collection"),
            fld("sample_custodian", "sample_custodian"),
            fld("access_rights", "access_rights"),
            fld("tissue_type", "tissue_type"),
            fld("tissue_preservation", "tissue_preservation"),
            fld("sample_quality", "sample_quality"),
            fld("taxon_id", "taxon_id"),
            fld("phylum", "phylum"),
            fld("klass", "class"),
            fld("order", "order"),
            fld("family", "family"),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("subspecies", "subspecies"),
            fld("common_name", "common_name"),
            fld("identified_by", "identified_by"),
            fld(
                "collection_date",
                "collection_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("collector", "collector"),
            fld("collection_method", "collection_method"),
            fld("collector_sample_id", "collector_sample_id"),
            fld("wild_captive", "wild_captive"),
            fld("source_population", "source_population"),
            fld("country", "country"),
            fld("state_or_region", "state_or_region"),
            fld("location_text", "location_text"),
            fld("habitat", "habitat"),
            fld("decimal_latitude", "decimal_latitude"),
            fld("decimal_longitude", "decimal_longitude"),
            fld("coord_uncertainty_metres", "coord_uncertainty_metres"),
            fld("genotypic_sex", "genotypic sex"),
            fld("phenotypic_sex", "phenotypic sex"),
            fld("method_of_determination", "method of determination"),
            fld("certainty", "certainty"),
            fld("lifestage", "life-stage"),
            fld("birth_date", "birth_date", coerce=ingest_utils.get_date_isoformat),
            fld("death_date", "death_date", coerce=ingest_utils.get_date_isoformat),
            fld("associated_media", "associated_media"),
            fld("ancillary_notes", "ancillary_notes"),
            fld("barcode_id", "barcode_id"),
            fld("ala_specimen_url", "ala_specimen_url"),
            fld("prior_genetics", "prior_genetics"),
            fld("taxonomic_group", "taxonomic_group"),
            fld("type_status", "type_status"),
            fld("material_extraction_type", "material_extraction_type"),
            fld(
                "material_extraction_date",
                "material_extraction_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("material_extracted_by", "material_extracted_by"),
            fld("material_extraction_method", "material_extraction_method"),
            fld("material_conc_ng_ul", "material_conc_ng_ul"),
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
                if not row.sample_id:
                    continue
                if row.sample_id in library_metadata:
                    raise Exception("duplicate sample id: {}".format(row.sample_id))
                sample_id = ingest_utils.extract_ands_id(self._logger, row.sample_id)
                library_metadata[sample_id] = row_meta = {}
                for field in row._fields:
                    value = getattr(row, field)
                    if field == "sample_id":
                        continue
                    row_meta[name_mapping.get(field, field)] = value
        return library_metadata
