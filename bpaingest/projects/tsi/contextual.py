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


class TSILibraryContextual:
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/tsi_staging/metadata/2020-12-17/"
    ]
    metadata_patterns = [re.compile(r"^.*\.xlsx$")]
    name = "tsi-library-contextual"
    sheet_names = ["Sample metadata"]

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
                "sample_id",
                re.compile(r"sample_[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            # specimen_ID
            fld("specimen_id", re.compile(r"specimen_?[Ii][Dd]")),
            # specimen_ID_description
            fld(
                "specimen_id_description", re.compile(r"specimen_?[Ii][Dd]_description")
            ),
            # tissue_number
            fld("tissue_number", "tissue_number"),
            ## FIX
            ##            fld("voucher_or_tissue_number", "voucher_or_tissue_number"),
            # institution_name
            fld("institution_name", "institution_name"),
            # tissue_collection
            fld("tissue_collection", "tissue_collection"),
            # sample_custodian
            fld("sample_custodian", "sample_custodian"),
            # access_rights
            fld("access_rights", "access_rights"),
            # tissue_type
            fld("tissue_type", "tissue_type"),
            # tissue_preservation
            fld("tissue_preservation", "tissue_preservation"),
            # sample_quality
            fld("sample_quality", "sample_quality"),
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
            # subspecies
            fld("subspecies", "subspecies"),
            # common_name
            fld("common_name", "common_name"),
            # identified_by
            fld("identified_by", "identified_by"),
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
            # decimal_latitude
            fld("decimal_latitude", "decimal_latitude"),
            # decimal_longitude
            fld("decimal_longitude", "decimal_longitude"),
            # coord_uncertainty_metres
            fld("coord_uncertainty_metres", "coord_uncertainty_metres"),
            # genotypic sex
            fld("genotypic_sex", "genotypic sex"),
            # phenotypic sex
            fld("phenotypic_sex", "phenotypic sex"),
            # method of determination
            fld("method_of_determination", "method of determination"),
            # certainty
            fld("certainty", "certainty"),
            # life-stage
            fld("lifestage", "life-stage"),
            # birth_date
            fld("birth_date", "birth_date", coerce=ingest_utils.get_date_isoformat),
            # death_date
            fld("death_date", "death_date", coerce=ingest_utils.get_date_isoformat),
            # associated_media
            fld("associated_media", "associated_media"),
            # ancillary_notes
            fld("ancillary_notes", "ancillary_notes"),
            # barcode_ID
            fld("barcode_id", "barcode_id"),
            # ALA_specimen_URL
            fld("ala_specimen_url", re.compile(r"[aA][Ll][aA]_specimen_[uU][rR][lL]")),
            # prior_genetics
            fld("prior_genetics", "prior_genetics"),
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
