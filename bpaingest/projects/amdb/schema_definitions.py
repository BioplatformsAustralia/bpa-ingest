from glob import glob
import pandas
from numpy import nan
import unicodedata
from ...util import one


class AustralianMicrobiomeSchema:
    metadata_urls = [
        "https://github.com/AusMicrobiome/contextualdb_doc/raw/4.2.0/db_schema_definitions/db_schema_definitions.xlsx"
    ]
    name = "amd-schema_definitions"
    sheet_name = "Schema_4.2.0"
    source_pattern = "/*.xlsx"

    def __init__(self, logger, path):
        self._logger = logger
        self.path_dir = path
        self.source_path = one(glob(path + self.source_pattern))
        self.schema_definitions = None

    def get_schema_definitions(self, use_cols=None, pandas_format=None):
        if self.schema_definitions is None:
            if use_cols is None:
                use_cols = ["Field", "dType", "AM_enviro", "Units_Definition", "Units"]
            if pandas_format is None:
                pandas_format = "records"
            schema_as_dataframes = pandas.read_excel(
                self.source_path, sheet_name=self.sheet_name, usecols=use_cols
            )
            self.schema_definitions = schema_as_dataframes.to_dict(pandas_format)
        return self.schema_definitions

    def validate_schema_units(self, context_field_specs):
        self._logger.info("comparing units...")
        missing_values = [None, nan]
        schema_definitions = {
            s["Field"]: s["Units"] for s in self.get_schema_definitions()
        }
        context_definitions = {c.column_name: c.units for c in context_field_specs}
        for s in schema_definitions:
            if s not in context_definitions:
                self._logger.error(
                    f"Schema definition column: {s} not found in context class"
                )
        for c in context_definitions:
            if c not in schema_definitions:
                self._logger.error(
                    f"Context Class column: {c} not found in schema class"
                )
            if (
                schema_definitions[c] in missing_values
                and context_definitions[c] in missing_values
            ):
                continue
            if schema_definitions[c] and context_definitions[c]:
                if make_unicode(schema_definitions[c]).rstrip() != make_unicode(context_definitions[c]).strip() and (
                        'yyyy-mm-dd' not in schema_definitions[c] or
                        'hh:mm:ss' not in schema_definitions[c]):
                    self._logger.error(
                        f"Units in Context column: {c} is {context_definitions[c]}, but in the schema it is: {schema_definitions[c]}"
                    )
                    context_first_char = unicodedata.name(context_definitions[c][0])
                    schema_first_char = unicodedata.name(schema_definitions[c][0][0])
                    context_first_hex = hex(ord(context_definitions[c][0]))
                    schema_first_hex = hex(ord(schema_definitions[c][0]))
                    if context_first_char != schema_first_char:
                        self._logger.error(
                            f"Unicode Units in Context column: {c} is {context_first_char}, but in the schema it is: {schema_first_char }"
                        )
                        self._logger.error(
                            f"HEX Units in Context column: {c} is {context_first_hex}, in the schema it is: {schema_first_hex}"
                        )

    def validate_schema_datatypes(self, context_field_specs):
        self._logger.info("comparing datatypes...")
        missing_values = [None, nan]
        schema_definitions = {
            s["Field"]: s["dType"] for s in self.get_schema_definitions()
        }

        context_definitions = {c.column_name: [c.coerce, c.units] for c in context_field_specs}

        for s in schema_definitions:
            if s not in context_definitions:
                self._logger.error(
                    f"validate_schema: Schema definition column: {s} not found in context class"
                )
        for c in context_definitions:
            if c not in schema_definitions:
                self._logger.error(
                    f"validate_schema: Context Class column: {c} not found in schema class"
                )

            if context_definitions[c][0]:
                context_type = context_definitions[c][0].__name__
            else:
                context_type = 'None'

            if context_definitions[c][1]:
                context_units = context_definitions[c][1]
            else:
                context_units = 'None'

            if not( ('TEXT' in schema_definitions[c] and context_type == 'None')
                    or ('TEXT PRIMARY KEY' in schema_definitions[c] and context_type == 'ands_orSAMN')
                    or ('DATE,' in schema_definitions[c] and context_type == 'get_date_isoformat')
                    or ('DATETIME' in schema_definitions[c] and context_type == 'get_date_isoformat_as_datetime')
                    or ('TIME,' in schema_definitions[c] and context_type == 'get_time')
                    or ('NUMERIC' in schema_definitions[c] and context_units != '%' and context_type == 'get_clean_number')
                    or ('NUMERIC' in schema_definitions[c] and context_units == '%' and context_type == 'get_percentage')
                   ):
                self._logger.error(
                    f"validate_schema: Field: {c}, Schema type: {schema_definitions[c]}, Context Type: {context_type}, Units: {context_units}")


def make_unicode(value):
    value = str(value)
    return unicodedata.normalize("NFKC", value)

