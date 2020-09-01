import re
import sqlite3 as lite
import sys
import tempfile

import pandas

from .contextual import AustralianMicrobiomeSampleContextual
from ...libs.excel_wrapper import (
    ExcelWrapper,
    FieldDefinition,
)


class NotInVocabulary(Exception):
    pass


class AustralianMicrobiomeSampleContextualSQLite(AustralianMicrobiomeSampleContextual):
    metadata_patterns = [re.compile(r"^.*\.db$")]
    name = "amd-samplecontextualsqlite"
    source_pattern = "/*.db"
    db_table_name = "AM_metadata"

    def initialise_source_path(self, source_path):
        data_frames = self.get_sqlite_data(source_path)
        fo = tempfile.NamedTemporaryFile(suffix=".xlsx")
        self.dataframe_to_excel_file(data_frames, fo.name)
        super().initialise_source_path(fo.name)
        fo.close()

    def dataframe_to_excel_file(self, df, fname):
        writer = pandas.ExcelWriter(fname)
        df.to_excel(writer, sheet_name="Sqlite")
        writer.save()
        self._logger.info("Excel file written.")

    # simple method to ensure have working sqlite connection
    def get_sqlite_data(self, db_path):
        con = None
        try:
            con = lite.connect(db_path)
            self.validate_db_connection(con)
            return self.fetch_data(con)
        except lite.Error as e:
            self._logger.error("Error %s:" % e.args[0])
            sys.exit(1)
        finally:
            if con:
                con.close()

    def validate_db_connection(self, con):
        cur = con.cursor()
        cur.execute("SELECT SQLITE_VERSION()")
        data = cur.fetchone()
        self._logger.info("SQLite version: %s" % data)

    def fetch_data(self, con):
        return pandas.read_sql_query(f"SELECT * FROM {self.db_table_name}", con)

    @classmethod
    def units_for_fields(cls):
        r = {}
        for sheet_name, fields in cls.field_specs.items():
            for field in fields:
                if not isinstance(field, FieldDefinition):
                    continue
                if field.attribute in r and r[field.attribute] != field.units:
                    raise Exception("units inconsistent for field: {}", field.attribute)
                r[field.attribute] = field.units
        return r

    def sample_ids(self):
        return list(self.sample_metadata.keys())

    def get(self, sample_id):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        self._logger.warning(
            "no %s metadata available for: %s" % (type(self).__name__, repr(sample_id))
        )
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.sample_id is None:
                continue
            if row.sample_id in sample_metadata:
                raise Exception(
                    "Metadata invalid, duplicate sample ID {} in row {}".format(
                        row.sample_id, row
                    )
                )
            assert row.sample_id not in sample_metadata
            sample_metadata[row.sample_id] = row_meta = {}
            for field in row._fields:
                val = getattr(row, field)
                if field != "sample_id":
                    row_meta[field] = val
        return sample_metadata

    @staticmethod
    def environment_for_sheet(sheet_name):
        return "Soil" if sheet_name == "Soil" else "Marine"

    def _read_metadata(self, metadata_path):
        rows = []
        for sheet_name, field_spec in sorted(self.field_specs.items()):
            wrapper = ExcelWrapper(
                self._logger,
                field_spec,
                metadata_path,
                sheet_name=sheet_name,
                header_length=1,
                column_name_row_index=0,
                suggest_template=True,
                additional_context={},
            )
            for error in wrapper.get_errors():
                self._logger.error(error)
            rows += wrapper.get_all()
        return rows

    def filename_metadata(self, *args, **kwargs):
        return {}
