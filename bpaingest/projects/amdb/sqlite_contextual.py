import os
import re
import shutil
import sqlite3 as lite
import sys
import tempfile

import pandas

from .contextual import AustralianMicrobiomeSampleContextual

class AustralianMicrobiomeSampleContextualSQLite(AustralianMicrobiomeSampleContextual):
    metadata_patterns = [re.compile(r"^.*\.db$")]
    name = "amd-samplecontextualsqlite"
    source_pattern = "/*.db"
    db_table_name = "AM_metadata"

    def __init__(self, logger, path):
        super().__init__(logger, path)

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


class AustralianMicrobiomeSampleContextualSQLiteToExcelCopy(AustralianMicrobiomeSampleContextualSQLite):
    excel_file_copy_name = "context_metadata.xlsx"

    def dataframe_to_excel_file(self, df, fname):
        super().dataframe_to_excel_file(df, fname)
        shutil.copyfile(fname, os.path.join(self.path_dir, self.excel_file_copy_name))
        self._logger.info(f"Excel copy made: {self.excel_file_copy_name}")