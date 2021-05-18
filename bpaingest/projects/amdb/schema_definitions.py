from glob import glob
import pandas
from ...util import one


class AustralianMicrobiomeSchema:
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/temp_amd/amd/metadata/schema/2021-05-07/"
    ]
    name = "amd-schema_definitions"
    source_pattern = "/*.xlsx"

    def __init__(self, logger, path):
        self._logger = logger
        self.path_dir = path
        self.source_path = one(glob(path + self.source_pattern))

    def

    def get_schema_definitions(self, use_cols=None, pandas_format=None):
        if use_cols is None:
            use_cols = ["Field", "dType", "AM_enviro", "Units_Definition", "Units"]
        if pandas_format is None:
            pandas_format = 'records'
        schema_as_dataframes = pandas.read_excel(
            self.source_path, sheet_name=0, usecols=use_cols
        )
        return schema_as_dataframes.to_dict(pandas_format)
