from glob import glob
import pandas
from ...util import one


class AustralianMicrobiomeSchema:
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/temp_amd/amd/metadata/schema/2021-05-07/"
    ]
    name = "amd-schema_definitions"
    source_pattern = "/*.xlsx"
    # .encode("utf8")
    use_cols = ["Field", "dType", "AM_enviro", "Units_Definition", "Units"]

    def __init__(self, logger, path):
        self._logger = logger
        self.path_dir = path
        source_path = one(glob(path + self.source_pattern))
        self.schema_definitions = self.initialise_source_path(source_path)

    def initialise_source_path(self, source_path):
        schema_as_dataframes = pandas.read_excel(
            source_path, sheet_name=0, usecols=self.use_cols
        )
        return schema_as_dataframes.to_dict('records')
