import os
from unipath import Path
from glob import glob
from urllib.parse import urljoin, quote

from ...libs.excel_wrapper import make_field_definition as fld
from ...libs import ingest_utils
from ...abstract import BaseMetadata
from ...util import (
    sample_id_to_ckan_name,
    apply_cc_by_license,
    clean_tag_name,
    clean_filename,
)
from . import files
from .runs import parse_run_data, BLANK_RUN


class WheatCultivarsMetadata(BaseMetadata):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/wheat_cultivars/tracking/"
    ]
    organization = "bpa-wheat-cultivars"
    ckan_data_type = "wheat-cultivars"
    sequence_data_type = "illumina-shortread"
    embargo_days = 365
    spreadsheet = {
        "fields": [
            fld("source_name", "Source Name"),
            fld("code", "CODE"),
            fld("sample_id", "BPA ID", coerce=ingest_utils.extract_ands_id),
            fld("characteristics", "Characteristics"),
            fld("organism", "Organism"),
            fld("variety", "Variety"),
            fld("organism_part", "Organism part"),
            fld("pedigree", "Pedigree"),
            fld("dev_stage", "Developmental stage"),
            fld("yield_properties", "Yield properties"),
            fld("morphology", "Morphology"),
            fld("maturity", "Maturity"),
            fld("pathogen_tolerance", "Pathogen tolerance"),
            fld("drought_tolerance", "Drought tolerance"),
            fld("soil_tolerance", "Soil tolerance"),
            fld("classification", "International classification"),
            fld("url", "Link"),
            fld("sample_name", "sample name"),
            fld(
                "factor_value_variety",
                "factor value[variety]",
                units="variety",
                coerce=ingest_utils.get_clean_number,
            ),
        ],
        "options": {"sheet_name": "Characteristics", "header_length": 1},
    }

    def __init__(self, logger, metadata_path, metadata_info=None):
        super().__init__(logger, metadata_path)
        self.metadata_info = metadata_info
        self.path = Path(metadata_path)
        self.runs = parse_run_data(self._logger, self.path)

    def _get_packages(self):
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing Stemcells Transcriptomics metadata file {0}".format(fname)
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                name = sample_id_to_ckan_name(sample_id)
                obj = {
                    "name": name,
                    "id": sample_id,
                    "sample_id": sample_id,
                    "title": sample_id,
                    "notes": "%s (%s): %s"
                    % (row.variety, row.code, row.classification),
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                    "license_id": apply_cc_by_license(),
                }
                ingest_utils.permissions_public(self._logger, obj)
                obj.update(
                    dict(
                        (t, getattr(row, t))
                        for t in (
                            "source_name",
                            "code",
                            "characteristics",
                            "classification",
                            "organism",
                            "variety",
                            "organism_part",
                            "pedigree",
                            "dev_stage",
                            "yield_properties",
                            "morphology",
                            "maturity",
                            "pathogen_tolerance",
                            "drought_tolerance",
                            "soil_tolerance",
                            "url",
                        )
                    )
                )
                tag_names = []
                if obj["organism"]:
                    tag_names.append(clean_tag_name(obj["organism"]))
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(
                self._logger, md5_file
            ):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = clean_filename(filename)
                resource.update(self.runs.get(resource["run"], BLANK_RUN))
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info["sample_id"]
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], "../all/" + quote(filename))
                resources.append(((sample_id,), legacy_url, resource))
        return resources
