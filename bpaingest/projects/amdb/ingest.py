import os
import re
from glob import glob
from urllib.parse import urljoin

from unipath import Path

from . import files
from .schema_definitions import AustralianMicrobiomeSchema
from .sqlite_contextual import (
    AustralianMicrobiomeSampleContextualSQLite,
    AustralianMicrobiomeSampleContextualSQLiteToExcelCopy,
)
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import (
    apply_license,
    sample_id_to_ckan_name,
    common_values,
    one,
)
from .contextual import (
    AustralianMicrobiomeSampleContextual,
    BASENCBIContextual,
    MarineMicrobesNCBIContextual,
    AustralianMicrobiomeDatasetControlContextual,
)
from .tracking import (
    AustralianMicrobiomeGoogleTrackMetadata,
    BASETrackMetadata,
    MarineMicrobesGoogleTrackMetadata,
    MarineMicrobesTrackMetadata,
)

common_context = [
    AustralianMicrobiomeSampleContextual,
    AustralianMicrobiomeDatasetControlContextual,
]
ncbi_context = [
    BASENCBIContextual,
    MarineMicrobesNCBIContextual,
]

# fixed read lengths provided by AB at CSIRO
amplicon_read_length = {
    "16S": "300bp",
    "A16S": "300bp",
    "ITS": "300bp",
    "18S": "150bp",
}
common_skip = [
    re.compile(r"^.*_metadata.*\.xlsx$"),
    re.compile(r"^.*SampleSheet.*"),
    re.compile(r"^.*TestFiles\.exe.*"),
]

CONSORTIUM_ORG_NAME = "AM Consortium Members"
# This is the URL slug of the organization whose members are
# permitted access during the embargo period
CONSORTIUM_ORG_NAME = "am-consortium-members"


def base_amplicon_read_length(amplicon):
    return amplicon_read_length[amplicon]


def build_base_amplicon_linkage(index_linkage, flow_id, index):
    # build linkage, `index_linkage` indicates whether we need
    # to include index in the linkage
    if index_linkage:
        # strip out _ and - as usage inconsistent in pilot data
        return flow_id + "_" + index.replace("-", "").replace("_", "")
    return flow_id


def build_contextual_field_names():
    field_names = {}
    lookup = AustralianMicrobiomeSampleContextual.units_for_fields()
    for field, units in lookup.items():
        if units is None:
            continue
        field_names[field] = "{} ({})".format(field, units.strip())
    return field_names


class AMDBaseMetadata(BaseMetadata):
    package_field_names = build_contextual_field_names()
    sql_to_excel_context_classes = [
        AustralianMicrobiomeSampleContextualSQLiteToExcelCopy
    ]
    # to validate schema class, uncomment this attribute
    # schema_classes = [AustralianMicrobiomeSchema]

    notes_mapping = [
        {"key": "env_material_control_vocab_0", "separator": ", "},
        {"key": "sample_site_location_description", "separator": ", "},
        {"key": "geo_loc_country_subregion", "separator": ", "},
        {"key": "data_type", "separator": " "},
        {"key": "analytical_platform"},
    ]

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger)
        self.path = Path(metadata_path)
        if kwargs.get("schema_definitions"):
            self.schema_definitions = kwargs["schema_definitions"]
            self.validate_schema_units()
        self.linkage_xlsx = {}

    def validate_schema_units(self):
        self._logger.info(
            "validating current schema units against current schema definitions file..."
        )
        if len(self.contextual_classes) != 1:
            raise Exception("Can only compare 1 contextual metadata file")
        context_object = self.contextual_classes[0]
        context_sheet_name = context_object.sheet_name
        if len(self.schema_definitions) != 1:
            raise Exception("Can only compare 1 contextual metadata file")
        schema_object = self.schema_definitions[0]
        schema_object.validate_schema_units(
            context_object.field_specs[context_sheet_name]
        )
        self._logger.info("Validation completed.")


class AMDFullIngestMetadata(AMDBaseMetadata):
    """
    classes for ingest proper (excludes `AccessAMDContextualMetadata` which is used by OTU)
    """

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.metadata_info = kwargs["metadata_info"]
        self.all_md5_filenames = [
            f for f in self.metadata_info.keys() if re.match(".*\.md5", f)
        ]


class AccessAMDContextualMetadata(AMDBaseMetadata):
    """
    for use by tools (e.g. bpaotu) which need access to the contextual metadata for all
    AMD data, but not package or resource metadata
    """

    contextual_classes = common_context + ncbi_context
    metadata_urls = []

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.contextual_metadata = kwargs["contextual_metadata"]

    def _get_packages(self):
        return []

    def _get_resources(self):
        return []


def filter_out_metadata_fields(source):
    for filtered in ["base_amplicon_linkage"]:
        source.pop(filtered, "")


class BASEAmpliconsMetadata(AMDFullIngestMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "base-genomics-amplicon"
    omics = "genomics"
    technology = "amplicons"
    sequence_data_type = "illumina-amplicons"
    embargo_days = 90
    contextual_classes = common_context + ncbi_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/base/raw/amplicons/",
    ]
    metadata_url_components = ("amplicon", "facility_code", "ticket")
    resource_linkage = ("sample_extraction_id", "amplicon", "base_amplicon_linkage")
    # pilot data
    index_linkage_spreadsheets = ("BASE_18S_UNSW_A6BRJ_metadata.xlsx",)
    index_linkage_md5s = ("BASE_18S_UNSW_A6BRJ_checksums.md5",)
    tickets_with_bad_data = [
        "BRLOPS-268",
        "BRLOPS-418",
        "BRLOPS-675",
        "BRLOPS-528",
        "BRLOPS-442",
        "BRLOPS-309",
        "BRLOPS-269",
        "BRLOPS-673",
        "BRLOPS-302",
        "BRLOPS-303",
        "BRLOPS-443",
        "BRLOPS-442",
        "BRLOPS-498",
        "BRLOPS-499",
        "BRLOPS-270",
        "BRLOPS-678",
    ]
    bad_resource_linkages = [
        ("19299_1", "18S", "AF4PN"),
        ("7046_1", "18S", "A78B7"),
        ("7052_1", "18S", "A78B7"),
        ("12887_1", "ITS", "A64JJ"),
    ]
    # FIXME: these to be corrected with the real dates (all early data)
    # work-around put in place to proceed with NCBI upload GB 10/01/2018
    missing_ingest_dates = [
        "BRLOPS-215",
        "BRLOPS-268",
        "BRLOPS-269",
        "BRLOPS-270",
        "BRLOPS-273",
        "BRLOPS-288",
        "BRLOPS-289",
        "BRLOPS-290",
        "BRLOPS-291",
        "BRLOPS-301",
        "BRLOPS-302",
        "BRLOPS-303",
        "BRLOPS-309",
        "BRLOPS-311",
        "BRLOPS-382",
        "BRLOPS-419",
        "BRLOPS-652",
        "BRLOPS-677",
    ]
    # spreadsheet
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                re.compile(r".*sample unique [Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_extraction_id",
                re.compile(r".*[Ss]ample extraction [Ii][dD](|_1)"),
                coerce=ingest_utils.fix_sample_extraction_id,
            ),
            fld("sequencing_facility", "Sequencing facility", optional=True),
            fld("target", "Target", coerce=lambda _, s: s.upper().strip()),
            fld("index", "Index", coerce=lambda _, s: s[:12], optional=True),
            fld("index1", "Index 1", coerce=lambda _, s: s[:12], optional=True),
            fld("index2", "Index2", coerce=lambda _, s: s[:12], optional=True),
            fld(
                "pcr_1_to_1",
                "1:1 PCR, P=pass, F=fail",
                coerce=ingest_utils.fix_pcr,
                optional=True,
            ),
            fld(
                "pcr_1_to_10",
                "1:10 PCR, P=pass, F=fail",
                coerce=ingest_utils.fix_pcr,
                optional=True,
            ),
            fld(
                "pcr_1_to_100",
                "1:100 PCR, P=pass, F=fail",
                coerce=ingest_utils.fix_pcr,
                optional=True,
            ),
            fld(
                "pcr_neat",
                "neat PCR, P=pass, F=fail",
                coerce=ingest_utils.fix_pcr,
                optional=True,
            ),
            fld("pcr", "P=pass / F=fail", coerce=ingest_utils.fix_pcr, optional=True),
            fld("dilution", "Dilution used", coerce=ingest_utils.fix_date_interval),
            fld("sequencing_run_number", "Sequencing run number", optional=True),
            fld("flow_cell_id", "Flowcell", optional=True),
            fld("reads", ("# of RAW reads", "# of reads"), coerce=ingest_utils.get_int),
            fld("sample_name", "Sample name on sample sheet", optional=True),
            fld("analysis_software_version", "AnalysisSoftwareVersion"),
            fld("comments", re.compile(r"comments(|1)"), optional=True, find_all=True),
            fld("comments2", re.compile(r"comments2"), optional=True),
            fld("comments3", re.compile(r"comments3"), optional=True),
        ],
        "options": {"header_length": 2, "column_name_row_index": 1,},
    }
    md5 = {
        "match": files.base_amplicon_regexps,
        "skip": common_skip + files.base_amplicon_control_regexps,
    }

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.contextual_metadata = kwargs["contextual_metadata"]
        self.track_meta = BASETrackMetadata()

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        self._logger.info("Ingesting BASE Amplicon metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing BASE Amplicon metadata file {0}".format(
                    os.path.basename(fname)
                )
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)
                flow_id = get_flow_id(fname)

                def track_get(k):
                    if row.ticket.strip() in self.missing_ingest_dates and k.startswith(
                        "date_of_transfer"
                    ):
                        return "2015-01-01"
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                sample_id = row.sample_id
                if sample_id is None:
                    continue
                sample_extraction_id = ingest_utils.make_sample_extraction_id(
                    row.sample_extraction_id, sample_id
                )
                base_fname = os.path.basename(fname)
                index_linkage = base_fname in self.index_linkage_spreadsheets
                base_amplicon_linkage = build_base_amplicon_linkage(
                    index_linkage, flow_id, row.index
                )
                if index_linkage:
                    note_extra = "%s %s" % (flow_id, row.index)
                else:
                    note_extra = flow_id
                obj = row._asdict()
                amplicon = row.amplicon.upper()
                name = sample_id_to_ckan_name(
                    sample_extraction_id,
                    self.ckan_data_type + "-" + amplicon,
                    base_amplicon_linkage,
                )
                archive_ingestion_date = ingest_utils.get_date_isoformat(
                    self._logger, track_get("date_of_transfer_to_archive")
                )

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_type": "Soil",
                        "read_length": base_amplicon_read_length(
                            amplicon
                        ),  # hard-coded for now, on advice of AB at CSIRO
                        "sample_id": sample_id,
                        "flow_id": flow_id,
                        "base_amplicon_linkage": base_amplicon_linkage,
                        "sample_extraction_id": sample_extraction_id,
                        "amplicon": amplicon,
                        "title": "BASE Amplicons %s %s %s"
                        % (amplicon, sample_extraction_id, note_extra),
                        "contextual_data_submission_date": None,
                        "facility": row.facility_code.upper(),
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer")
                        ),
                        "data_type": track_get("data_type"),
                        "description": track_get("description"),
                        "folder_name": track_get("folder_name"),
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer")
                        ),
                        "data_generated": ingest_utils.get_date_isoformat(
                            self._logger, track_get("date_of_transfer_to_archive")
                        ),
                        "archive_ingestion_date": archive_ingestion_date,
                        "license_id": apply_license(archive_ingestion_date),
                        "dataset_url": track_get("download"),
                    }
                )
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                for contextual_source in self.contextual_metadata:
                    filter_out_metadata_fields(contextual_source.get(sample_id))
                    obj.update(contextual_source.get(sample_id))
                ingest_utils.apply_access_control(self._logger, self, obj)
                ingest_utils.add_spatial_extra(self._logger, obj)
                self.build_notes_into_object(obj)
                tag_names = ["amplicons", amplicon, obj["sample_type"]]
                obj["tags"] = [{"name": t} for t in tag_names]
                if not self.is_bad_package(obj):
                    self.track_packages_for_md5(obj, row.ticket)
                    packages.append(obj)
        return packages

    # these packages were set `not to upload` in metadata sheet
    def is_bad_package(self, obj):
        package_link = tuple(obj[t] for t in self.resource_linkage)
        reads = int(obj.get("reads") or 0)
        ticket = obj.get("ticket") or ""
        return (
            # // what is considered a low read is relative - a small number of resource_linkages are bad as special cases
            ticket in self.tickets_with_bad_data
            and reads < 1200
        ) or package_link in self.bad_resource_linkages

    def _get_resources(self):
        self._logger.info(
            "Ingesting BASE Amplicon md5 file information from {0}".format(self.path)
        )
        resources = []

        for md5_file in glob(self.path + "/*.md5"):
            index_linkage = os.path.basename(md5_file) in self.index_linkage_md5s
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_extraction_id = (
                    sample_id.split("/")[-1] + "_" + file_info.get("extraction")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (
                            sample_extraction_id,
                            resource["amplicon"],
                            build_base_amplicon_linkage(
                                index_linkage, resource["flow_id"], resource["index"]
                            ),
                        ),
                        legacy_url,
                        resource,
                    )
                )
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


class BASEAmpliconsControlMetadata(AMDFullIngestMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "base-genomics-amplicon-control"
    omics = "genomics"
    technology = "amplicons-control"
    sequence_data_type = "illumina-amplicons"
    embargo_days = 90
    ## TODO: control classes don't normally have context, but historically this has always been here. Probably harmless
    # as without the relevant ID, context will be skipped over, but at some point sequencing metadata IDs should be checked to see
    # if it is TRUE that none exist in context and then safely removed.
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/base/raw/amplicons/",
    ]
    metadata_url_components = ("amplicon", "facility_code", "ticket")
    resource_linkage = ("amplicon", "flow_id")
    md5 = {
        "match": files.base_amplicon_control_regexps,
        "skip": common_skip + files.base_amplicon_regexps,
    }

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.contextual_metadata = kwargs["contextual_metadata"]
        self.track_meta = BASETrackMetadata()

    def _get_packages(self):
        flow_id_ticket = dict(
            ((t["amplicon"], t["flow_id"]), self.metadata_info[os.path.basename(fname)])
            for _, _, fname, t in self.md5_lines()
        )
        packages = []
        for (amplicon, flow_id), info in sorted(flow_id_ticket.items()):
            obj = {}
            name = sample_id_to_ckan_name(
                "control", self.ckan_data_type + "-" + amplicon, flow_id
            ).lower()
            track_meta = self.track_meta.get(info["ticket"])

            def track_get(k, default=None):
                if track_meta is None:
                    return None
                return getattr(track_meta, k, default)

            archive_ingestion_date = ingest_utils.get_date_isoformat(
                self._logger, track_get("date_of_transfer_to_archive")
            )

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "flow_id": flow_id,
                    "title": "BASE Amplicons Control %s %s" % (amplicon, flow_id),
                    "read_length": base_amplicon_read_length(
                        amplicon
                    ),  # hard-coded for now, on advice of AB at CSIRO
                    "omics": "Genomics",
                    "analytical_platform": "MiSeq",
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger,
                        track_get("date_of_transfer", default=archive_ingestion_date),
                    ),
                    "data_type": track_get("data_type"),
                    "description": track_get("description"),
                    "folder_name": track_get("folder_name"),
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, track_get("date_of_transfer")
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, track_get("date_of_transfer_to_archive")
                    ),
                    "archive_ingestion_date": archive_ingestion_date,
                    "license_id": apply_license(archive_ingestion_date),
                    "dataset_url": track_get("download"),
                    "ticket": info["ticket"],
                    "facility": info["facility_code"].upper(),
                    "amplicon": amplicon,
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                }
            )
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "archive_ingestion_date",
                self.embargo_days,
                CONSORTIUM_ORG_NAME,
            )
            ingest_utils.apply_access_control(self._logger, self, obj)
            ingest_utils.add_spatial_extra(self._logger, obj)
            self.build_notes_into_object(obj)
            tag_names = ["amplicons-control", amplicon, "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            self.track_packages_for_md5(obj, info["ticket"])
            packages.append(obj)
        return packages

    def _get_resources(self):
        resources = []
        self._logger.info("Ingesting MD5 file information from {0}".format(self.path))
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    ((resource["amplicon"], resource["flow_id"]), legacy_url, resource)
                )
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


class BASEMetagenomicsMetadata(AMDFullIngestMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "base-metagenomics"
    omics = "metagenomics"
    sequence_data_type = "illumina-shortread"
    embargo_days = 90
    contextual_classes = common_context + ncbi_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/base/raw/metagenomics/",
    ]
    metadata_url_components = ("facility_code", "ticket")
    resource_linkage = ("sample_extraction_id", "flow_id")
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                re.compile(r".*[Ss]oil sample unique [Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_extraction_id",
                "Sample extraction ID",
                coerce=ingest_utils.fix_sample_extraction_id,
            ),
            fld("insert_size_range", "Insert size range", optional=True),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld("casava_version", "CASAVA version", optional=True),
            fld("flow_cell_id", "Run #:Flow Cell ID", optional=True),
            fld("sequencing_facility", "sequencing facility", optional=True),
            fld("target", "target", optional=True),
            fld("index", "index", optional=True),
            fld("library", "library", optional=True),
            fld("library_code", "library code", optional=True),
            fld(
                "library_construction_insert_size_bp",
                "library construction (insert size bp)",
                optional=True,
            ),
            fld(
                "library_construction_average_insert_size",
                "library construction - average insert size",
                optional=True,
            ),
            fld("run_number", "run number", optional=True),
            fld("run_flow_cell_id", "run #:flow cell id", optional=True),
            fld("lane_number", "lane number", optional=True),
        ],
        "options": {"header_length": 2, "column_name_row_index": 1,},
    }
    md5 = {
        "match": files.base_metagenomics_regexps,
        "skip": common_skip,
    }
    # these are packages from the pilot, which have missing metadata
    # we synthethise minimal packages for this data - see
    # https://github.com/BioplatformsAustralia/bpa-archive-ops/issues/140
    missing_packages = [
        ("8154_2", "H9BB6ADXX"),
        ("8158_2", "H9BB6ADXX"),
        ("8159_2", "H9BB6ADXX"),
        ("8262_2", "H9EV8ADXX"),
        ("8263_2", "H9BB6ADXX"),
        ("8268_2", "H80EYADXX"),
        ("8268_2", "H9EV8ADXX"),
        ("8269_2", "H80EYADXX"),
        ("8269_2", "H9EV8ADXX"),
        ("8270_2", "H80EYADXX"),
        ("8271_2", "H80EYADXX"),
        ("8271_2", "H9EV8ADXX"),
    ]

    bad_flow_ids = ["H8AABADXX"]

    missing_ingest_dates = [
        "BRLOPS-638",
        "BRLOPS-649",
        "BRLOPS-650",
        "BRLOPS-652",
        "BPAOPS-117",
        "BPAOPS-140",
    ]

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.contextual_metadata = kwargs["contextual_metadata"]
        self.track_meta = BASETrackMetadata()

    def assemble_obj(self, sample_id, sample_extraction_id, flow_id, row, track_meta):
        def track_get(k, default=None):
            if row.ticket.strip() in self.missing_ingest_dates and k.startswith(
                "date_of_transfer"
            ):
                return "2017-05-11"
            if track_meta is None:
                return None
            return getattr(track_meta, k, default)

        def row_get(k, v_fn=None):
            if row is None:
                return None
            res = getattr(row, k)
            if v_fn is not None:
                res = v_fn(res)
            return res

        name = sample_id_to_ckan_name(
            sample_extraction_id, self.ckan_data_type, flow_id
        )
        archive_ingestion_date = ingest_utils.get_date_isoformat(
            self._logger, track_get("date_of_transfer_to_archive")
        )

        obj = {
            "name": name,
            "sample_type": "Soil",
            "id": name,
            "sample_id": sample_id,
            "flow_id": flow_id,
            "read_length": "150bp",  # hard-coded for now, on advice of AB at CSIRO
            "sample_extraction_id": sample_extraction_id,
            "insert_size_range": row_get("insert_size_range"),
            "library_construction_protocol": row_get("library_construction_protocol"),
            "sequencer": row_get("sequencer"),
            "analysis_software_version": row_get("casava_version"),
            "title": "BASE Metagenomics %s" % (sample_extraction_id),
            "contextual_data_submission_date": None,
            "ticket": row_get("ticket"),
            "facility": row_get("facility_code", lambda v: v.upper()),
            "date_of_transfer": ingest_utils.get_date_isoformat(
                self._logger,
                track_get("date_of_transfer", default=archive_ingestion_date),
            ),
            "sample_submission_date": ingest_utils.get_date_isoformat(
                self._logger, track_get("date_of_transfer")
            ),
            "data_generated": ingest_utils.get_date_isoformat(
                self._logger, track_get("date_of_transfer_to_archive")
            ),
            "archive_ingestion_date": archive_ingestion_date,
            "license_id": apply_license(archive_ingestion_date),
            "data_type": track_get("data_type"),
            "description": track_get("description"),
            "folder_name": track_get("folder_name"),
            "dataset_url": track_get("download"),
            "type": self.ckan_data_type,
            "sequence_data_type": self.sequence_data_type,
        }
        ingest_utils.permissions_organization_member_after_embargo(
            self._logger,
            obj,
            "archive_ingestion_date",
            self.embargo_days,
            CONSORTIUM_ORG_NAME,
        )
        for contextual_source in self.contextual_metadata:
            obj.update(contextual_source.get(sample_id))
        ingest_utils.apply_access_control(self._logger, self, obj)
        ingest_utils.add_spatial_extra(self._logger, obj)
        self.build_notes_into_object(obj)
        tag_names = ["metagenomics", obj["sample_type"]]
        obj["tags"] = [{"name": t} for t in tag_names]
        return obj

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_([A-Z0-9]{9})_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                self._logger.warning(
                    "unable to find flowcell for filename: `%s'" % (fname)
                )
                return None
            return m.groups()[0]

        self._logger.info(
            "Ingesting BASE Metagenomics metadata from {0}".format(self.path)
        )
        packages = []

        class FakePilotRow:
            def __init__(self, xlsx_info):
                self.ticket = xlsx_info["ticket"]
                self.facility_code = xlsx_info["facility_code"]
                for attr in (
                    "insert_size_range",
                    "library_construction_protocol",
                    "sequencer",
                    "casava_version",
                ):
                    setattr(self, attr, None)

        class FakePilotTrackMeta:
            def __init__(self):
                # place-holder date, this is very early data
                self.archive_ingest_date = (
                    self.date_of_transfer_to_archive
                ) = self.date_of_transfer = "2015-01-01"
                for attr in ("data_type", "description", "folder_name", "download"):
                    setattr(self, attr, None)

        # missing metadata (see note above)
        for sample_extraction_id, flow_id in self.missing_packages:
            sample_id = ingest_utils.extract_ands_id(
                self._logger, sample_extraction_id.split("_")[0]
            )
            sample_extraction_id = ingest_utils.make_sample_extraction_id(
                sample_extraction_id, sample_id
            )
            md5_file = one(glob(self.path + "/*%s*.md5" % (flow_id)))
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            packages.append(
                self.assemble_obj(
                    sample_id,
                    sample_extraction_id,
                    flow_id,
                    FakePilotRow(xlsx_info),
                    FakePilotTrackMeta(),
                )
            )

        # the generated package IDs will have duplicates, due to data issues in the pilot data
        # we simply skip over the duplicates, which don't have any significant data differences
        generated_packages = set()
        for fname in glob(self.path + "/*.xlsx"):
            self._logger.info(
                "Processing BASE Metagenomics metadata file {0}".format(
                    os.path.basename(fname)
                )
            )
            # unique the rows, duplicates in some of the sheets
            uniq_rows = set(
                t for t in self.parse_spreadsheet(fname, self.metadata_info)
            )
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            for row in uniq_rows:
                track_meta = self.track_meta.get(row.ticket)
                if not track_meta:
                    self._logger.error(
                        "No tracking metadata for: {}".format(xlsx_info)
                    )
                # pilot data has the flow cell in the spreadsheet; in the main dataset
                # there is one flow-cell per spreadsheet, so it's in the spreadsheet
                # filename
                flow_id = row.flow_cell_id
                if flow_id is None:
                    flow_id = get_flow_id(fname)
                if flow_id is None:
                    raise Exception(
                        "can't determine flow_id for %s / %s" % (fname, repr(row))
                    )
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                sample_extraction_id = ingest_utils.make_sample_extraction_id(
                    row.sample_extraction_id, sample_id
                )
                new_obj = self.assemble_obj(
                    sample_id, sample_extraction_id, flow_id, row, track_meta
                )
                if new_obj["id"] in generated_packages:
                    self._logger.debug(
                        "skipped attempt to generate duplicate package: %s"
                        % new_obj["id"]
                    )
                    continue
                generated_packages.add(new_obj["id"])
                if not new_obj.get("flow_id") in self.bad_flow_ids:
                    self.track_packages_for_md5(new_obj, row.ticket)
                    packages.append(new_obj)

        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting BASE Metagenomics md5 file information from {0}".format(
                self.path
            )
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_extraction_id = (
                    sample_id.split("/")[-1] + "_" + file_info.get("extraction")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    ((sample_extraction_id, resource["flow_id"]), legacy_url, resource)
                )
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


class BASESiteImagesMetadata(AMDFullIngestMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "base-site-image"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$"]
    omics = None
    technology = "site-images"
    sequence_data_type = "image"
    embargo_days = 90
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/base/site-images/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("site_ids",)
    md5 = {
        "match": [files.base_site_image_filename_re],
        "skip": None,
    }

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.contextual_metadata = kwargs["contextual_metadata"]
        self.id_to_resources = self._read_md5s()

    def _read_md5s(self):
        id_to_resources = {}
        for fname in glob(self.path + "/*.md5"):
            self._logger.info("Processing MD5 file %s" % (fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            for filename, md5, file_info in self.parse_md5file(fname):
                id_tpl = (file_info["id1"], file_info["id2"])
                assert id_tpl not in id_to_resources
                obj = {"md5": md5, "filename": filename}
                obj.update(xlsx_info)
                id_to_resources[id_tpl] = obj
        return id_to_resources

    def id_tpl_to_site_ids(self, id_tpl):
        return ", ".join(
            [ingest_utils.extract_ands_id(self._logger, t) for t in id_tpl]
        )

    def _get_packages(self):
        self._logger.info(
            "Ingesting BPA BASE Images metadata from {0}".format(self.path)
        )
        packages = []
        for id_tpl in sorted(self.id_to_resources):
            info = self.id_to_resources[id_tpl]
            obj = {}
            name = sample_id_to_ckan_name("%s-%s" % id_tpl, self.ckan_data_type).lower()
            # find the common contextual metadata for the site IDs
            context = []
            for abbrev in id_tpl:
                fragment = {}
                for contextual_source in self.contextual_metadata:
                    fragment.update(
                        contextual_source.get(
                            ingest_utils.extract_ands_id(self._logger, abbrev)
                        )
                    )
                context.append(fragment)
            obj.update(common_values(context))
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "site_ids": self.id_tpl_to_site_ids(id_tpl),
                    "title": "BASE Site Image %s %s" % id_tpl,
                    "omics": "Genomics",
                    "analytical_platform": "MiSeq",
                    "ticket": info["ticket"],
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                }
            )

            # date if none provided
            obj.setdefault("date_of_transfer", "2017-04-28")

            ingest_utils.permissions_organization_member(self._logger, obj)
            self.build_notes_into_object(obj)
            ingest_utils.apply_access_control(self._logger, self, obj)
            ingest_utils.add_spatial_extra(self._logger, obj)
            tag_names = ["site-images"]
            obj["tags"] = [{"name": t} for t in tag_names]
            self.track_packages_for_md5(obj, info["ticket"])
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting Sepsis md5 file information from {0}".format(self.path)
        )
        resources = []
        for id_tpl in sorted(self.id_to_resources):
            site_ids = self.id_tpl_to_site_ids(id_tpl)
            info = self.id_to_resources[id_tpl]
            resource = {}
            resource["md5"] = resource["id"] = info["md5"]
            filename = info["filename"]
            resource["name"] = filename
            legacy_url = urljoin(info["base_url"], filename)
            resources.append(((site_ids,), legacy_url, resource))

        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing MD5 file %s for resources" % (md5_file))
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


marine_read_lengths = {
    "16S": "300bp",
    "A16S": "300bp",
    "18S": "250bp",
    "A16": "300bp",
    "ITS": "300bp",
}


def mm_amplicon_read_length(amplicon):
    return marine_read_lengths[amplicon]


index_from_comment_re = re.compile(r"([G|A|T|C|-]{6,}_[G|A|T|C|-]{6,})")
index_from_comment_pilot_re = re.compile(r"_([G|A|T|C|-]{6,})_")


def index_from_comment(attrs):
    # return the index from a comment (for linkage on pilot data)
    # 34865_1_18S_UNSW_ATCTCAGG_GTAAGGAG_AWMVL
    # 21644_16S_UNSW_GTCAATTGACCG_AFGB7
    # 21644_A16S_UNSW_CGGAGCCT_TCGACTAG_AG27L
    for attr in attrs:
        if not attr:
            continue
        m = index_from_comment_re.search(attr)
        if not m:
            m = index_from_comment_pilot_re.search(attr)
        if not m:
            continue
        return m.groups()[0]


def build_mm_amplicon_linkage(index_linkage, flow_id, index):
    # build linkage, `index_linkage` indicates whether we need
    # to include index in the linkage
    if index_linkage:
        # strip out _ and - as usage inconsistent in pilot data
        return flow_id + "_" + index.replace("-", "").replace("_", "")
    return flow_id


def unique_spreadsheets(fnames):
    # project manager is updating submission sheets to correct errors
    # we want to keep the originals in case of any problems, so override
    # original with the update
    update_files = [t for t in fnames if "_UPDATE" in t]
    skip_files = set()
    for fname in update_files:
        skip_files.add(fname.replace("_UPDATE", ""))
    return [t for t in fnames if t not in skip_files]


def base_extract_bpam_metadata(track_meta):
    fields = (
        "archive_ingestion_date",
        "contextual_data_submission_date",
        "data_generated",
        "sample_submission_date",
        "submitter",
        "work_order",
    )
    return dict((t, track_meta.get(t, "")) for t in fields)


class MarineMicrobesAmpliconsMetadata(AMDFullIngestMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "mm-genomics-amplicon"
    omics = "genomics"
    contextual_classes = common_context + ncbi_context
    sequence_data_type = "illumina-amplicons"
    embargo_days = 90
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata.*.*\.xlsx"]
    resource_linkage = ("sample_id", "mm_amplicon_linkage")
    amplicon_tracker = {
        "16s": "Amplicon16STrack",
        "a16s": "AmpliconA16STrack",
        "18s": "Amplicon18STrack",
    }
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                re.compile(r"^.*sample unique id$"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample_extraction_id", "Sample extraction ID", optional=True),
            fld("target", "Target"),
            fld(
                "dilution_used", "Dilution used", coerce=ingest_utils.fix_date_interval
            ),
            fld("reads", re.compile(r"^# of (raw )?reads$")),
            fld("analysis_software_version", "AnalysisSoftwareVersion"),
            fld("comments", re.compile(r"^comments"), find_all=True),
            fld(
                "sample_name_on_sample_sheet",
                "Sample name on sample sheet",
                optional=True,
            ),
            # special case: we merge these together (and throw a hard error if more than one has data for a given row)
            fld("pass_fail", re.compile(r".*[Pp]=[Pp](|ass) ?[,/] ?[Ff]=[Ff]ail")),
            fld("pass_fail_neat", "1:10 PCR, P=pass, F=fail", optional=True),
            fld("pass_fail_10", "1:100 PCR, P=pass, F=fail", optional=True),
            fld("pass_fail_100", "neat PCR, P=pass, F=fail", optional=True),
            fld("index", "index", optional=True),
        ],
        "options": {"header_length": 2, "column_name_row_index": 1,},
    }
    technology = "amplicons"
    index_linkage_spreadsheets = (
        "MM_Pilot_1_16S_UNSW_AFGB7_metadata.xlsx",
        "MM-Pilot_A16S_UNSW_AG27L_metadata_UPDATE.xlsx",
        "MM_Pilot_18S_UNSW_AGGNB_metadata.xlsx",
    )
    index_linkage_md5s = (
        "MM_1_16S_UNSW_AFGB7_checksums.md5",
        "MM_Pilot_A16S_UNSW_AG27L_checksums.md5",
        "MM_18S_UNSW_AGGNB_checksums.md5",
    )
    flowcell_comment_spreadsheets = (
        "MM_16S_preBPA2_UNSW_metadata.xlsx",
        "MM_A16S_preBPA2_UNSW_metadata.xlsx",
        "MM_18S_preBPA2_UNSW_metadata.xlsx",
    )
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/amplicons/"
    ]
    metadata_url_components = ("amplicon", "facility_code", "ticket")
    md5 = {
        "match": [files.mm_amplicon_filename_re],
        "skip": common_skip + [files.mm_amplicon_control_filename_re],
    }
    missing_resources = [("102.100.100/34937", "AUWLK"), ("102.100.100/37712", "BHHYV")]

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.google_track_meta = MarineMicrobesGoogleTrackMetadata()
        self.contextual_metadata = kwargs["contextual_metadata"]
        self.track_meta = {
            amplicon: MarineMicrobesTrackMetadata(self._logger, fname)
            for (amplicon, fname) in self.amplicon_tracker.items()
        }

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")
        flow_comment_re = re.compile(r"^\d{4,6}_(\w{5,})$")  # e.g. 34658_AYBH6

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        def get_flow_id_from_comment(comment):
            m = flow_comment_re.match(comment)
            if not m:
                raise Exception(
                    "unable to derive flowcell from comment: `%s'" % (comment)
                )
            return m.groups()[0]

        self._logger.info(
            "Ingesting Marine Microbes metadata from {0}".format(self.path)
        )
        packages = []
        for fname in unique_spreadsheets(glob(self.path + "/*.xlsx")):
            base_fname = os.path.basename(fname)
            self._logger.info(
                "Processing Marine Microbes metadata file {0}".format(
                    os.path.basename(fname)
                )
            )
            flow_id = get_flow_id(fname)
            # the pilot data needs increased linkage, due to multiple trials on the same BPA ID
            use_index_linkage = base_fname in self.index_linkage_spreadsheets
            # the GOSHIP data has flowcells in the comments field
            use_flowid_from_comment = base_fname in self.flowcell_comment_spreadsheets
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                if use_flowid_from_comment:
                    flow_id = get_flow_id_from_comment(row.comments)
                track_meta = self.track_meta[row.amplicon].get(sample_id)
                google_track_meta = self.google_track_meta.get(row.ticket)
                obj = base_extract_bpam_metadata(track_meta)
                index = index_from_comment(
                    [row.comments, row.sample_name_on_sample_sheet]
                )
                mm_amplicon_linkage = build_mm_amplicon_linkage(
                    use_index_linkage, flow_id, index
                )
                name = sample_id_to_ckan_name(
                    sample_id.split("/")[-1],
                    self.ckan_data_type + "-" + row.amplicon.lower(),
                    mm_amplicon_linkage,
                )
                archive_ingestion_date = ingest_utils.get_date_isoformat(
                    self._logger, google_track_meta.date_of_transfer_to_archive
                )

                amplicon = row.amplicon.upper()
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_id": sample_id,
                        "flow_id": flow_id,
                        "mm_amplicon_linkage": mm_amplicon_linkage,
                        "sample_extraction_id": ingest_utils.make_sample_extraction_id(
                            row.sample_extraction_id, sample_id
                        ),
                        "read_length": mm_amplicon_read_length(amplicon),
                        "target": row.target,
                        "pass_fail": ingest_utils.merge_pass_fail(row),
                        "dilution_used": row.dilution_used,
                        "reads": row.reads,
                        "analysis_software_version": row.analysis_software_version,
                        "amplicon": amplicon,
                        "title": "Marine Microbes Amplicons %s %s %s"
                        % (amplicon, sample_id, flow_id),
                        "omics": "Genomics",
                        "analytical_platform": "MiSeq",
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer
                        ),
                        "data_type": google_track_meta.data_type,
                        "description": google_track_meta.description,
                        "folder_name": google_track_meta.folder_name,
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer
                        ),
                        "data_generated": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer_to_archive
                        ),
                        "archive_ingestion_date": archive_ingestion_date,
                        "license_id": apply_license(archive_ingestion_date),
                        "dataset_url": google_track_meta.download,
                        "ticket": row.ticket,
                        "facility": row.facility_code.upper(),
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "comments": row.comments,
                    }
                )
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id))
                ingest_utils.add_spatial_extra(self._logger, obj)
                self.build_notes_into_object(obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                tag_names = ["amplicons", amplicon]
                if obj.get("sample_type"):
                    tag_names.append(obj["sample_type"])
                obj["tags"] = [{"name": t} for t in tag_names]
                package_link = tuple(obj[t] for t in self.resource_linkage)
                if package_link not in self.missing_resources:
                    self.track_packages_for_md5(obj, row.ticket)
                    packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting MM md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            use_index_linkage = os.path.basename(md5_file) in self.index_linkage_md5s
            self._logger.info(
                "Processing md5 file {} {}".format(md5_file, use_index_linkage)
            )
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (
                            sample_id,
                            build_mm_amplicon_linkage(
                                use_index_linkage,
                                resource["flow_id"],
                                resource["index"],
                            ),
                        ),
                        legacy_url,
                        resource,
                    )
                )
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


class MarineMicrobesAmpliconsControlMetadata(AMDFullIngestMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "mm-genomics-amplicon-control"
    omics = "genomics"
    technology = "amplicons-control"
    sequence_data_type = "illumina-amplicons"
    embargo_days = 90
    contextual_classes = []
    metadata_patterns = [r"^.*\.md5"]
    resource_linkage = ("amplicon", "flow_id")
    md5 = {
        "match": [files.mm_amplicon_control_filename_re],
        "skip": common_skip + [files.mm_amplicon_filename_re],
    }
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/amplicons/"
    ]
    metadata_url_components = ("amplicon", "facility_code", "ticket")

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.google_track_meta = MarineMicrobesGoogleTrackMetadata()

    def _get_packages(self):
        flow_id_info = {
            t["flow_id"]: self.metadata_info[os.path.basename(fname)]
            for _, _, fname, t in self.md5_lines()
        }
        packages = []
        for flow_id, info in sorted(flow_id_info.items()):
            obj = {}
            amplicon = info["amplicon"].upper()
            name = sample_id_to_ckan_name(
                "control", self.ckan_data_type + "-" + amplicon, flow_id
            ).lower()
            google_track_meta = self.google_track_meta.get(info["ticket"])

            archive_ingestion_date = ingest_utils.get_date_isoformat(
                self._logger, google_track_meta.date_of_transfer_to_archive
            )

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "flow_id": flow_id,
                    "title": "Marine Microbes Amplicons Control %s %s"
                    % (amplicon, flow_id),
                    "omics": "Genomics",
                    "analytical_platform": "MiSeq",
                    "read_length": mm_amplicon_read_length(amplicon),
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer
                    ),
                    "data_type": google_track_meta.data_type,
                    "description": google_track_meta.description,
                    "folder_name": google_track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer
                    ),
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": archive_ingestion_date,
                    "license_id": apply_license(archive_ingestion_date),
                    "dataset_url": google_track_meta.download,
                    "ticket": info["ticket"],
                    "facility": info["facility_code"].upper(),
                    "amplicon": amplicon,
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                }
            )
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "archive_ingestion_date",
                self.embargo_days,
                CONSORTIUM_ORG_NAME,
            )
            ingest_utils.add_spatial_extra(self._logger, obj)
            self.build_notes_into_object(obj)
            ingest_utils.apply_access_control(self._logger, self, obj)
            tag_names = ["amplicons-control", amplicon, "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            self.track_packages_for_md5(obj, info["ticket"])
            packages.append(obj)
        return packages

    def _get_resources(self):
        resources = []
        self._logger.info("Ingesting MD5 file information from {0}".format(self.path))
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                amplicon = xlsx_info["amplicon"].upper()
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                resource["amplicon"] = amplicon
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    ((amplicon, resource["flow_id"]), legacy_url, resource)
                )
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


class BaseMarineMicrobesMetadata(AMDFullIngestMetadata):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.google_track_meta = MarineMicrobesGoogleTrackMetadata()
        self.track_meta = MarineMicrobesTrackMetadata(
            self._logger, self.tracker_filename
        )


class MarineMicrobesMetagenomicsMetadata(BaseMarineMicrobesMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "mm-metagenomics"
    sequence_data_type = "illumina-shortread"
    embargo_days = 90
    omics = "metagenomics"
    contextual_classes = common_context + ncbi_context
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata.*\.xlsx"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/metagenomics/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    tracker_filename = "MetagenomicsTrack"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                re.compile(r"^.*sample unique id$"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample_extraction_id", "Sample extraction ID", optional=True),
            fld("insert_size_range", "Insert size range"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld(
                "analysis_software_version",
                ("casava version", "bcl2fastq2", re.compile(r"^software[ &]+version$")),
            ),
            fld("comments", "comments", optional=True),
        ],
        "options": {"header_length": 2, "column_name_row_index": 1,},
    }
    md5 = {
        "match": [
            files.mm_metagenomics_filename_re,
            files.mm_metagenomics_filename_v2_re,
        ],
        "skip": common_skip,
    }

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.contextual_metadata = kwargs["contextual_metadata"]

    def _get_packages(self):
        self._logger.info(
            "Ingesting Marine Microbes metadata from {0}".format(self.path)
        )
        packages = []
        for fname in unique_spreadsheets(glob(self.path + "/*.xlsx")):
            self._logger.info(
                "Processing Marine Microbes metadata file {0}".format(
                    os.path.basename(fname)
                )
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                track_meta = self.track_meta.get(sample_id)
                google_track_meta = self.google_track_meta.get(row.ticket)
                obj = base_extract_bpam_metadata(track_meta)
                name = sample_id_to_ckan_name(
                    sample_id.split("/")[-1], self.ckan_data_type
                )
                archive_ingestion_date = ingest_utils.get_date_isoformat(
                    self._logger, google_track_meta.date_of_transfer_to_archive
                )

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_id": sample_id,
                        "title": "Marine Microbes Metagenomics %s" % (sample_id),
                        "omics": "metagenomics",
                        "analytical_platform": "HiSeq",
                        "read_length": "250bp",
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer
                        ),
                        "data_type": google_track_meta.data_type,
                        "description": google_track_meta.description,
                        "folder_name": google_track_meta.folder_name,
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer
                        ),
                        "data_generated": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer_to_archive
                        ),
                        "archive_ingestion_date": archive_ingestion_date,
                        "license_id": apply_license(archive_ingestion_date),
                        "dataset_url": google_track_meta.download,
                        "sample_extraction_id": ingest_utils.make_sample_extraction_id(
                            row.sample_extraction_id, sample_id
                        ),
                        "insert_size_range": row.insert_size_range,
                        "library_construction_protocol": row.library_construction_protocol,
                        "sequencer": row.sequencer,
                        "analysis_software_version": row.analysis_software_version,
                        "ticket": row.ticket,
                        "facility": row.facility_code.upper(),
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                    }
                )
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id))
                ingest_utils.add_spatial_extra(self._logger, obj)
                self.build_notes_into_object(obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                tag_names = ["metagenomics", "raw"]
                if obj.get("sample_type"):
                    tag_names.append(obj["sample_type"])
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_packages_for_md5(obj, row.ticket)
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting MM md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((sample_id,), legacy_url, resource))
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


class MarineMicrobesMetatranscriptomeMetadata(BaseMarineMicrobesMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "mm-metatranscriptome"
    sequence_data_type = "illumina-transcriptomics"
    embargo_days = 90
    contextual_classes = common_context + ncbi_context
    omics = "metatranscriptomics"
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata.*\.xlsx"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/metatranscriptome/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    tracker_filename = "MetatranscriptomeTrack"
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                re.compile(r"^.*sample unique id$"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample_extraction_id", "Sample extraction ID", optional=True),
            fld("insert_size_range", "Insert size range"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld(
                "analysis_software_version",
                ("casava version", "bcl2fastq2", re.compile(r"^software[ &]+version$")),
            ),
        ],
        "options": {"header_length": 2, "column_name_row_index": 1,},
    }
    md5 = {
        "match": [
            files.mm_metatranscriptome_filename_re,
            files.mm_metatranscriptome_filename2_re,
        ],
        "skip": None,
    }

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.contextual_metadata = kwargs["contextual_metadata"]

    def _get_packages(self):
        self._logger.info(
            "Ingesting Marine Microbes Transcriptomics metadata from {0}".format(
                self.path
            )
        )
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and sample_id is the primary key
        all_rows = set()
        for fname in unique_spreadsheets(glob(self.path + "/*.xlsx")):
            self._logger.info(
                "Processing Marine Microbes Transcriptomics metadata file {0}".format(
                    os.path.basename(fname)
                )
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                all_rows.add(row)
        for row in all_rows:
            sample_id = row.sample_id
            if sample_id is None:
                continue
            track_meta = self.track_meta.get(sample_id)
            google_track_meta = self.google_track_meta.get(row.ticket)
            obj = base_extract_bpam_metadata(track_meta)
            name = sample_id_to_ckan_name(sample_id.split("/")[-1], self.ckan_data_type)
            archive_ingestion_date = ingest_utils.get_date_isoformat(
                self._logger, google_track_meta.date_of_transfer_to_archive
            )

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "sample_id": sample_id,
                    "title": "Marine Microbes Metatranscriptome %s" % (sample_id),
                    "omics": "metatranscriptomics",
                    "analytical_platform": "HiSeq",
                    "read_length": "250bp",  # to be confirmed by Jason Koval
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer
                    ),
                    "data_type": google_track_meta.data_type,
                    "description": google_track_meta.description,
                    "folder_name": google_track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer
                    ),
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": archive_ingestion_date,
                    "license_id": apply_license(archive_ingestion_date),
                    "dataset_url": google_track_meta.download,
                    "sample_extraction_id": ingest_utils.make_sample_extraction_id(
                        row.sample_extraction_id, sample_id
                    ),
                    "insert_size_range": row.insert_size_range,
                    "library_construction_protocol": row.library_construction_protocol,
                    "sequencer": row.sequencer,
                    "analysis_software_version": row.analysis_software_version,
                    "ticket": row.ticket,
                    "facility": row.facility_code.upper(),
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                }
            )
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "archive_ingestion_date",
                self.embargo_days,
                CONSORTIUM_ORG_NAME,
            )
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(sample_id))
            ingest_utils.add_spatial_extra(self._logger, obj)
            self.build_notes_into_object(obj)
            ingest_utils.apply_access_control(self._logger, self, obj)
            tag_names = ["metatranscriptome", "raw"]
            if obj.get("sample_type"):
                tag_names.append(obj["sample_type"])
            obj["tags"] = [{"name": t} for t in tag_names]
            self.track_packages_for_md5(obj, row.ticket)
            packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting MM md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((sample_id,), legacy_url, resource))
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


class AustralianMicrobiomeMetagenomicsNovaseqMetadata(AMDFullIngestMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "amdb-metagenomics-novaseq"
    omics = "metagenomics"
    technology = "novaseq"
    sequence_data_type = "illumina-shortread"
    embargo_days = 90
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5", r"^.*_metadata.*\.xlsx"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/amd/metagenomics-novaseq/"
    ]
    metadata_url_components = ("facility_code", "ticket")
    resource_linkage = ("sample_id", "flowcell")
    spreadsheet = {
        "fields": [
            fld(
                "sample_id",
                re.compile(r"sample_?[Ii][Dd]"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("insert_size_range", re.compile(r"insert[ _]size[ _]range")),
            fld(
                "library_construction_protocol",
                re.compile(r"library[ _]construction[ _]protocol"),
            ),
            fld("sequencer", "sequencer"),
            fld("conversion_software", re.compile(r"conversion[ _]software")),
            fld(
                "conversion_software_version",
                re.compile(r"conversion[ _]software[ _]version"),
            ),
            fld(
                "number_of_raw_reads",
                re.compile(r"(number_of_raw_reads|# of raw reads)"),
            ),
            fld("number_pcr_cycle", "number_pcr_cycle", optional=True),
            fld("dna_concentration_method", "dna_concentration_method", optional=True),
            fld("dna_concentration", "dna_concentration", optional=True),
            fld("absorbance_260_280_ratio", "260_280_ratio", optional=True),
            fld("absorbance_260_230_ratio", "260_230_ratio", optional=True),
        ],
        "options": {"header_length": 1, "column_name_row_index": 0,},
    }
    md5 = {
        "match": [files.amd_metagenomics_novaseq_re,],
        "skip": [files.amd_metagenomics_novaseq_control_re,],
    }

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.contextual_metadata = kwargs["contextual_metadata"]
        self.google_track_meta = AustralianMicrobiomeGoogleTrackMetadata()

    def _get_packages(self):
        packages = []
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        for fname in unique_spreadsheets(glob(self.path + "/*.xlsx")):
            self._logger.info(
                "Processing Australian Microbiome metadata file {0}".format(
                    os.path.basename(fname)
                )
            )
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                flow_id = get_flow_id(fname)
                google_track_meta = self.google_track_meta.get(row.ticket)
                name = sample_id_to_ckan_name(
                    sample_id.split("/")[-1], self.ckan_data_type, flow_id
                )
                archive_ingestion_date = ingest_utils.get_date_isoformat(
                    self._logger, google_track_meta.date_of_transfer_to_archive
                )

                obj = row._asdict()

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "title": "Australian Microbiome Metagenomics Novaseq %s"
                        % (sample_id),
                        "omics": "metagenomics",
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer
                        ),
                        "data_type": google_track_meta.data_type,
                        "description": google_track_meta.description,
                        "folder_name": google_track_meta.folder_name,
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer
                        ),
                        "data_generated": ingest_utils.get_date_isoformat(
                            self._logger, google_track_meta.date_of_transfer_to_archive
                        ),
                        "archive_ingestion_date": archive_ingestion_date,
                        "license_id": apply_license(archive_ingestion_date),
                        "dataset_url": google_track_meta.download,
                        "type": self.ckan_data_type,
                        "sequence_data_type": self.sequence_data_type,
                        "flowcell": flow_id,
                    }
                )
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "archive_ingestion_date",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id))
                ingest_utils.add_spatial_extra(self._logger, obj)
                self.build_notes_into_object(obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                tag_names = ["metagenomics", "raw"]
                if obj.get("sample_type"):
                    tag_names.append(obj["sample_type"])
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_packages_for_md5(obj, row.ticket)
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting AM MGE md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = os.path.basename(filename)
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                flowcell = file_info.get("flowcell")
                resources.append(((sample_id, flowcell), legacy_url, resource))
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


class AustralianMicrobiomeMetagenomicsNovaseqControlMetadata(AMDFullIngestMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "amdb-metagenomics-novaseq-control"
    omics = "metagenomics"
    technology = "novaseq-control"
    sequence_data_type = "illumina-shortread"
    embargo_days = 90
    contextual_classes = []
    metadata_patterns = [r"^.*\.md5"]
    resource_linkage = ("flowcell",)
    md5 = {
        "match": [files.amd_metagenomics_novaseq_control_re],
        "skip": [files.amd_metagenomics_novaseq_re],
    }
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/amd/metagenomics-novaseq/"
    ]
    metadata_url_components = ("facility_code", "ticket")

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.google_track_meta = AustralianMicrobiomeGoogleTrackMetadata()

    def _get_packages(self):
        flowcell_info = {
            t["flowcell"]: self.metadata_info[os.path.basename(fname)]
            for _, _, fname, t in self.md5_lines()
        }
        packages = []
        for flowcell, info in sorted(flowcell_info.items()):
            obj = {}
            name = sample_id_to_ckan_name(
                "control", self.ckan_data_type, flowcell
            ).lower()
            google_track_meta = self.google_track_meta.get(info["ticket"])

            archive_ingestion_date = ingest_utils.get_date_isoformat(
                self._logger, google_track_meta.date_of_transfer_to_archive
            )

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "flowcell": flowcell,
                    "title": "Australian Microbiome Novaseq Control %s" % (flowcell),
                    "omics": "Genomics",
                    "analytical_platform": "Novaseq",
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer
                    ),
                    "data_type": google_track_meta.data_type,
                    "description": google_track_meta.description,
                    "folder_name": google_track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer
                    ),
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": archive_ingestion_date,
                    "license_id": apply_license(archive_ingestion_date),
                    "dataset_url": google_track_meta.download,
                    "ticket": info["ticket"],
                    "facility": info["facility_code"].upper(),
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                }
            )
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "archive_ingestion_date",
                self.embargo_days,
                CONSORTIUM_ORG_NAME,
            )
            ingest_utils.add_spatial_extra(self._logger, obj)
            self.build_notes_into_object(obj)
            ingest_utils.apply_access_control(self._logger, self, obj)
            tag_names = ["novaseq-control", "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            self.track_packages_for_md5(obj, info["ticket"])
            packages.append(obj)
        return packages

    def _get_resources(self):
        resources = []
        self._logger.info("Ingesting MD5 file information from {0}".format(self.path))
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = os.path.basename(filename)
                resource["resource_type"] = self.ckan_data_type
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((resource["flowcell"],), legacy_url, resource))
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


class AustralianMicrobiomeAmpliconsMetadata(AMDFullIngestMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "amdb-genomics-amplicon"
    omics = "genomics"
    sequence_data_type = "illumina-amplicons"
    embargo_days = 90
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx"]
    resource_linkage = ("sample_id", "flow_id", "index")
    spreadsheet = {
        "fields": [
            fld("sample_id", "sampleid", coerce=ingest_utils.extract_ands_id),
            fld("target", "target"),
            fld("pass_fail", "p=pass / f=fail"),
            fld(
                "dilution_used", "dilution used", coerce=ingest_utils.fix_date_interval
            ),
            fld("reads", "# of reads"),
            fld("analysis_software", "analysissoftware"),
            fld("analysis_software_version", "analysissoftwareversion"),
            fld("comments", "comments"),
            fld("index", "index"),
            fld("pcr_plate_name", "pcr plate name", optional=True),
        ],
        "options": {"header_length": 1, "column_name_row_index": 0,},
    }
    technology = "amplicons"
    metadata_urls = ["https://downloads-qcif.bioplatforms.com/bpa/amd/amplicons-miseq/"]
    metadata_url_components = ("amplicon", "ticket")
    md5 = {
        "match": [files.amd_amplicon_filename_re],
        "skip": [files.amd_amplicon_control_filename_re],
    }

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.google_track_meta = AustralianMicrobiomeGoogleTrackMetadata()
        self.contextual_metadata = kwargs["contextual_metadata"]

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")
        flow_comment_re = re.compile(r"^\d{4,6}_(\w{5,})$")  # e.g. 34658_AYBH6

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        def get_flow_id_from_comment(comment):
            m = flow_comment_re.match(comment)
            if not m:
                raise Exception(
                    "unable to derive flowcell from comment: `%s'" % (comment)
                )
            return m.groups()[0]

        self._logger.info(
            "Ingesting Australian Microbiome metadata from {0}".format(self.path)
        )
        packages = []
        for fname in unique_spreadsheets(glob(self.path + "/*.xlsx")):
            base_fname = os.path.basename(fname)
            self._logger.info(
                "Processing Australian Microbiome metadata file {0}".format(
                    os.path.basename(fname)
                )
            )
            flow_id = get_flow_id(fname)
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                obj = row._asdict()
                obj["flow_id"] = flow_id
                google_track_meta = self.google_track_meta.get(row.ticket)
                if google_track_meta is None:
                    self._logger.warning(
                        "no google tracking metadata for ticket {}".format(row.ticket)
                    )
                    archive_transfer_date = None
                    archive_ingestion_date = None
                else:
                    obj.update(google_track_meta._asdict())
                    archive_transfer_date = ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer
                    )
                    archive_ingestion_date = ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer_to_archive
                    )
                name = sample_id_to_ckan_name(
                    sample_id.split("/")[-1],
                    self.ckan_data_type + "-" + row.amplicon.lower(),
                    "{}_{}".format(obj["flow_id"], obj["index"]),
                )
                amplicon = row.amplicon.upper()
                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "amplicon": amplicon,
                        "title": "Australian Microbiome Amplicons %s %s %s"
                        % (amplicon, sample_id, flow_id),
                        "omics": "Genomics",
                        "analytical_platform": "MiSeq",
                        "license_id": apply_license(archive_ingestion_date),
                        "ticket": row.ticket,
                        "type": self.ckan_data_type,
                        "date_of_transfer": archive_transfer_date,
                        "date_of_transfer_to_archive": archive_ingestion_date,
                        "sequence_data_type": self.sequence_data_type,
                    }
                )
                ingest_utils.permissions_organization_member_after_embargo(
                    self._logger,
                    obj,
                    "date_of_transfer_to_archive",
                    self.embargo_days,
                    CONSORTIUM_ORG_NAME,
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id))
                ingest_utils.add_spatial_extra(self._logger, obj)
                self.build_notes_into_object(obj)
                ingest_utils.apply_access_control(self._logger, self, obj)
                tag_names = ["amplicons", amplicon]
                if obj.get("sample_type"):
                    tag_names.append(obj["sample_type"])
                obj["tags"] = [{"name": t} for t in tag_names]
                self.track_packages_for_md5(obj, row.ticket)
                packages.append(obj)
        return packages

    def _get_resources(self):
        self._logger.info(
            "Ingesting MM md5 file information from {0}".format(self.path)
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_id = ingest_utils.extract_ands_id(
                    self._logger, file_info.get("id")
                )
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    (
                        (sample_id, resource["flow_id"], resource["index"],),
                        legacy_url,
                        resource,
                    )
                )
            resources.extend(self.generate_md5_resources(md5_file))
        return resources


class AustralianMicrobiomeAmpliconsControlMetadata(AMDFullIngestMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "amdb-genomics-amplicon-control"
    omics = "genomics"
    technology = "amplicons-control"
    sequence_data_type = "illumina-amplicons"
    embargo_days = 90
    contextual_classes = []
    metadata_patterns = [r"^.*\.md5"]
    resource_linkage = ("amplicon", "flow_id")
    md5 = {
        "match": [files.amd_amplicon_control_filename_re],
        "skip": [files.amd_amplicon_filename_re],
    }
    metadata_urls = ["https://downloads-qcif.bioplatforms.com/bpa/amd/amplicons-miseq/"]
    metadata_url_components = ("amplicon", "ticket")

    def __init__(self, logger, metadata_path, **kwargs):
        super().__init__(logger, metadata_path, **kwargs)
        self.google_track_meta = AustralianMicrobiomeGoogleTrackMetadata()

    def _get_packages(self):
        flow_id_info = {
            t["flow_id"]: self.metadata_info[os.path.basename(fname)]
            for _, _, fname, t in self.md5_lines()
        }
        packages = []
        for flow_id, info in sorted(flow_id_info.items()):
            obj = {}
            amplicon = info["amplicon"].upper()
            name = sample_id_to_ckan_name(
                "control", self.ckan_data_type + "-" + amplicon, flow_id
            ).lower()
            google_track_meta = self.google_track_meta.get(info["ticket"])

            archive_ingestion_date = ingest_utils.get_date_isoformat(
                self._logger, google_track_meta.date_of_transfer_to_archive
            )

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "flow_id": flow_id,
                    "title": "Australian Microbiome Amplicons Control %s %s"
                    % (amplicon, flow_id),
                    "omics": "Genomics",
                    "analytical_platform": "MiSeq",
                    "read_length": mm_amplicon_read_length(amplicon),
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer
                    ),
                    "data_type": google_track_meta.data_type,
                    "description": google_track_meta.description,
                    "folder_name": google_track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer
                    ),
                    "data_generated": ingest_utils.get_date_isoformat(
                        self._logger, google_track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": archive_ingestion_date,
                    "license_id": apply_license(archive_ingestion_date),
                    "dataset_url": google_track_meta.download,
                    "ticket": info["ticket"],
                    "amplicon": amplicon,
                    "type": self.ckan_data_type,
                    "sequence_data_type": self.sequence_data_type,
                }
            )
            ingest_utils.permissions_organization_member_after_embargo(
                self._logger,
                obj,
                "archive_ingestion_date",
                self.embargo_days,
                CONSORTIUM_ORG_NAME,
            )
            ingest_utils.add_spatial_extra(self._logger, obj)
            self.build_notes_into_object(obj)
            ingest_utils.apply_access_control(self._logger, self, obj)
            tag_names = ["amplicons-control", amplicon, "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            self.track_packages_for_md5(obj, info["ticket"])
            packages.append(obj)
        return packages

    def _get_resources(self):
        resources = []
        self._logger.info("Ingesting MD5 file information from {0}".format(self.path))
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                amplicon = xlsx_info["amplicon"].upper()
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                resource["amplicon"] = amplicon
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(
                    ((amplicon, resource["flow_id"]), legacy_url, resource)
                )
            resources.extend(self.generate_md5_resources(md5_file))
        return resources
