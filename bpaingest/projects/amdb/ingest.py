import os
import re
from glob import glob
from urllib.parse import urljoin

from unipath import Path

from . import files
from ...abstract import BaseMetadata
from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from ...util import (
    apply_license,
    sample_id_to_ckan_name,
    common_values,
    make_logger,
    one,
)
from .contextual import (
    AustralianMicrobiomeSampleContextual,
    BASENCBIContextual,
    MarineMicrobesNCBIContextual,
)
from .tracking import (
    BASETrackMetadata,
    MarineMicrobesGoogleTrackMetadata,
    MarineMicrobesTrackMetadata,
)

logger = make_logger(__name__)

common_context = [
    AustralianMicrobiomeSampleContextual,
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
    re.compile(r"^.*_metadata\.xlsx$"),
    re.compile(r"^.*SampleSheet.*"),
    re.compile(r"^.*TestFiles\.exe.*"),
]


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


class AccessAMDContextualMetadata(AMDBaseMetadata):
    """
    for use by tools (e.g. bpaotu) which need access to the contextual metadata for all
    AMD data, but not package or resource metadata
    """

    contextual_classes = common_context
    metadata_urls = []

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super().__init__()
        self.contextual_metadata = contextual_metadata

    def _get_packages(self):
        return []

    def _get_resources(self):
        return []


class BASEAmpliconsMetadata(AMDBaseMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "base-genomics-amplicon"
    omics = "genomics"
    technology = "amplicons"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$", r"^.*_metadata.*.*\.xlsx$"]
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/base/raw/amplicons/",
    ]
    metadata_url_components = ("amplicon", "facility_code", "ticket")
    resource_linkage = ("sample_extraction_id", "amplicon", "base_amplicon_linkage")
    # pilot data
    index_linkage_spreadsheets = ("BASE_18S_UNSW_A6BRJ_metadata.xlsx",)
    index_linkage_md5s = ("BASE_18S_UNSW_A6BRJ_checksums.md5",)
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
                re.compile(r".*sample unique id"),
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_extraction_id",
                "Sample extraction ID",
                coerce=ingest_utils.fix_sample_extraction_id,
            ),
            fld("sequencing_facility", "Sequencing facility"),
            fld("target", "Target", coerce=lambda s: s.upper().strip()),
            fld("index", "Index", coerce=lambda s: s[:12]),
            fld("index1", "Index 1", coerce=lambda s: s[:12]),
            fld("index2", "Index2", coerce=lambda s: s[:12]),
            fld("pcr_1_to_10", "1:10 PCR, P=pass, F=fail", coerce=ingest_utils.fix_pcr),
            fld(
                "pcr_1_to_100", "1:100 PCR, P=pass, F=fail", coerce=ingest_utils.fix_pcr
            ),
            fld("pcr_neat", "neat PCR, P=pass, F=fail", coerce=ingest_utils.fix_pcr),
            fld("dilution", "Dilution used", coerce=ingest_utils.fix_date_interval),
            fld("sequencing_run_number", "Sequencing run number"),
            fld("flow_cell_id", "Flowcell"),
            fld("reads", ("# of RAW reads", "# of reads"), coerce=ingest_utils.get_int),
            fld("sample_name", "Sample name on sample sheet"),
            fld("analysis_software_version", "AnalysisSoftwareVersion"),
            fld("comments", "Comments"),
        ],
        "options": {"header_length": 2, "column_name_row_index": 1,},
    }
    md5 = {
        "match": files.base_amplicon_regexps,
        "skip": common_skip + files.base_amplicon_control_regexps,
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(BASEAmpliconsMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = BASETrackMetadata()

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_(\w+)_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        logger.info("Ingesting BASE Amplicon metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + "/*.xlsx"):
            logger.info(
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
                obj = {}
                amplicon = row.amplicon.upper()
                name = sample_id_to_ckan_name(
                    sample_extraction_id,
                    self.ckan_data_type + "-" + amplicon,
                    base_amplicon_linkage,
                )
                archive_ingestion_date = ingest_utils.get_date_isoformat(
                    track_get("date_of_transfer_to_archive")
                )

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_type": "soil",
                        "read_length": base_amplicon_read_length(
                            amplicon
                        ),  # hard-coded for now, on advice of AB at CSIRO
                        "sample_id": sample_id,
                        "flow_id": flow_id,
                        "base_amplicon_linkage": base_amplicon_linkage,
                        "sample_extraction_id": sample_extraction_id,
                        "target": row.target,
                        "index": row.index,
                        "index1": row.index1,
                        "index2": row.index2,
                        "pcr_1_to_10": row.pcr_1_to_10,
                        "pcr_1_to_100": row.pcr_1_to_100,
                        "pcr_neat": row.pcr_neat,
                        "dilution": row.dilution,
                        "sequencing_run_number": row.sequencing_run_number,
                        "flow_cell_id": row.flow_cell_id,
                        "reads": row.reads,
                        "sample_name": row.sample_name,
                        "analysis_software_version": row.analysis_software_version,
                        "amplicon": amplicon,
                        "notes": "BASE Amplicons %s %s %s"
                        % (amplicon, sample_extraction_id, note_extra),
                        "title": "BASE Amplicons %s %s %s"
                        % (amplicon, sample_extraction_id, note_extra),
                        "contextual_data_submission_date": None,
                        "ticket": row.ticket,
                        "facility": row.facility_code.upper(),
                        "type": self.ckan_data_type,
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            track_get("date_of_transfer")
                        ),
                        "data_type": track_get("data_type"),
                        "description": track_get("description"),
                        "folder_name": track_get("folder_name"),
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            track_get("date_of_transfer")
                        ),
                        "data_generated": ingest_utils.get_date_isoformat(
                            track_get("date_of_transfer_to_archive")
                        ),
                        "archive_ingestion_date": archive_ingestion_date,
                        "license_id": apply_license(archive_ingestion_date),
                        "dataset_url": track_get("download"),
                        "comments": row.comments,
                        "private": True,
                    }
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id))
                ingest_utils.add_spatial_extra(obj)
                tag_names = ["amplicons", amplicon, obj["sample_type"]]
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info(
            "Ingesting BASE Amplicon md5 file information from {0}".format(self.path)
        )
        resources = []

        for md5_file in glob(self.path + "/*.md5"):
            index_linkage = os.path.basename(md5_file) in self.index_linkage_md5s
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                sample_id = ingest_utils.extract_ands_id(file_info.get("id"))
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
        return resources


class BASEAmpliconsControlMetadata(AMDBaseMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "base-genomics-amplicon-control"
    omics = "genomics"
    technology = "amplicons-control"
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

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(BASEAmpliconsControlMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info
        self.contextual_metadata = contextual_metadata
        self.track_meta = BASETrackMetadata()

    def md5_lines(self):
        for md5_file in glob(self.path + "/*.md5"):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                yield filename, md5, md5_file, file_info

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

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            archive_ingestion_date = ingest_utils.get_date_isoformat(
                track_get("date_of_transfer_to_archive")
            )

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "flow_id": flow_id,
                    "notes": "BASE Amplicons Control %s %s" % (amplicon, flow_id),
                    "title": "BASE Amplicons Control %s %s" % (amplicon, flow_id),
                    "read_length": base_amplicon_read_length(
                        amplicon
                    ),  # hard-coded for now, on advice of AB at CSIRO
                    "omics": "Genomics",
                    "analytical_platform": "MiSeq",
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        track_get("date_of_transfer")
                    ),
                    "data_type": track_get("data_type"),
                    "description": track_get("description"),
                    "folder_name": track_get("folder_name"),
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        track_get("date_of_transfer")
                    ),
                    "contextual_data_submission_date": None,
                    "data_generated": ingest_utils.get_date_isoformat(
                        track_get("date_of_transfer_to_archive")
                    ),
                    "archive_ingestion_date": archive_ingestion_date,
                    "license_id": apply_license(archive_ingestion_date),
                    "dataset_url": track_get("download"),
                    "ticket": info["ticket"],
                    "facility": info["facility_code"].upper(),
                    "amplicon": amplicon,
                    "type": self.ckan_data_type,
                    "private": True,
                }
            )
            ingest_utils.add_spatial_extra(obj)
            tag_names = ["amplicons-control", amplicon, "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
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
        return resources


class BASEMetagenomicsMetadata(AMDBaseMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "base-metagenomics"
    omics = "metagenomics"
    contextual_classes = common_context
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
                "Soil sample unique ID",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld(
                "sample_extraction_id",
                "Sample extraction ID",
                coerce=ingest_utils.fix_sample_extraction_id,
            ),
            fld("insert_size_range", "Insert size range"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld("casava_version", "CASAVA version"),
            fld("flow_cell_id", "Run #:Flow Cell ID"),
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

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(BASEMetagenomicsMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = BASETrackMetadata()

    def assemble_obj(self, sample_id, sample_extraction_id, flow_id, row, track_meta):
        def track_get(k):
            if track_meta is None:
                return None
            return getattr(track_meta, k)

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
            track_get("date_of_transfer_to_archive")
        )

        obj = {
            "name": name,
            "sample_type": "soil",
            "id": name,
            "sample_id": sample_id,
            "flow_id": flow_id,
            "read_length": "150bp",  # hard-coded for now, on advice of AB at CSIRO
            "sample_extraction_id": sample_extraction_id,
            "insert_size_range": row_get("insert_size_range"),
            "library_construction_protocol": row_get("library_construction_protocol"),
            "sequencer": row_get("sequencer"),
            "analysis_software_version": row_get("casava_version"),
            "notes": "BASE Metagenomics %s" % (sample_extraction_id),
            "title": "BASE Metagenomics %s" % (sample_extraction_id),
            "contextual_data_submission_date": None,
            "ticket": row_get("ticket"),
            "facility": row_get("facility_code", lambda v: v.upper()),
            "date_of_transfer": ingest_utils.get_date_isoformat(
                track_get("date_of_transfer")
            ),
            "sample_submission_date": ingest_utils.get_date_isoformat(
                track_get("date_of_transfer")
            ),
            "data_generated": ingest_utils.get_date_isoformat(
                track_get("date_of_transfer_to_archive")
            ),
            "archive_ingestion_date": archive_ingestion_date,
            "license_id": apply_license(archive_ingestion_date),
            "data_type": track_get("data_type"),
            "description": track_get("description"),
            "folder_name": track_get("folder_name"),
            "dataset_url": track_get("download"),
            "type": self.ckan_data_type,
            "private": True,
        }
        for contextual_source in self.contextual_metadata:
            obj.update(contextual_source.get(sample_id))
        ingest_utils.add_spatial_extra(obj)
        tag_names = ["metagenomics", obj["sample_type"]]
        obj["tags"] = [{"name": t} for t in tag_names]
        return obj

    def _get_packages(self):
        xlsx_re = re.compile(r"^.*_([A-Z0-9]{9})_metadata.*\.xlsx$")

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                logger.warning("unable to find flowcell for filename: `%s'" % (fname))
                return None
            return m.groups()[0]

        logger.info("Ingesting BASE Metagenomics metadata from {0}".format(self.path))
        packages = []

        class FakePilotRow(object):
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

        class FakePilotTrackMeta(object):
            def __init__(self):
                # place-holder date, this is very early data
                self.archive_ingest_date = (
                    self.date_of_transfer_to_archive
                ) = self.date_of_transfer = "2015-01-01"
                for attr in ("data_type", "description", "folder_name", "download"):
                    setattr(self, attr, None)

        # missing metadata (see note above)
        for sample_extraction_id, flow_id in self.missing_packages:
            sample_id = ingest_utils.extract_ands_id(sample_extraction_id.split("_")[0])
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
            logger.info(
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
                    logger.critical("OOPS: {}".format(xlsx_info))
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
                    logger.debug(
                        "skipped attempt to generate duplicate package: %s"
                        % new_obj["id"]
                    )
                    continue
                generated_packages.add(new_obj["id"])
                packages.append(new_obj)

        return packages

    def _get_resources(self):
        logger.info(
            "Ingesting BASE Metagenomics md5 file information from {0}".format(
                self.path
            )
        )
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                sample_id = ingest_utils.extract_ands_id(file_info.get("id"))
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
        return resources


class BASESiteImagesMetadata(AMDBaseMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "base-site-image"
    contextual_classes = common_context
    metadata_patterns = [r"^.*\.md5$"]
    omics = None
    technology = "site-images"
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/base/site-images/",
    ]
    metadata_url_components = ("ticket",)
    resource_linkage = ("site_ids",)
    md5 = {
        "match": [files.base_site_image_filename_re],
        "skip": None,
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(BASESiteImagesMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.id_to_resources = self._read_md5s()

    def _read_md5s(self):
        id_to_resources = {}
        for fname in glob(self.path + "/*.md5"):
            logger.info("Processing MD5 file %s" % (fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            for filename, md5, file_info in self.parse_md5file(fname):
                id_tpl = (file_info["id1"], file_info["id2"])
                assert id_tpl not in id_to_resources
                obj = {"md5": md5, "filename": filename}
                obj.update(xlsx_info)
                id_to_resources[id_tpl] = obj
        return id_to_resources

    @classmethod
    def id_tpl_to_site_ids(cls, id_tpl):
        return ", ".join([ingest_utils.extract_ands_id(t) for t in id_tpl])

    def _get_packages(self):
        logger.info("Ingesting BPA BASE Images metadata from {0}".format(self.path))
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
                        contextual_source.get(ingest_utils.extract_ands_id(abbrev))
                    )
                context.append(fragment)
            obj.update(common_values(context))
            obj.update(
                {
                    "name": name,
                    "id": name,
                    "site_ids": self.id_tpl_to_site_ids(id_tpl),
                    "title": "BASE Site Image %s %s" % id_tpl,
                    "notes": "Site image: %s" % (obj["location_description"]),
                    "omics": "Genomics",
                    "analytical_platform": "MiSeq",
                    "ticket": info["ticket"],
                    "type": self.ckan_data_type,
                    "private": True,
                }
            )
            ingest_utils.add_spatial_extra(obj)
            tag_names = ["site-images"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
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
        return resources


marine_read_lengths = {"16s": "300bp", "a16s": "300bp", "18s": "250bp"}


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


class MarineMicrobesAmpliconsMetadata(AMDBaseMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "mm-genomics-amplicon"
    omics = "genomics"
    contextual_classes = common_context
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
            fld("sample_extraction_id", "Sample extraction ID"),
            fld("target", "Target"),
            fld(
                "dilution_used", "Dilution used", coerce=ingest_utils.fix_date_interval
            ),
            fld("reads", re.compile(r"^# of (raw )?reads$")),
            fld("analysis_software_version", "AnalysisSoftwareVersion"),
            fld("comments", re.compile(r"^comments")),
            fld("sample_name_on_sample_sheet", "Sample name on sample sheet"),
            # special case: we merge these together (and throw a hard error if more than one has data for a given row)
            fld("pass_fail", "P=pass, F=fail"),
            fld("pass_fail_neat", "1:10 PCR, P=pass, F=fail"),
            fld("pass_fail_10", "1:100 PCR, P=pass, F=fail"),
            fld("pass_fail_100", "neat PCR, P=pass, F=fail"),
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
        "skip": [files.mm_amplicon_control_filename_re],
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super().__init__()
        self.google_track_meta = MarineMicrobesGoogleTrackMetadata()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = {
            amplicon: MarineMicrobesTrackMetadata(fname)
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

        logger.info("Ingesting Marine Microbes metadata from {0}".format(self.path))
        packages = []
        for fname in unique_spreadsheets(glob(self.path + "/*.xlsx")):
            base_fname = os.path.basename(fname)
            logger.info(
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
                    google_track_meta.date_of_transfer_to_archive
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
                        "read_length": mm_amplicon_read_length(row.amplicon),
                        "target": row.target,
                        "pass_fail": ingest_utils.merge_pass_fail(row),
                        "dilution_used": row.dilution_used,
                        "reads": row.reads,
                        "analysis_software_version": row.analysis_software_version,
                        "amplicon": amplicon,
                        "notes": "Marine Microbes Amplicons %s %s %s"
                        % (amplicon, sample_id, flow_id),
                        "title": "Marine Microbes Amplicons %s %s %s"
                        % (amplicon, sample_id, flow_id),
                        "omics": "Genomics",
                        "analytical_platform": "MiSeq",
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            google_track_meta.date_of_transfer
                        ),
                        "data_type": google_track_meta.data_type,
                        "description": google_track_meta.description,
                        "folder_name": google_track_meta.folder_name,
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            google_track_meta.date_of_transfer
                        ),
                        "data_generated": ingest_utils.get_date_isoformat(
                            google_track_meta.date_of_transfer_to_archive
                        ),
                        "archive_ingestion_date": archive_ingestion_date,
                        "license_id": apply_license(archive_ingestion_date),
                        "dataset_url": google_track_meta.download,
                        "ticket": row.ticket,
                        "facility": row.facility_code.upper(),
                        "type": self.ckan_data_type,
                        "comments": row.comments,
                        "private": True,
                    }
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id))
                ingest_utils.add_spatial_extra(obj)
                tag_names = ["amplicons", amplicon]
                if obj.get("sample_type"):
                    tag_names.append(obj["sample_type"])
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting MM md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            use_index_linkage = os.path.basename(md5_file) in self.index_linkage_md5s
            logger.info("Processing md5 file {} {}".format(md5_file, use_index_linkage))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_id = ingest_utils.extract_ands_id(file_info.get("id"))
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
        return resources


class MarineMicrobesAmpliconsControlMetadata(AMDBaseMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "mm-genomics-amplicon-control"
    omics = "genomics"
    technology = "amplicons-control"
    contextual_classes = []
    metadata_patterns = [r"^.*\.md5"]
    resource_linkage = ("amplicon", "flow_id")
    md5 = {
        "match": [files.mm_amplicon_control_filename_re],
        "skip": [files.mm_amplicon_filename_re],
    }
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/amplicons/"
    ]
    metadata_url_components = ("amplicon", "facility_code", "ticket")

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super().__init__()
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info
        self.google_track_meta = MarineMicrobesGoogleTrackMetadata()

    def md5_lines(self):
        logger.info("Ingesting MM md5 file information from {0}".format(self.path))
        for md5_file in glob(self.path + "/*.md5"):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                yield filename, md5, md5_file, file_info

    def _get_packages(self):
        flow_id_info = {
            t["flow_id"]: self.metadata_info[os.path.basename(fname)]
            for _, _, fname, t in self.md5_lines()
        }
        packages = []
        for flow_id, info in sorted(flow_id_info.items()):
            obj = {}
            amplicon = info["amplicon"]
            name = sample_id_to_ckan_name(
                "control", self.ckan_data_type + "-" + amplicon, flow_id
            ).lower()
            google_track_meta = self.google_track_meta.get(info["ticket"])

            archive_ingestion_date = ingest_utils.get_date_isoformat(
                google_track_meta.date_of_transfer_to_archive
            )

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "flow_id": flow_id,
                    "notes": "Marine Microbes Amplicons Control %s %s"
                    % (amplicon, flow_id),
                    "title": "Marine Microbes Amplicons Control %s %s"
                    % (amplicon, flow_id),
                    "omics": "Genomics",
                    "analytical_platform": "MiSeq",
                    "read_length": mm_amplicon_read_length(amplicon),
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        google_track_meta.date_of_transfer
                    ),
                    "data_type": google_track_meta.data_type,
                    "description": google_track_meta.description,
                    "folder_name": google_track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        google_track_meta.date_of_transfer
                    ),
                    "data_generated": ingest_utils.get_date_isoformat(
                        google_track_meta.date_of_transfer_to_archive
                    ),
                    "archive_ingestion_date": archive_ingestion_date,
                    "license_id": apply_license(archive_ingestion_date),
                    "dataset_url": google_track_meta.download,
                    "ticket": info["ticket"],
                    "facility": info["facility_code"].upper(),
                    "amplicon": amplicon,
                    "type": self.ckan_data_type,
                    "private": True,
                }
            )
            ingest_utils.add_spatial_extra(obj)
            tag_names = ["amplicons-control", amplicon, "raw"]
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            amplicon = xlsx_info["amplicon"]
            resource = file_info.copy()
            resource["md5"] = resource["id"] = md5
            resource["name"] = filename
            resource["resource_type"] = self.ckan_data_type
            resource["amplicon"] = amplicon
            legacy_url = urljoin(xlsx_info["base_url"], filename)
            resources.append(((amplicon, resource["flow_id"]), legacy_url, resource))
        return resources


class BaseMarineMicrobesMetadata(AMDBaseMetadata):
    def __init__(self, *args, **kwargs):
        super(BaseMarineMicrobesMetadata, self).__init__(*args, **kwargs)
        self.google_track_meta = MarineMicrobesGoogleTrackMetadata()
        self.track_meta = MarineMicrobesTrackMetadata(self.tracker_filename)


class MarineMicrobesMetagenomicsMetadata(BaseMarineMicrobesMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "mm-metagenomics"
    omics = "metagenomics"
    contextual_classes = common_context
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
            fld("sample_extraction_id", "Sample extraction ID"),
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
            files.mm_metagenomics_filename_re,
            files.mm_metagenomics_filename_v2_re,
        ],
        "skip": None,
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(MarineMicrobesMetagenomicsMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info

    def _get_packages(self):
        logger.info("Ingesting Marine Microbes metadata from {0}".format(self.path))
        packages = []
        for fname in unique_spreadsheets(glob(self.path + "/*.xlsx")):
            logger.info(
                "Processing Marine Microbes metadata file {0}".format(
                    os.path.basename(fname)
                )
            )
            for row in MarineMicrobesMetagenomicsMetadata.parse_spreadsheet(
                fname, self.metadata_info
            ):
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
                    google_track_meta.date_of_transfer_to_archive
                )

                obj.update(
                    {
                        "name": name,
                        "id": name,
                        "sample_id": sample_id,
                        "notes": "Marine Microbes Metagenomics %s" % (sample_id),
                        "title": "Marine Microbes Metagenomics %s" % (sample_id),
                        "omics": "metagenomics",
                        "analytical_platform": "HiSeq",
                        "read_length": "250bp",
                        "date_of_transfer": ingest_utils.get_date_isoformat(
                            google_track_meta.date_of_transfer
                        ),
                        "data_type": google_track_meta.data_type,
                        "description": google_track_meta.description,
                        "folder_name": google_track_meta.folder_name,
                        "sample_submission_date": ingest_utils.get_date_isoformat(
                            google_track_meta.date_of_transfer
                        ),
                        "data_generated": ingest_utils.get_date_isoformat(
                            google_track_meta.date_of_transfer_to_archive
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
                        "private": True,
                    }
                )
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id))
                ingest_utils.add_spatial_extra(obj)
                tag_names = ["metagenomics", "raw"]
                if obj.get("sample_type"):
                    tag_names.append(obj["sample_type"])
                obj["tags"] = [{"name": t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting MM md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_id = ingest_utils.extract_ands_id(file_info.get("id"))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((sample_id,), legacy_url, resource))
        return resources


class MarineMicrobesMetatranscriptomeMetadata(BaseMarineMicrobesMetadata):
    organization = "australian-microbiome"
    ckan_data_type = "mm-metatranscriptome"
    contextual_classes = common_context
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
            fld("sample_extraction_id", "Sample extraction ID"),
            fld("insert_size_range", "Insert size range"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld("analysis_software_version", "CASAVA version"),
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

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(MarineMicrobesMetatranscriptomeMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info

    def _get_packages(self):
        logger.info(
            "Ingesting Marine Microbes Transcriptomics metadata from {0}".format(
                self.path
            )
        )
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and sample_id is the primary key
        all_rows = set()
        for fname in unique_spreadsheets(glob(self.path + "/*.xlsx")):
            logger.info(
                "Processing Marine Microbes Transcriptomics metadata file {0}".format(
                    os.path.basename(fname)
                )
            )
            for row in MarineMicrobesMetatranscriptomeMetadata.parse_spreadsheet(
                fname, self.metadata_info
            ):
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
                google_track_meta.date_of_transfer_to_archive
            )

            obj.update(
                {
                    "name": name,
                    "id": name,
                    "sample_id": sample_id,
                    "notes": "Marine Microbes Metatranscriptome %s" % (sample_id),
                    "title": "Marine Microbes Metatranscriptome %s" % (sample_id),
                    "omics": "metatranscriptomics",
                    "analytical_platform": "HiSeq",
                    "read_length": "250bp",  # to be confirmed by Jason Koval
                    "date_of_transfer": ingest_utils.get_date_isoformat(
                        google_track_meta.date_of_transfer
                    ),
                    "data_type": google_track_meta.data_type,
                    "description": google_track_meta.description,
                    "folder_name": google_track_meta.folder_name,
                    "sample_submission_date": ingest_utils.get_date_isoformat(
                        google_track_meta.date_of_transfer
                    ),
                    "data_generated": ingest_utils.get_date_isoformat(
                        google_track_meta.date_of_transfer_to_archive
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
                    "private": True,
                }
            )
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(sample_id))
            ingest_utils.add_spatial_extra(obj)
            tag_names = ["metatranscriptome", "raw"]
            if obj.get("sample_type"):
                tag_names.append(obj["sample_type"])
            obj["tags"] = [{"name": t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting MM md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + "/*.md5"):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource["md5"] = resource["id"] = md5
                resource["name"] = filename
                resource["resource_type"] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_id = ingest_utils.extract_ands_id(file_info.get("id"))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info["base_url"], filename)
                resources.append(((sample_id,), legacy_url, resource))
        return resources
