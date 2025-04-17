import os
import re
from copy import deepcopy
from glob import glob
from urllib.parse import urlparse, urljoin, quote

from .libs import ingest_utils
from .libs.excel_wrapper import (
    ExcelWrapper,
    make_field_definition as fld,
    make_skip_column as skp,
)
from .libs.md5lines import MD5Parser
from .resource_metadata import resource_metadata_from_file, resource_metadata_id
from .util import make_logger, one, clean_filename
import re


class BaseMetadata:
    auth = ("bpaingest", "bpaingest")
    resource_linkage = ("sample_id",)
    resource_info = {}
    common_files = []

    """
    The following regexp is used to mark certain resources as optional, so they don't get
    automatically included in the bulk download.
    Files ending in _pod5.tar, and subreads.bam may be very large, and are not required by
    most researchers. Sample filenames that are optional include:
       PP_BRF_PAW59370_ONTPromethION_pod5.tar
       357464_TSI_AGRF_DA061164.subreads.bam
       57368_TSI_CAGRF20114490_DA060254_subreads.bam
    """

    OPTIONAL_PATTERN = r"""
    (.*pod5\.tar$|.*subreads\.bam$)
            """

    def method_exists(self, method_name):
        if hasattr(self, method_name):  # should check if its callable too
            return True
        return False

    def run_method_if_exists(self, method_name, arg):
        if self.method_exists(method_name):
            self.method_name(arg)
            print("ran the method")
            return True
        print("did not run the method")
        return False

    def build_notes_into_object(self, obj, additional={}):
        obj.update(
            {
                "notes": self.build_string_from_map_without_blanks(
                    self.notes_mapping, obj, additional
                )
            }
        )

    def build_string_from_map_without_blanks(self, field_map, obj, additional={}):
        result = ""
        # ensure blank fields are not used
        for next_field in field_map:
            next_value = str(
                obj.get(next_field["key"], additional.get(next_field["key"], ""))
            )
            if next_value:
                result += next_value + next_field.get("separator", "")
        # remove any additional trailing blanks and commas before returning
        return result.rstrip(", ")

    def build_title_into_object(self, obj, additional={}):
        built_title = self.build_string_from_map_without_blanks(
            self.title_mapping, obj, additional
        )
        if built_title:
            obj.update({"title": built_title})

    def parse_spreadsheet(self, fname, metadata_info):
        kwargs = self.spreadsheet["options"]
        wrapper = ExcelWrapper(
            self._logger,
            self.spreadsheet["fields"],
            fname,
            additional_context=metadata_info[os.path.basename(fname)],
            suggest_template=True,
            **kwargs,
        )
        for error in wrapper.get_errors():
            self._logger.error(error)
        rows = list(wrapper.get_all())
        return rows

    def parse_md5file_unwrapped(self, fname):
        match = self.md5["match"]
        skip = self.md5["skip"]
        return MD5Parser(fname, match, skip)

    def parse_md5file(self, fname):
        p = self.parse_md5file_unwrapped(fname)
        for tpl in p.matches:
            yield tpl
        for tpl in p.no_match:
            self._logger.error("No match for filename: `%s'" % tpl)

    def get_tracking_info(self, ticket, field_name=None):
        if self.google_track_meta is None:
            return None

        if ticket is None:
            return None

        tracking_row = self.google_track_meta.get(ticket)
        if tracking_row is None:
            self._logger.warn("No tracking row found for {}".format(ticket))
            return None

        if field_name is None:
            return tracking_row
            # todo check attribute exists, throw error/log if not
        return getattr(tracking_row, field_name)

    def _get_resource_info(self, resource_info):
        # subclasses may choose to implement this, not required tho. TSI pacbio-hifi as an example
        return

    def _get_common_resources(self):
        self._logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        md5_files_added_as_resources = set({})
        self._get_resource_info(self.resource_info)
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = file_info.copy()
            resource["md5"] = resource["id"] = md5
            resource["name"] = clean_filename(os.path.basename(filename))
            resource["resource_path"] = os.path.dirname(filename)
            resource["resource_type"] = self.ckan_data_type
            # these are set to False by default
            # methods to a) determine if they should be set to True and
            #            b) set other values within the resource (eg adjusted id, file location etc) as required
            # are still to be developed.
            resource["shared_file"] = False
            resource["optional_file"] = False
            optional_re = re.compile(self.OPTIONAL_PATTERN, re.VERBOSE)
            if optional_re.match(os.path.basename(filename)):
                resource["optional_file"] = True
                self._logger.warn("Optional files match {}".format(filename))

            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info["base_url"], quote(filename))
            raw_resources_info = self.resource_info.get(os.path.basename(filename), "")
            # if download_info exists for raw_resources, then use remote URL
            if raw_resources_info:
                legacy_url = urljoin(
                    raw_resources_info["base_url"], quote(os.path.basename(filename))
                )
            self._add_datatype_specific_info_to_resource(resource, md5_file)
            if hasattr(self, "common_files_match"):
                if any(
                    regex.match(os.path.basename(filename))
                    for regex in self.common_files_match
                ):
                    resource["shared_file"] = True
                    self.common_files.append(
                        (
                            self.ckan_data_type,
                            (
                                self._build_common_files_linkage(
                                    xlsx_info, resource, file_info
                                ),
                                legacy_url,
                                resource,
                            ),
                        )
                    )
                    self._logger.warn("Common files match {}".format(filename))
                    continue
            resources.append(
                (
                    self._build_resource_linkage(xlsx_info, resource, file_info),
                    legacy_url,
                    resource,
                )
            )

            # could add similar code to call to the generate_md5_resources, but it would
            # need to migrate to the ambd base class, otherwise it will be in existance
            #  for all datatypes (as it is in abstract)
            #             from inspect import ismethod
            #
            #             def method_exists(instance, method):
            #                 return hasattr(instance, method) and ismethod(getattr(instance, method))
            if hasattr(self, "add_md5_as_resource"):
                if (
                    self.add_md5_as_resource is not None
                    and self.add_md5_as_resource is True
                ):
                    if md5_file not in md5_files_added_as_resources:
                        resources.extend(self.generate_md5_resources(md5_file))
                        md5_files_added_as_resources.add(md5_file)
        return resources

    def _add_datatype_specific_info_to_resource(self, resource, md5_file=None):
        """
        Add datatype specific items to the resource dict (eg sample_id)
        """
        raise NotImplementedError("implement _add_datatype_specific_info_to_resource()")

    def _build_resource_linkage(self, xlsx_info, resource, file_info):
        """
        Build the resource linkage. This varies from datatype to datatype.
        """
        raise NotImplementedError("implement _build_resource_linkage()")

    def _build_common_files_linkage(self, xlsx_info, resource, file_info):
        """
        Build the common files linkage. This varies from datatype to datatype.
        """
        raise NotImplementedError("implement _build_common_files_linkage()")

    def _get_packages(self):
        """
        return a list of dictionaries representing CKAN packages
        private method, do not call directly.
        """
        raise NotImplementedError("implement _get_packages()")

    def _get_resources(self):
        """
        return a list of tuples:
          (package_id, legacy_url, resource)
        private method, do not call directly.

        package_id:
          value of attr `resource_linkage` on the corresponding package for
          this resource
        legacy_url: link to download asset from legacy archive
        resource: dictionary representing CKAN resource
        """
        raise NotImplementedError("implement _get_resources()")

    @classmethod
    def resources_add_format(cls, resources):
        """
        centrally assign formats to resources, based on file extension: no point
        duplicating this function in all the get_resources() implementations.
        if a get_resources() implementation needs to override this, it can just set
        the format key in the resource, and this function will leave the resource
        alone
        """
        extension_map = {
            "JPG": "JPEG",
            "TGZ": "TAR",
        }
        for resource_linkage, legacy_url, resource_obj in resources:
            if "format" in resource_obj:
                continue
            filename = urlparse(legacy_url).path.split("/")[-1]
            if "." not in filename:
                continue
            extension = filename.rsplit(".", 1)[-1].upper()
            extension = extension_map.get(extension, extension)
            if filename.lower().endswith(".fastq.gz"):
                resource_obj["format"] = "FASTQ"
            elif filename.lower().endswith(".fasta.gz"):
                resource_obj["format"] = "FASTA"
            elif filename.lower().endswith(".vcf.gz"):
                resource_obj["format"] = "VCF"
            elif filename.lower().endswith(".gvcf.gz"):
                resource_obj["format"] = "GVCF"
            elif filename.lower().endswith(".md5sum"):
                resource_obj["format"] = "MD5"
            elif extension in (
                "PNG",
                "XLSX",
                "XLS",
                "PPTX",
                "ZIP",
                "TAR",
                "GZ",
                "DOC",
                "DOCX",
                "PDF",
                "CSV",
                "JPEG",
                "XML",
                "BZ2",
                "EXE",
                "EXF",
                "FASTA",
                "FASTQ",
                "SCAN",
                "WIFF",
                "JSON",
                "BAM",
                "HTML",
                "MD5",
                "BLOW5",
                "REK",
                "RAW",
                "WIFF2",
                "DATA",
                "SER",
                "FID",
            ):
                resource_obj["format"] = extension

    @classmethod
    def obj_round_floats_and_stringify(cls, objs):
        """
        CKAN will turn our floats into strings, and it'll round them in the process.
        to avoid a bug in our sync code, trying to undo that forever, we round
        and stringify ourselves. this mutates each object in-place.
        """
        for obj in objs:
            for k, v in obj.items():
                if isinstance(v, float):
                    obj[k] = str(round(v, 10))

    def __init__(self, logger, *args, **kwargs):
        self._logger = logger
        self._packages = self._resources = None
        self._linkage_xlsx_linkage = {}
        self._linkage_xlsx_file = {}
        self._linkage_md5 = {}

    def track_xlsx_resource(self, obj, fname):
        """
        track a spreadsheet that needs to be uploaded into the packages generated from it
        """
        linkage = tuple([obj[t] for t in self.resource_linkage])
        linkage_key = (fname, linkage)
        assert linkage_key not in self._linkage_xlsx_linkage
        self._linkage_xlsx_linkage[linkage_key] = linkage
        self._linkage_xlsx_file[linkage_key] = fname

    def track_packages_for_md5(self, obj, ticket):
        """
        track packages for md5s that needs to be uploaded into the packages, if metadata_info shows the ticket matches
        """
        linkage = tuple([obj[t] for t in self.resource_linkage])
        for f in self.all_md5_filenames:
            if f not in self._linkage_md5:
                self._linkage_md5[f] = []
            if (
                self.metadata_info[f]["ticket"] == ticket
                and linkage not in self._linkage_md5[f]
            ):
                self._linkage_md5[f].append(linkage)

    def generate_xlsx_resources(self):
        if len(self._linkage_xlsx_linkage) == 0:
            self._logger.error(
                "no linkage xlsx, likely a bug in the ingest class (xlsx resource needs to be tracked in package "
                "creation) "
            )
        resources = []
        for key in self._linkage_xlsx_linkage:
            linkage = self._linkage_xlsx_linkage[key]
            fname = self._linkage_xlsx_file[key]
            resource = resource_metadata_from_file(linkage, fname, self.ckan_data_type)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            legacy_url = urljoin(xlsx_info["base_url"], quote(os.path.basename(fname)))
            resources.append((linkage, legacy_url, resource))
        return resources

    def generate_common_files_resources(self, linked_resources):
        resources = []
        resource_linkages = set()

        # common_files_linkage must be a subset of resource_linkage
        # for every distinct resource_linkage in linked_resources,
        #
        # check common resources not empty
        if len(self.common_files_linkage) == 0:
            self._logger.error(
                "no common files linkage, likely a bug in the ingest class"
            )

        # check linked_resources is not empty
        if len(linked_resources) == 0:
            self._logger.error(
                "no resources to link common files to, likely a bug in the ingest class"
            )

        # check common files linkage a subset of resource linkage
        if not set(self.common_files_linkage) <= set(self.resource_linkage):
            self._logger.error(
                "common files linkage not a subset, likely a bug in the ingest class"
            )

        # seen linkages empty set
        # for each linked resources
        for linked_resource in linked_resources:
            linkage = linked_resource[0]
            #   if linkage not seen
            if linkage not in resource_linkages:
                #      add linkage to seen
                resource_linkages.add(linkage)
                #      iterate over common files
                for data_type, common_file in self.common_files:
                    if data_type is not self.ckan_data_type:
                        continue
                    lr = dict(zip(self.resource_linkage, linked_resource[0]))
                    cr = dict(zip(self.common_files_linkage, common_file[0]))
                    shared_items = {
                        k: lr[k]
                        for k in self.common_files_linkage
                        if k in lr and lr[k] == cr[k]
                    }
                    #        if linkage matches
                    if len(shared_items) == len(self.common_files_linkage):
                        self._logger.info(
                            "Attaching {} with linkage {}".format(
                                common_file[2]["name"], linkage
                            )
                        )
                        #           generate linkage and resource
                        common_resource = deepcopy(common_file[2])
                        #           perturb id
                        common_resource["id"] = resource_metadata_id(
                            linkage, common_resource["name"]
                        )
                        # add resource
                        resources.append(
                            (
                                linkage,
                                common_file[1],
                                common_resource,
                            )
                        )

        return resources

    def md5_lines(self):
        files_in_md5 = set({})
        md5_files = set({})
        self._logger.info("Ingesting MD5 file information from {0}".format(self.path))
        for md5_file in glob(self.path + "/*.md5"):
            if md5_file not in md5_files:
                md5_files.add(md5_file)
            else:
                ticket = self.metadata_info[os.path.basename(md5_file)]["ticket"]
                self._logger.error(
                    "Duplicate MD5 file {0} in ticket {1} may lead to duplicate resources or other issues".format(
                        md5_file, ticket
                    )
                )

            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                if filename not in files_in_md5:
                    files_in_md5.add(filename)
                else:
                    ticket = self.metadata_info[os.path.basename(md5_file)]["ticket"]
                    self._logger.error(
                        "Duplicate filename {0} in md5 file {1} in ticket {2} may lead to duplicate resources".format(
                            filename, md5_file, ticket
                        )
                    )

                yield filename, md5, md5_file, file_info

    def generate_md5_resources(self, md5_file):
        self._logger.info("Processing md5 file {}".format(md5_file))
        md5_basename = os.path.basename(md5_file)
        file_info = self.metadata_info[md5_basename]
        if len(self._linkage_md5) < 1:
            self._logger.error(
                "no linkage xlsx, likely a bug in the ingest class (xlsx resource needs to be tracked in package "
                "creation) "
            )
        resources = []
        for linkage in self._linkage_md5[md5_basename]:
            resource = resource_metadata_from_file(
                linkage, md5_file, self.ckan_data_type
            )
            resource["shared_file"] = True  # all md5s are shared.
            legacy_url = urljoin(file_info["base_url"], quote(md5_basename))
            resources.append((linkage, legacy_url, resource))
        return resources

    def _get_packages_and_resources(self):
        # ensure that each class can expect to have _get_packages() called first,
        # then _get_resources(), and only once in the entire lifetime of the class.
        if self._packages is None:
            self._packages = self._get_packages()
            self._resources = self._get_resources()
            BaseMetadata.resources_add_format(self._resources)
            BaseMetadata.obj_round_floats_and_stringify(self._packages)
            BaseMetadata.obj_round_floats_and_stringify(
                t for _, _, t in self._resources
            )
        return self._packages, self._resources

    def get_packages(self):
        self._get_packages_and_resources()
        return self._packages

    def get_resources(self):
        self._get_packages_and_resources()
        return self._resources


class BaseDatasetControlContextual:
    metadata_patterns = [re.compile(r"^.*\.xlsx$")]
    sheet_names = [
        "Dataset Control",
    ]
    contextual_linkage = ()
    name_mapping = {}
    additional_fields = []

    def __init__(self, logger, path):
        self._logger = logger
        self._logger.info("dataset control path is: {}".format(path))
        self.dataset_metadata = self._read_metadata(one(glob(path + "/*.xlsx")))

    def get(self, *context):
        if len(context) != len(self.contextual_linkage):
            self._logger.error(
                "Dataset Control context wanted %s does not match linkage %s"
                % (repr(context), repr(self.contextual_linkage))
            )
            return {}
        if context in self.dataset_metadata:
            self._logger.info("Dataset Control metadata found for: %s" % repr(context))
            return self.dataset_metadata[context]
        return {}

    def _coerce_ands(self, name, value):
        if name in (
            "sample_id",
            "library_id",
            "dataset_id",
            "bpa_sample_id",
            "bpa_library_id",
            "bpa_dataset_id",
            "bioplatforms_sample_id",
            "bioplatforms_library_id",
            "bioplatforms_dataset_id",
        ):
            return ingest_utils.extract_ands_id(self._logger, value)
        return value

    def _read_metadata(self, fname):
        # Obligatory fields
        field_spec = [
            fld(
                "access_control_date",
                "access_control_date",
                coerce=ingest_utils.date_or_int_or_comment,
            ),
            fld("access_control_reason", "access_control_reason"),
            fld("related_data", "related_data"),
        ]

        # Add any additional fields
        field_spec.extend(self.additional_fields)

        # ID fields used for linkage, add if present in linkage
        # Handle some data types using prepending bpa_ to the linkage fields
        # todo: make sure the linkage is a set at this point
        if len(
            set(self.contextual_linkage).intersection(
                {
                    "bpa_sample_id",
                    "bpa_library_id",
                    "bpa_dataset_id",
                    "bioplatforms_sample_id",
                    "bioplatforms_library_id",
                    "bioplatforms_dataset_id",
                },
            )
        ):
            for field in (
                "bpa_sample_id",
                "bpa_library_id",
                "bpa_dataset_id",
                "bioplatforms_sample_id",
                "bioplatforms_library_id",
                "bioplatforms_dataset_id",
            ):
                if field in self.contextual_linkage:
                    field_spec.append(
                        fld(
                            field,
                            field,
                            coerce=ingest_utils.extract_ands_id,
                        )
                    )
        else:
            for field in ("sample_id", "library_id", "dataset_id"):
                if field in self.contextual_linkage:
                    field_spec.append(
                        fld(
                            field,
                            field,
                            coerce=ingest_utils.extract_ands_id,
                        )
                    )

        dataset_metadata = {}
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

            name_mapping = self.name_mapping

            for row in wrapper.get_all():
                context = tuple(
                    [
                        self._coerce_ands(v, row._asdict().get(v, None))
                        for v in self.contextual_linkage
                    ]
                )
                # keys not existing in row to create linkage
                if None in context:
                    continue

                if context in dataset_metadata:
                    raise Exception(
                        "duplicate ids for linkage {}: {}".format(
                            repr(self.contextual_linkage), repr(context)
                        )
                    )

                dataset_metadata[context] = row_meta = {}
                for field in row._fields:
                    value = getattr(row, field)
                    if field in self.contextual_linkage:
                        continue
                    row_meta[name_mapping.get(field, field)] = value
        return dataset_metadata

    def filename_metadata(self, *args, **kwargs):
        return {}


class BaseLibraryContextual:
    metadata_patterns = [re.compile(r"^.*\.xlsx$")]

    sheet_names = ["Sample metadata"]

    name_mapping = {
        "decimal_longitude": "longitude",
        "decimal_latitude": "latitude",
        "klass": "class",
    }
    metadata_unique_identifier = "bioplatforms_sample_id"

    field_spec = [
        fld(
            "bioplatforms_sample_id",
            "bioplatforms_sample_id",
            coerce=ingest_utils.extract_ands_id,
        ),
        # sample_ID
        fld(
            "sample_id",
            "sample_id",
        ),
        fld("specimen_custodian", "specimen_custodian", optional=True),
        # specimen_ID
        fld("specimen_id", re.compile(r"specimen_?[Ii][Dd]")),
        # specimen_ID_description
        fld("specimen_id_description", re.compile(r"specimen_?[Ii][Dd]_description")),
        fld("sample_custodian", "sample_custodian"),
        fld("sample_type", "sample_type"),
        # sample_ID_description
        fld("sample_id_description", re.compile(r"sample_?[Ii][Dd]_description")),
        fld("sample_collection_type", "sample_collection_type"),
        fld("tissue", "tissue"),
        # tissue_preservation
        fld("tissue_preservation", "tissue_preservation"),
        fld("tissue_preservation_temperature", "tissue_preservation_temperature"),
        # sample_quality
        fld("sample_quality", "sample_quality"),
        fld("collection_permit", "collection_permit"),
        fld("identified_by", "identified_by"),
        fld("env_broad_scale", "env_broad_scale"),
        fld("env_local_scale", "env_local_scale"),
        fld("env_medium", "env_medium"),
        fld("altitude", "altitude"),
        fld("depth", "depth", coerce=ingest_utils.get_clean_number),
        fld("temperature", "temperature"),
        fld("location_info_restricted", "location_info_restricted"),
        fld("genotypic_sex", "genotypic_sex"),
        fld("phenotypic_sex", "phenotypic_sex", optional=True),
        fld("method_sex_determination", "method_sex_determination", optional=True),
        fld("sex_certainty", "sex_certainty", optional=True),
        # taxon_id
        fld("taxon_id", re.compile(r"taxon_[Ii][Dd]"), coerce=ingest_utils.get_int),
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
        fld("sub_species", "sub_species"),
        fld("scientific_name", "scientific_name"),
        fld("scientific_name_note", "scientific_name_note"),
        fld("scientific_name_authorship", "scientific_name_authorship"),
        # common_name
        fld("common_name", "common_name"),
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
        # skip the private lat/long as this will contain data not to be shared
        skp("decimal_latitude_private"),
        skp("decimal_longitude_private"),
        # decimal_latitude
        fld("decimal_latitude", "decimal_latitude_public"),
        fld("decimal_latitude_public", "decimal_latitude_public"),
        # decimal_longitude
        fld("decimal_longitude", "decimal_longitude_public"),
        fld("decimal_longitude_public", "decimal_longitude_public"),
        # coord_uncertainty_metres
        fld("coord_uncertainty_metres", "coord_uncertainty_metres", optional=True),
        # life-stage
        fld("lifestage", re.compile("life[_-]stage")),
        # birth_date
        fld("birth_date", "birth_date", coerce=ingest_utils.get_date_isoformat, optional=True),
        # death_date
        fld("death_date", "death_date", coerce=ingest_utils.get_date_isoformat, optional=True),
        fld("health_state", "health_state"),
        # associated_media
        fld("associated_media", "associated_media"),
        # ancillary_notes
        fld("ancillary_notes", "ancillary_notes"),
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
        fld('indigenous_location', 'indigenous_location', optional=True),
        fld('isolate', 'isolate', optional=True),
        fld('host_common_name', 'host_common_name', optional=True),
        fld('host_family', 'host_family', optional=True),
        fld('host_scientific_name', 'host_scientific_name', optional=True),
        fld('host_organ', 'host_organ', optional=True),
        fld('host_symptom', 'host_symptom', optional=True),
        fld('host_status', 'host_status', optional=True),
        fld('project_lead', 'project_lead', optional=True),
    ]

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
        library_metadata = {}
        for sheet_name in self.sheet_names:
            wrapper = ExcelWrapper(
                self._logger,
                self.field_spec,
                fname,
                sheet_name=sheet_name,
                header_length=1,
                column_name_row_index=0,
                suggest_template=True,
            )
            for error in wrapper.get_errors():
                self._logger.error(error)

            for row in wrapper.get_all():
                library_metadata = self.process_row(
                    row, library_metadata, os.path.basename(fname), wrapper.modified
                )

        return library_metadata

    def process_row(self, row, library_metadata, metadata_filename, metadata_modified):
        key_value = getattr(row, self.metadata_unique_identifier)
        if not key_value:
            return library_metadata
        if key_value in library_metadata:
            raise Exception(
                "duplicate {}: {}".format(self.metadata_unique_identifier, key_value)
            )
        library_metadata[key_value] = row_meta = {}
        library_metadata[key_value][
            "metadata_revision_date"
        ] = ingest_utils.get_date_isoformat(self._logger, metadata_modified)
        library_metadata[key_value]["metadata_revision_filename"] = metadata_filename
        for field in row._fields:
            value = getattr(row, field)
            if field == self.metadata_unique_identifier:
                continue
            row_meta[self.name_mapping.get(field, field)] = value
        return library_metadata
