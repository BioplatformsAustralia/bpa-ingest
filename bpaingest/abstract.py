import os
import re
from glob import glob
from urllib.parse import urlparse, urljoin

from .libs import ingest_utils
from .libs.excel_wrapper import (
    ExcelWrapper,
    make_field_definition as fld,
    make_skip_column as skp,
)
from .libs.md5lines import MD5Parser
from .resource_metadata import resource_metadata_from_file
from .util import make_logger, one


class BaseMetadata:
    auth = ("bpaingest", "bpaingest")
    resource_linkage = ("sample_id",)

    def build_notes_into_object(self, obj):
        obj.update({"notes": self.build_notes_without_blanks(obj)})

    def build_notes_without_blanks(self, obj):
        notes = ""
        # ensure blank fields are not used
        for next_note in self.notes_mapping:
            next_value = obj.get(next_note["key"], "")
            if next_value:
                notes += next_value + next_note.get("separator", "")
        return notes

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
            legacy_url = urljoin(xlsx_info["base_url"], os.path.basename(fname))
            resources.append((linkage, legacy_url, resource))
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
                    "Duplicate MD5 file {0} in ticket {1} may lead to duplicate resources or other issues"
                    .format(md5_file, ticket))

            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                if filename not in files_in_md5:
                    files_in_md5.add(filename)
                else:
                    ticket = self.metadata_info[os.path.basename(md5_file)]["ticket"]
                    self._logger.error(
                       "Duplicate filename {0} in md5 file {1} in ticket {2} may lead to duplicate resources"
                       .format(filename, md5_file, ticket))

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
            legacy_url = urljoin(file_info["base_url"], md5_basename)
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
        ):
            return ingest_utils.extract_ands_id(self._logger, value)
        return value

    def _read_metadata(self, fname):

        field_spec = [
            fld(
                "access_control_date",
                "access_control_date",
                coerce=ingest_utils.date_or_int_or_comment,
            ),
            fld("access_control_reason", "access_control_reason"),
            fld("related_data", "related_data"),
        ]

        # Handle some data types using prepending bpa_ to the linkage fields
        if len(
            set(self.contextual_linkage).intersection(
                {"bpa_sample_id", "bpa_library_id", "bpa_dataset_id"},
            )
        ):
            for field in ("bpa_sample_id", "bpa_library_id", "bpa_dataset_id"):
                field_spec.append(
                    fld(field, field, coerce=ingest_utils.extract_ands_id,)
                )
        else:
            for field in ("sample_id", "library_id", "dataset_id"):
                field_spec.append(
                    fld(field, field, coerce=ingest_utils.extract_ands_id,)
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

