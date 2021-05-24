import os
from glob import glob
from urllib.parse import urlparse, urljoin

from .libs.excel_wrapper import ExcelWrapper
from .libs.md5lines import MD5Parser
from .resource_metadata import resource_metadata_from_file


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
        self._linkage_xlsx = {}

    def track_xlsx_resource(self, obj, fname):
        """
        track a spreadsheet that needs to be uploaded into the packages generated from it
        """
        linkage_key = tuple([obj[t] for t in self.resource_linkage])
        assert linkage_key not in self._linkage_xlsx
        self._linkage_xlsx[linkage_key] = fname

    def generate_xlsx_resources(self):
        if len(self._linkage_xlsx) == 0:
            self._logger.error(
                "no linkage xlsx, likely a bug in the ingest class (xlsx resource needs to be tracked in package "
                "creation) "
            )
        resources = []
        for linkage, fname in self._linkage_xlsx.items():
            resource = resource_metadata_from_file(linkage, fname, self.ckan_data_type)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            legacy_url = urljoin(xlsx_info["base_url"], os.path.basename(fname))
            resources.append((linkage, legacy_url, resource))
        return resources

    def md5_lines(self):
        self._logger.info(
            "Ingesting MD5 file information from {0}".format(self.path)
        )
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                yield filename, md5, md5_file, file_info

    def generate_md5_resources(self):
        md5_resources = []
        for md5_file in glob(self.path + "/*.md5"):
            self._logger.info("Processing md5 file {}".format(md5_file))
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            base_url = urljoin(xlsx_info["base_url"], md5_file)
            self.generate_static_file_for_resources(md5_file, base_url, md5_resources)
        return md5_resources

    # can use `track_xlsx_resource` to add any resource to each package
    def generate_static_file_for_resources(self, source_path, base_url, resources=None):
        if resources is None:
            resources = []
        if len(self._linkage_xlsx) == 0:
            self._logger.error(
                "no linkage xlsx, likely a bug in the ingest class (xlsx resource needs to be tracked in package "
                "creation) "
            )
        for linkage, xlsx_name in self._linkage_xlsx.items():
            resource = resource_metadata_from_file(
                linkage, source_path, self.ckan_data_type
            )
            legacy_url = urljoin(base_url, os.path.basename(source_path))
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
