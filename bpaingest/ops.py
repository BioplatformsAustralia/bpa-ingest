import subprocess
import tempfile
import urllib

import requests
import ckanapi
import os
from urllib.parse import urlparse
from collections import defaultdict
from .util import make_logger

logger = make_logger(__name__)
UPLOAD_RETRY = 3


# https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


method_stats = defaultdict(int)


def ckan_method(ckan, object_type, method):
    """
    returns a CKAN method from the upstream API, with an
    intermediate function which does some global accounting
    """
    fn = getattr(ckan.action, object_type + "_" + method)

    def _proxy_fn(*args, **kwargs):
        method_stats[(object_type, method)] += 1
        return fn(*args, **kwargs)

    return _proxy_fn


def print_accounts():
    print("API call accounting:")
    for object_type, method in sorted(method_stats, key=lambda x: method_stats[x]):
        print(
            (
                "  %14s  %6s  %d"
                % (object_type, method, method_stats[(object_type, method)])
            )
        )


def diff_objects(obj1, obj2, desc, skip_differences=None):
    def sort_if_list(v):
        if isinstance(v, list):
            return list(sorted(v, key=lambda v: repr(v)))
        return v

    differences = []
    for k in list(obj1.keys()):
        if skip_differences and k in skip_differences:
            continue
        v1 = sort_if_list(obj1.get(k))
        v2 = sort_if_list(obj2.get(k))
        # co-erce to string to cope with numeric types in the JSON data
        if v1 != v2 and str(v1) != str(v2):
            differences.append((k, v1, v2))
    if differences:
        logger.info("%s/%s: differs" % (desc, obj2.get("id", "<no id?>")))
    differences.sort()
    for k, v1, v2 in differences:
        logger.info("   -  %s=%s (%s)" % (k, v2, type(v2).__name__))
    for k, v1, v2 in differences:
        logger.info("   +  %s=%s (%s)" % (k, v1, type(v1).__name__))
    return len(differences) > 0


def patch_if_required(
    ckan, object_type, ckan_object, patch_object, skip_differences=None
):
    """
    patch ckan_object if applying patch_object would change it. ckan_object is unchanged
    for any keys which are not mentioned in patch_object
    """
    patch_needed = diff_objects(patch_object, ckan_object, object_type)
    if patch_needed:
        ckan_object = ckan_method(ckan, object_type, "patch")(**patch_object)
    return patch_needed, ckan_object


def make_obj(ckan, obj_type, obj):
    try:
        ckan_obj = ckan_method(ckan, obj_type, "show")(id=obj["name"])
    except ckanapi.errors.NotFound:
        ckan_obj = ckan_method(ckan, obj_type, "create")(name=obj["name"])
        logger.info("created %s `%s'" % (obj_type, obj["name"]))
    # copy over auto-allocated ID
    obj["id"] = ckan_obj["id"]
    was_patched, ckan_obj = patch_if_required(ckan, obj_type, ckan_obj, obj)
    if was_patched:
        logger.info("updated %s `%s'" % (obj_type, obj["name"]))
    return ckan_obj


def make_group(ckan, group_obj):
    return make_obj(ckan, "group", group_obj)


def make_organization(ckan, organization_obj):
    return make_obj(ckan, "organization", organization_obj)


class BaseArchiveInfo:
    def __init__(self):
        self._size_cache = {}

    def check_status_code(self, response):
        if response.status_code in (403, 401):
            # if we're getting 403s and we re-upload a bunch of data because of it, that is unhelpful
            logger.error("authentication error accessing archive")
            raise Exception("authentication failed")

    def size_from_response(self, response):
        self.check_status_code(response)
        if response.status_code not in (200, 206):
            return None
        if "content-range" in response.headers:
            return int(response.headers.get("content-range").rsplit("/", 1)[-1])
        if "content-length" in response.headers:
            return int(response.headers["content-length"])


def same_netloc(u1, u2):
    n1 = urlparse(u1).netloc
    n2 = urlparse(u2).netloc
    return n1 == n2


class CKANArchiveInfo(BaseArchiveInfo):
    def __init__(self, ckan):
        self.ckan = ckan
        self.session = requests.Session()
        super().__init__()

    def on_ckan(self, url):
        return same_netloc(self.ckan.address, url)

    def resolve_url(self, url):
        # the archive will issue an S3 link with auth token
        response = self.session.head(url, headers={"Authorization": self.ckan.apikey})
        self.check_status_code(response)
        assert response.status_code == 302
        return response.headers["location"]

    def ckan_address(self):
        return self.ckan.address

    def s3_simulated_head(self, url):
        # we have to do a range request for the first byte, as S3 doesn't let us head
        # with an authorization token. however, we can still get the full size from the
        # content-range header
        return requests.get(url, headers={"Range": "bytes=0-0"})

    def get_etag(self, url):
        if not url:
            return None
        # a URL on S3 with auth token
        resolved = self.resolve_url(url)
        if resolved is None:
            return None
        response = self.s3_simulated_head(resolved)
        self.check_status_code(response)
        if response.status_code not in (200, 206):
            return None
        return response.headers.get("etag")

    def get_size(self, url):
        if not url:
            return None
        if url not in self._size_cache:
            # a URL on S3 with auth token
            resolved = self.resolve_url(url)
            if resolved is None:
                return None
            # we have to do a range request for the first byte, as S3 doesn't let us head
            # with an authorization token. however, we can still get the full size from the
            # content-range header
            response = self.s3_simulated_head(resolved)
            self._size_cache[url] = self._size_cache[
                resolved
            ] = self.size_from_response(response)
        return self._size_cache[url]


class ApacheArchiveInfo(BaseArchiveInfo):
    def __init__(self, auth):
        self.auth = auth
        self.session = requests.Session()
        super().__init__()

    def head(self, url):
        return self.session.head(url, auth=self.auth)

    def resolve_url(self, url):
        """
        follow redirects until we get the final URL; unfortunately there are symlinks in the flat-file
        archive that need to be walked
        """
        new_url = url
        for i in range(4):
            response = self.session.head(new_url, auth=self.auth)
            self.check_status_code(response)
            if response.status_code == 301 or response.status_code == 302:
                new_url = response.headers.get("location")
            elif response.status_code == 200:
                return new_url
            else:
                return None

    def get_size(self, url):
        if not url:
            return None
        if url not in self._size_cache:
            resolved = self.resolve_url(url)
            if resolved is None:
                return None
            self._size_cache[url] = self._size_cache[
                resolved
            ] = self.size_from_response(self.head(resolved))
        return self._size_cache[url]


def check_resource(
    ckan_archive_info, apache_archive_info, current_url, legacy_url, metadata_etags,
):
    """
    returns None if the ckan_obj looks good (is on the CKAN server, size matches legacy url size)
    otherwise returns a short string describing the problem
    """

    if current_url is None:
        logger.error("resource missing (no current URL)")
        return "missing"

    if not ckan_archive_info.on_ckan(current_url):
        logger.error("resource is not hosted on CKAN server: %s" % (current_url))
        return "not-on-ckan"

    # determine the size of the original file in the legacy archive
    legacy_size = apache_archive_info.get_size(legacy_url)
    if legacy_size is None:
        logger.error("error getting size of: %s" % (legacy_url))
        return "error-getting-size"

    # determine the URL of the proxied s3 resource, and then its size
    current_size = ckan_archive_info.get_size(current_url)
    if current_size is None:
        logger.error("error getting size of: %s" % (current_url))
        return "error-getting-size"

    if current_size != legacy_size:
        logger.error(
            "CKAN resource %s has incorrect size: %d (should be %d)"
            % (current_url, current_size, legacy_size)
        )
        return "wrong-size"

    # if we have a pre-calculated s3etag in metadata, check it matches
    current_etag = ckan_archive_info.get_etag(current_url)
    if current_etag.strip('"') not in metadata_etags:
        if None in metadata_etags:
            logger.warning(
                "CKAN resource %s has no metadata etag: run genhash for this project."
                % (legacy_url)
            )
        else:
            logger.error(
                "CKAN resource %s has incorrect etag: %s (should be one of %s)"
                % (current_url, current_etag, metadata_etags)
            )
            return "wrong-etag"

    return None


def download_legacy_file(legacy_url, auth):
    if legacy_url.startswith("file:///"):
        raise Exception("Cannot upload local file. URL reference must be via http or https")
    basename = legacy_url.rsplit("/", 1)[-1]
    tempdir = tempfile.mkdtemp(prefix="bpaingest-data-")
    path = os.path.join(tempdir, basename)
    archive_info = ApacheArchiveInfo(auth)
    resolved_url = archive_info.resolve_url(legacy_url)
    logger.info("Resolved `%s' to `%s'" % (legacy_url, resolved_url))
    if not resolved_url:
        logger.error("unable to resolve `%s' - file missing?" % (legacy_url))
        os.rmdir(tempdir)
        return None, None
    # wget will resume downloads, which is a huge win when dealing with
    # mirrors that sometimes close connections. ugly, but pragmatic.
    wget_args = ["wget", "-q", "-c", "-t", "0", "-O", path]
    if auth:
        wget_args += ["--user", auth[0]]
        wget_args += ["--password", auth[1]]
    wget_args.append(resolved_url)
    status = subprocess.call(wget_args)
    if status != 0:
        try:
            os.unlink(path)
        except OSError:
            pass
        try:
            os.rmdir(tempdir)
        except OSError:
            pass
        return None, None
    return tempdir, path


def reupload_resource(ckan, ckan_obj, legacy_url, parent_destination, auth=None):
    "reupload data from legacy_url to ckan_obj"

    tempdir, path = download_legacy_file(legacy_url, auth)
    if path is None:
        logger.error("download from legacy archive failed")
        return
    try:
        logger.info("re-uploading from tempfile: %s" % (path))
        filename = path.split("/")[-1]
        s3_destination = "s3://{}/resources/{}/{}".format(
            parent_destination, ckan_obj["id"], filename
        )
        s3cmd_args = [
            "aws",
            "s3",
            "cp",
            path,
            s3_destination,
        ]
        status = subprocess.call(s3cmd_args)
        if status == 0:
            # patch the object in CKAN to have full URL
            resource_url = "{}/dataset/{}/resource/{}/download/{}".format(
                ckan.address, ckan_obj["package_id"], ckan_obj["id"], filename
            )
            ckan.action.resource_patch(
                id=ckan_obj["id"],
                url=resource_url,
                url_type="upload",
                size=os.path.getsize(path),
            )
        else:
            logger.error("upload failed: status {}".format(status))
    finally:
        os.unlink(path)
        os.rmdir(tempdir)


def create_resource(ckan, ckan_obj):
    "create resource, uploading data from legacy_url"
    return ckan_method(ckan, "resource", "create")(**ckan_obj)


def get_organization(ckan, id):
    return ckan_method(ckan, "organization", "show")(id=id)
