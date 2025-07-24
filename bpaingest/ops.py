import logging
import subprocess
import tempfile
import urllib
import urllib3
import shutil
import bitmath

import ckanapi
import tqdm
import os

import boto3
import boto3.session
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, WaiterError

from urllib.parse import urlparse
import time
from urllib.parse import urlparse, unquote
from urllib.request import url2pathname
from collections import defaultdict

from urllib3 import request

from .libs.ingest_utils import ApiFqBuilder
from .libs.bpa_constants import AUDIT_VERIFIED
from .libs.s3 import update_tags
from .libs.munge import bpa_munge_filename
from .util import make_logger
from .util import build_apache_headers_for_urllib3


logger = make_logger(__name__)
UPLOAD_RETRY = 3

method_stats = defaultdict(int)

KB = 1024
MB = KB * KB
GB = MB * KB

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
    logger.debug("start diff_objects")

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
    logger.debug("end diff_objects")
    return len(differences) > 0


def patch_if_required(
    ckan, object_type, ckan_object, patch_object, skip_differences=None
):
    """
    patch ckan_object if applying patch_object would change it. ckan_object is unchanged
    for any keys which are not mentioned in patch_object
    """
    logger.debug("start patch if required")
    patch_needed = diff_objects(patch_object, ckan_object, object_type)
    if patch_needed:
        try:
            ckan_object = ckan_method(ckan, object_type, "patch")(**patch_object)
        except ckanapi.errors.CKANAPIError:
            # wait 2 sec, then try again (give up if it fails a second time)
            logger.warning("ckan patch failed, waiting 2 sec, then trying again")
            time.sleep(2)
            ckan_object = ckan_method(ckan, object_type, "patch")(**patch_object)

    logger.debug("end patch_if_required")
    return patch_needed, ckan_object


def make_obj(ckan, obj_type, obj):
    logger.debug("start make_obj")
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
    logger.debug("end make_obj")
    return ckan_obj


def make_group(ckan, group_obj):
    return make_obj(ckan, "group", group_obj)


def make_organization(ckan, organization_obj):
    return make_obj(ckan, "organization", organization_obj)


class BaseArchiveInfo:
    def __init__(self):
        self._size_cache = {}
        self.http = urllib3.PoolManager()

    def check_status_code(self, response):
        if response.status in (403, 401):
            # if we're getting 403s and we re-upload a bunch of data because of it, that is unhelpful
            logger.error("authentication error accessing archive")
            raise Exception("authentication failed")

    def size_from_response(self, response):
        self.check_status_code(response)
        if response.status not in (200, 206):
            return None
        if "content-range" in response.headers:
            return int(response.headers.get("content-range").rsplit("/", 1)[-1])
        if "content-length" in response.headers:
            return int(response.headers["content-length"])
        logger.error("Unable to determine size, headers are: %s" % (response.headers,))
        return None


def same_netloc(u1, u2):
    n1 = urlparse(u1).netloc
    n2 = urlparse(u2).netloc
    return n1 == n2


class CKANArchiveInfo(BaseArchiveInfo):
    def __init__(self, ckan):
        self.ckan = ckan
        self._etag_cache = {}
        super().__init__()

    def on_ckan(self, url):
        return same_netloc(self.ckan.address, url)

    def ckan_address(self):
        return self.ckan.address

    def get_size_and_etag(self, url):
        if not url:
            return None
        logger.debug("start get_size_and_etag `%s' " % url)
        if url not in self._size_cache:

            logger.debug("URl not is size/etag cache, {}, go and get it".format(self._size_cache))
            # get the first 0 bytes, which will let us determine the size - HEAD does not work for S3
            try:
                response = self.http.request("GET", url,
                                             headers={"Authorization": self.ckan.apikey,
                                                      "User-Agent": "BPA-INGEST",
                                                      "Range": "bytes=0-0"})
            except urllib3.exceptions.MaxRetryError as mre:
                logger.error("Error when getting the url {} is {}".format(url, mre))
                return None

            logger.debug("got the headers for size and etag (empty content):{}".format(response.headers))
            logger.debug("https status is:{}".format(response.status))
            self.check_status_code(response)

            if response.status not in (200, 206):
                logger.error("Returning None for get size and etag - bad response code ({})".format(response.status))
                return None
            # Make sure it is AWSS3
            if response.headers.get("Server") != 'AmazonS3':
                logger.error("the URL {} does not reside on s3. Headers returned were {}".format(url, response.headers))
                return None
            self._size_cache[url] = self._size_cache[url] = self.size_from_response(response)
            self._etag_cache[url] = self._etag_cache[url] = response.headers.get("etag")

        return self._size_cache[url], self._etag_cache[url]


class ApacheArchiveInfo(BaseArchiveInfo):
    def __init__(self, auth):
        self.auth = auth
        super().__init__()   # need this first to set up the http
        self.headers = build_apache_headers_for_urllib3(self.auth)


    def resolve_url(self, url):
        """
        follow redirects until we get the final URL; unfortunately there are symlinks in the flat-file
        archive that need to be walked
        """
        new_url = url
        #todo: As we do all this now via http and not file system, and urrlib3 walks the http redirects for us,
        # do we still need to do this?
        for i in range(4):
            logger.debug("about to get the  head with urllib3, headers are:".format(self.headers))
            response = self.http.request("HEAD",
                                         new_url,
                                         headers=self.headers)
            logger.debug("response headers for {} are {}".format(new_url, response.headers))
            self.check_status_code(response)
            if response.status == 301 or response.status == 302:
                logger.debug("Walking the redirect tree of {}".format(new_url))
                new_url = response.headers.get("location")
            elif response.status == 200:
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
            ] = self.size_from_response(self.http.request("HEAD", resolved, headers=self.headers))
        return self._size_cache[url]


def get_legacy_size(apache_archive_info, legacy_url):
    if legacy_url and legacy_url.startswith("file:///"):
        logger.info("Determining local file `%s' size for upload" % (legacy_url,))
        p = urlparse(legacy_url)
        file_path = url2pathname(p.path)
        logger.info("Local file URL resolved to '%s'" % (file_path,))
        return os.path.getsize(file_path)

    return apache_archive_info.get_size(legacy_url)


def check_resource(
    ckan_archive_info,
    apache_archive_info,
    current_url,
    legacy_url,
    metadata_etags,
):
    """
    returns None if the ckan_obj looks good (is on the CKAN server, size matches legacy url size)
    otherwise returns a short string describing the problem
    """
    logger.debug("start check_resource `%s' " % current_url)
    if current_url is None:
        logger.error("resource missing (no current URL)")
        return "missing"

    if not ckan_archive_info.on_ckan(current_url):
        logger.error("resource is not hosted on CKAN server: %s" % (current_url))
        return "not-on-ckan"

    # determine the size of the original file in the legacy archive
    legacy_size = get_legacy_size(apache_archive_info, legacy_url)
    if legacy_size is None:
        logger.error("error getting legacy size of: %s" % (legacy_url))
        return "error-getting-size-legacy"

    # check if current URL aligns with resource on legacy
    legacy_filename_cleaned = bpa_munge_filename(legacy_url.split("/")[-1])
    current_filename = current_url.split("/")[-1]

    if current_filename != legacy_filename_cleaned:
        return "filename-change-reupload-needed"

    # single call to s3 from this function to speed up checks
    try:
        current_size, current_etag = ckan_archive_info.get_size_and_etag(current_url)
    except TypeError:
        current_size = None

    # determine the URL of the proxied s3 resource, and then its size
    if current_size is None:
        logger.error("error getting s3 size of: %s" % (current_url))
        return "error-getting-size-s3"

    if current_size != legacy_size:
        logger.error(
            "CKAN resource %s has incorrect size: %d (should be %d)"
            % (current_url, current_size, legacy_size)
        )
        return "wrong-size"

    # if we have a pre-calculated s3etag in metadata, check it matches
    logger.info(f"current etag is {current_etag}")
    if current_etag and current_etag.strip('"') not in metadata_etags:
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
    logger.debug("end check_resource `%s' " % current_url)
    return None


def resolve_legacy_file(legacy_url, auth):
    if legacy_url and legacy_url.startswith("file:///"):
        raise Exception(
            "Cannot download local file. URL reference must be via http or https"
        )
    archive_info = ApacheArchiveInfo(auth)
    resolved_url = archive_info.resolve_url(legacy_url)
    logger.info("Resolved `%s' to `%s'" % (legacy_url, resolved_url))
    if not resolved_url:
        logger.error("unable to resolve `%s' - file missing?" % (legacy_url))
        return None
    return resolved_url


def download_legacy_local_file(legacy_url):
    logger.info("Checking local file `%s' for upload" % (legacy_url,))

    p = urlparse(legacy_url)
    file_path = url2pathname(p.path)
    logger.info("Local file URL resolved to '%s'" % (file_path,))

    # Need to check directory is readable
    # Need to check file is readable and regular

    if not os.path.isfile(file_path) and not os.access(file_path, os.R_OK):
        logger.warning("File '%s' doesn't exist or isn't readable" % (file_path,))
        return None, None

    # Create place to copy them to
    basename = file_path.rsplit(os.path.sep, 1)[-1]
    tempdir = tempfile.mkdtemp(prefix="bpaingest-data-local-")
    dest_path = os.path.join(tempdir, basename)

    # Make a copy
    # Returned Path and Tempdir need to be copies, as elsewhere they get removed
    shutil.copy2(file_path, tempdir)

    logger.debug("end download_legacy_local_file `%s' " % legacy_url)
    return tempdir, dest_path


def download_legacy_file(legacy_url, auth):
    logger.debug("start download_legacy_file `%s' " % legacy_url)
    if legacy_url and legacy_url.startswith("file:///"):
        return download_legacy_local_file(legacy_url)
    basename = unquote(legacy_url.rsplit("/", 1)[-1])
    tempdir = tempfile.mkdtemp(prefix="bpaingest-data-")
    path = os.path.join(tempdir, basename)
    archive_info = ApacheArchiveInfo(auth)

    # resolve URL
    resolved_url = archive_info.resolve_url(legacy_url)
    logger.info("Resolved `%s' to `%s'" % (legacy_url, resolved_url))
    if not resolved_url:
        logger.error("unable to resolve `%s' - file missing?" % (legacy_url))
        os.rmdir(tempdir)
        return None, None

    # check transfer space
    resolved_size = archive_info.get_size(legacy_url)
    if resolved_size is None:
        logger.error("unable to retrieve file size for `%s' " % (legacy_url))
    usage = shutil.disk_usage(tempdir)
    free_space = usage.free
    if resolved_size is not None and resolved_size > free_space:
        logger.error(
            "Not enough free space to download to %s from archive" % (tempdir,)
        )
        logger.error("Have %d free but needed %d" % (free_space, resolved_size))
        return None, None

    else:
        logger.info(
            "Space OK for transfer - Have %s - Require %s"
            % (
                bitmath.Byte(bytes=free_space)
                .best_prefix()
                .format("{value:.2f} {unit}"),
                bitmath.Byte(bytes=resolved_size)
                .best_prefix()
                .format("{value:.2f} {unit}"),
            )
        )

    # wget will resume downloads, which is a huge win when dealing with
    # mirrors that sometimes close connections. ugly, but pragmatic.
    wget_args = ["wget", "-q", "-c", "-t", "0", "-O", path]
    if auth:
        wget_args += ["--user", auth[0]]
        wget_args += ["--password", auth[1]]
    wget_args.append(resolved_url)
    status = subprocess.call(wget_args)
    if status != 0:
        logger.error("wget failed, returned %s" % (str(status)))
        logger.error("wget args were: %s" % (str(wget_args)))
        try:
            os.unlink(path)
        except OSError:
            logger.error("failed to unlink temp file")
        try:
            os.rmdir(tempdir)
        except OSError:
            logger.error("failed to remove temp directory")
        return None, None
    logger.debug("end download_legacy_file `%s' " % legacy_url)
    return tempdir, path


def reupload_resource(ckan, ckan_obj, legacy_url, parent_destination, auth=None):
    "reupload data from legacy_url to ckan_obj"
    logger.debug("start reupload_resource `%s' " % legacy_url)

    if legacy_url is None:
        logger.error("download from legacy archive URL failed - legacy_url not set")
        return

    stream = False
    transfer_mode = os.getenv("BPAINGEST_STREAM")
    if transfer_mode is not None and transfer_mode == "yes":
        logger.info(f"Streaming upload from legacy URL to S3: {legacy_url}")
        stream = True
    else:
        logger.info(f"Downloading from legacy URL: {legacy_url}")

    if stream:
        path = resolve_legacy_file(legacy_url, auth)
    else:
        tempdir, path = download_legacy_file(legacy_url, auth)

    if path is None:
        logger.error("download from legacy archive failed")
        return

    try:
        status = -1
        logger.info("re-uploading from: %s" % (path))
        # Always store in S3 with a clean filename
        filename = bpa_munge_filename(path.split("/")[-1])
        s3_destination = "s3://{}/resources/{}/{}".format(
            parent_destination, ckan_obj["id"], filename
        )

        logger.info(f"S3 destination is: {s3_destination}")

        if stream:
            logger.info("Streaming - get mirror file info...")
            headers = build_apache_headers_for_urllib3(auth)
            logger.debug("Headers for stream retrieval are {}".format(headers))
            logger.debug("using  url {}".format(legacy_url))
            http = urllib3.PoolManager()

            with http.request("GET", legacy_url, preload_content=False, headers=headers) as response:
                logger.debug("Response should be set (streaming)")

                file_size = response.headers.get("Content-length", None)
                logger.info("File size of legacy file is : {}".format(file_size))
                if file_size:
                    # calculate a 1000 part split, make the chunksize 5MB greater
                    calculated_chunksize = int(int(file_size)/1000) + (5*MB)  # default to 1000 chunks
                    logger.info("Initial chunksize value = {}".format(calculated_chunksize))
                    if int(file_size) > 100*GB:

                        calculated_chunksize = int(int(file_size) / 5000) + (5 * MB)  # go with more chunks?
                        logger.info("File size > 100Gb, using 5000 as the divisor, chunk size is {}".format(calculated_chunksize))

                    multipart_chunksize = max(20*MB, calculated_chunksize)
                    logger.info("Using chunksize of: {}".format(multipart_chunksize))
                else:
                    logger.warn("File size not able to be determined from legacy URL")
                    # YOLO
                    multipart_chunksize=1024*20

                logger.info("Streaming - set S3 session...")
                b3_config = Config(
                    retries={
                        'max_attempts': 10,
                        'mode': 'standard'
                    },
                    max_pool_connections=96,
                    # duration_seconds=7200  # 2 hours
                )
                stream_session = boto3.session.Session()
                s3_client = stream_session.client("s3", config=b3_config)
                s3_resource = stream_session.resource("s3")
                # set logging for boto3 and botocore: (commented out so as not to add too much to the ingest logs
                #boto3.set_stream_logger('boto3', logging.DEBUG)
                #boto3.set_stream_logger('botocore', logging.DEBUG)
                #boto3.set_stream_logger('')

                tf_config = TransferConfig(multipart_threshold=20*MB,  # this is irrelevant when chunksize is larger
                                        multipart_chunksize=multipart_chunksize,
                                        use_threads=True,
                                        max_concurrency=5)

            # Configure the progress bar
                bar = {"unit": "B", "unit_scale": True, "unit_divisor": 1024, "ascii": True}
                if file_size:
                    bar["total"] = int(file_size)
                else:
                    logger.warn("File size not able to be determined from legacy URL")

                content_length = 0
                # Chunk size of 1024 bytes

                key = s3_destination.split("/", 3)[3]

                bucket_name = parent_destination.split("/")[0]

                # validate bucket
                try:
                    logger.debug("Bucket in stream is {}".format(bucket_name))
                    bucket = s3_resource.Bucket(bucket_name)
                    logger.debug("Bucket in stream is {}".format(bucket))
                except ClientError as e:
                    logger.error("Error setting Bucket in stream {}".format(e))
                    bucket = None

                # handle pre-existing file case
                try:
                    # In case filename already exists, get current etag to check if the
                    # contents change after upload
                    head = s3_client.head_object(Bucket=bucket_name, Key=key)
                    logger.debug("Got the Head in the pre-check")
                except ClientError as e:
                    logger.debug("Failed getting the head, set etag blank - file not in S3, file will be created {}".format(e))
                    etag = ""
                else:
                    etag = head["ETag"].strip('"')
                    logger.debug("Did get the head, etag is {}".format(etag))

                # create the Object to represent the file
                try:
                    s3_obj = bucket.Object(key)
                except ClientError as e:
                    logger.error("ClientError when setting key: {}".format(e))
                    s3_obj = None
                except AttributeError as e:
                    logger.error("Attribute Error when setting key:: {}".format(e))
                    s3_obj = None
                try:
                    with tqdm.tqdm(**bar) as progress:
                        try:
                         # upload with progress bar
                            try:
                                logger.debug("about to try the s3 upload")
                                s3_client.upload_fileobj(response, bucket_name, key,
                                                         Callback=progress.update, Config=tf_config)
                                logger.debug("Done with the upload_fileobj")
                            except ClientError as e:
                                logger.error("ClientError when upload file object: {}".format(e))
                                # pass
                            except AttributeError as e:
                                logger.error("AttributeError when upload file object: {}".format(e))
                                # pass
                            except Exception as e:
                                logger.error("Generic Exception when upload file object: {}".format(e))
                            else:
                                logger.debug("waiting for the object to exist")
                                # wait for S3 Object to exist
                                try:
                                    logger.debug("TRY...waiting for the object to exist")
                                    s3_obj.wait_until_exists(IfNoneMatch=etag)
                                except WaiterError as e:
                                    logger.error("WaiterError while streaming to S3 {}".format(e))
                                else:
                                    logger.debug("ELSE..waiting for the object to exist")
                                finally:
                                    logger.debug("In the finally, waited or not, go and get the new head")
                                    head = s3_client.head_object(Bucket=bucket_name, Key=key)
                                    logger.debug("Head is: {}".format(head))
                                    content_length = head["ContentLength"]

                                    if content_length > 0 and content_length == int(file_size):
                                        logger.debug("Content length of {} is good, setting status to 0"
                                                     .format(content_length))
                                        status = 0
                                    else:
                                        logger.error("uploaded s3 file size {} not legacy file size {} "
                                                     .format(content_length, file_size))
                        except Exception as e:
                            logger.error("Exception in with response/part: {}".format(e))

                except Exception as e:
                    logger.error("Exception in status bar: {}".format(e))

                response.release_conn()  # regardless of the outcome, we should free up the connection.

                logger.debug("Got to the end of the Stream logic, status is {}".format(status))

        else:
            s3cmd_args = ["aws", "s3", "cp", path, s3_destination]
            status = subprocess.call(s3cmd_args)
            content_length = os.path.getsize(path)
            logger.debug("status after non-stream: {}".format(status))

        logger.debug("status before check: {}".format(status))

        if status == 0:
            # patch the object in CKAN to have full URL
            logger.debug("build the resource url using {} {} {} {}"
                         .format(ckan.address, filename, ckan_obj["package_id"], ckan_obj["id"]))
            logger.debug(parent_destination)
            resource_url = "{}/dataset/{}/resource/{}/download/{}".format(
                ckan.address, ckan_obj["package_id"], ckan_obj["id"], filename
            )
            logger.debug("resource_url is:{}".format(resource_url))
            logger.debug("updating the ckan resource with id {} with the size {}  and new URL {}"
                         .format(ckan_obj["id"], content_length, resource_url))
            ckan.action.resource_patch(
                id=ckan_obj["id"],
                url=resource_url,
                url_type="upload",
                size=content_length,
            )
            logger.debug("Update Complete")
        else:
            logger.error("upload failed: status {}".format(status))
            logger.error("Skipping applying audit tag to {}".format(s3_destination))
            raise Exception("Upload failed to S3")

        # if resource_patch throws an exception, we shouldn't get to
        # tagging the s3 resource

        # tag resource in S3:
        # - permit lifecycle rules
        # - storage audit

        tags = {
            "source": "bpaingest",
            "audit": AUDIT_VERIFIED,
        }

        logger.info("tagging resource : %s (%s)" % (s3_destination, tags))
        bucket = parent_destination.split("/")[0]
        key = "{}/resources/{}/{}".format(
            parent_destination.split("/", 1)[1], ckan_obj["id"], filename
        )

        status = update_tags(bucket, key, tags)

        # FIXME Fix handling of status response
        logger.debug(status)
        # if status != 0:
        #    logger.error("tagging failed: status {}".format(status))

    finally:
        if not stream:
            os.unlink(path)
            os.rmdir(tempdir)


def create_resource(ckan, ckan_obj):
    "create resource, uploading data from legacy_url"
    logger.debug("start create_resource - ckan resource create call")
    return ckan_method(ckan, "resource", "create")(**ckan_obj)


def get_organization(ckan, id):
    return ckan_method(ckan, "organization", "show")(id=id)


def ckan_get_from_dict(logger, ckan, dict):
    logger.debug("start ckan_get_from_dict (package search) ")
    fq = ApiFqBuilder.from_collection(logger, dict)
    ## keep search parameters as broad as possible (the raw metadata may be from different project/organization
    #     # ckan api will only return first 1000 responses for some calls - so set very high limit.
    #     # Ensure that 'private' is turned on
    search_package_arguments = {
        "rows": 10000,
        "start": 0,
        "fq": fq,
        "include_private": True,
    }
    ckan_result = {}
    try:
        ckan_wrapped_results = ckan.call_action(
            "package_search", search_package_arguments
        )
        if ckan_wrapped_results and ckan_wrapped_results["count"] == 1:
            result = ckan_wrapped_results["results"][0]
            ckan_result = {"package_id": result["id"]}
        else:
            raise Exception(
                f"Unable to retrieve single result for raw package search. Unfortunately, the solr query: {fq} returned {getattr(ckan_wrapped_results, 'count', 0)} results."
            )
    except Exception as e:
        logger.error(e)
        raise Exception(f"Error calling CKAN server")

    return ckan_result
