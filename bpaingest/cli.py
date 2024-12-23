import argparse
import logging
import sys
import os

from .util import (
    make_registration_decorator,
    make_ckan_api,
    make_reuploads_cache_path,
    validate_write_reuploads_interval,
)
from .sync import sync_metadata
from .schema import generate_schemas
from .ops import print_accounts, make_organization
from .dump import dump_state
from .util import make_logger
from .genhash import genhash as genhash_fn
from .projects import ProjectInfo
from .organizations import ORGANIZATIONS
from .metadata import DownloadMetadata

register_command, command_fns = make_registration_decorator()
project_info = ProjectInfo()
project_cli_options = project_info.cli_options()

LOG_LEVELS = {
    t: getattr(logging, t) for t in ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")
}


def make_cli_logger(args):
    return make_logger(args.project_name, level=LOG_LEVELS[args.log_level])


@register_command
def bootstrap(args):
    "bootstrap basic organisation data"
    ckan = make_ckan_api(args)
    for organization in ORGANIZATIONS:
        make_organization(ckan, organization)


# https://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse
def str2bool(v):
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def setup_ckan(subparser, required=True):
    # Possible future enhancement would be to grab these from environment variables
    # See approach in
    #   https://gist.github.com/bebosudo/8352d6ba4c8911528f085890becc01c4
    subparser.add_argument("-k", "--api-key", required=required, help="CKAN API Key")
    subparser.add_argument("-u", "--ckan-url", required=required, help="CKAN base url")
    subparser.add_argument(
        "--verify-ssl",
        required=required,
        type=str2bool,
        default=True,
        help="CKAN base url",
    )


def setup_sync(subparser):
    setup_ckan(subparser)
    subparser.add_argument(
        "project_name",
        choices=sorted(project_cli_options.keys()),
        help="path to metadata",
    )
    subparser.add_argument(
        "--uploads", type=int, default=4, help="number of parallel uploads"
    )
    subparser.add_argument(
        "--metadata-only",
        "-m",
        action="store_const",
        const=True,
        default=False,
        help="set metadata only, no data uploads",
    )
    subparser.add_argument(
        "--skip-resource-checks",
        action="store_const",
        const=True,
        default=False,
        help="skip resource checks",
    )
    subparser.add_argument(
        "--delete",
        action="store_const",
        const=True,
        default=False,
        help="enable package and resource deletion (dangerous: only enable after a dry-run)",
    )
    subparser.add_argument(
        "--write-reuploads",
        "-w",
        action="store_const",
        const=True,
        default=False,
        help="write reuploads to disk",
    )
    subparser.add_argument(
        "--write-reuploads-interval",
        "-i",
        type=int,
        help="write reuploads to disk interval when uploading resources (e.g., 100 means a reupload write occurs after 100 resources uploaded).",
    )
    subparser.add_argument(
        "--read-reuploads",
        "-r",
        action="store_const",
        const=True,
        default=False,
        help="read reuploads from disk",
    )
    subparser.add_argument(
        "--audit",
        action="store_const",
        const=True,
        default=False,
        help="verify and update audit tags on known resources",
    )
    subparser.add_argument(
        "-p", "--download-path", required=False, default=None, help="CKAN base url"
    )
    subparser.add_argument(
        "--update-orgs",
        action="store_const",
        const=True,
        default=False,
        help="add / update organizations from google sheet data",
    )
    subparser.add_argument(
        "--single-ticket",
        "-s",
        type=str,
        default=None,
        help="Process a single ticket only",
    )


def setup_hash(subparser):
    setup_ckan(subparser)
    subparser.add_argument(
        "project_name",
        choices=sorted(project_cli_options.keys()),
        help="path to metadata",
    )
    subparser.add_argument(
        "mirror_path",
        help="path to locally mounted mirror",
        nargs="?",
        default=os.environ.get("MIRROR_PATH"),
    )


def setup_dump(subparser):
    subparser.add_argument("filename", help="output target")
    subparser.add_argument("--dump-re", help="restrict dump by slug", default="")
    subparser.add_argument(
        "--sql-context", help="generate excel file from sql db if available"
    )
    subparser.add_argument("--validate-schema", help="validate schema if applicable")
    setup_ckan(subparser, required=False)


def setup_makeschema(subparser):
    subparser.add_argument("--dump-re", help="restrict dump by slug", default="")
    subparser.add_argument("--validate-schema", help="validate schema if applicable")


@register_command
def sync(args):
    """sync a project"""
    ckan = make_ckan_api(args)

    logger = make_cli_logger(args)
    kwargs = {
        "write_reuploads": args.write_reuploads,
        "read_reuploads": args.read_reuploads,
        "reuploads_path": make_reuploads_cache_path(logger, args),
        "write_reuploads_interval": validate_write_reuploads_interval(logger, args),
    }
    with DownloadMetadata(
        logger,
        project_cli_options[args.project_name],
        path=args.download_path,
    ) as dlmeta:
        sync_metadata(
            ckan,
            dlmeta.meta,
            dlmeta.auth,
            args.uploads,
            not args.metadata_only,
            not args.skip_resource_checks,
            args.delete,
            args.update_orgs,
            args.single_ticket,
            args.audit,
            **kwargs,
        )
        print_accounts()


@register_command
def makeschema(args):
    generate_schemas(args)


@register_command
def dumpstate(args):
    dump_state(args)


@register_command
def genhash(args):
    ckan = make_ckan_api(args)
    """
    verify MD5 sums for a local (filesystem mounted) mirror of the BPA
    data, and generate expected E-Tag and SHA256 values.
    """
    with DownloadMetadata(
        make_cli_logger(args),
        project_cli_options[args.project_name],
        path=args.download_path,
    ) as dlmeta:
        genhash_fn(ckan, dlmeta.meta, args.mirror_path, num_threads=4)
        print_accounts()


sync.setup = setup_sync
bootstrap.setup = setup_ckan
dumpstate.setup = setup_dump
genhash.setup = setup_hash
makeschema.setup = setup_makeschema


def version():
    import pkg_resources

    version = pkg_resources.require("bpaingest")[0].version
    print(
        """\
bpa-ingest, version %s

Copyright 2016 CCG, Murdoch University
License GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>.
This is free software: you are free to change and redistribute it.
There is NO WARRANTY, to the extent permitted by law."""
        % (version)
    )
    sys.exit(0)


def usage(parser):
    parser.print_usage()
    sys.exit(0)


def commands():
    for fn in command_fns:
        name = fn.__name__.replace("_", "-")
        yield name, fn, getattr(fn, "setup", None), fn.__doc__


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true", help="print version and exit")
    parser.add_argument(
        "-p", "--download-path", required=False, default=None, help="CKAN base url"
    )
    parser.add_argument(
        "--log-level", required=False, default="INFO", choices=LOG_LEVELS.keys()
    )

    subparsers = parser.add_subparsers(dest="name")
    for name, fn, setup_fn, help_text in sorted(commands()):
        subparser = subparsers.add_parser(name, help=help_text)
        subparser.set_defaults(func=fn)
        if setup_fn is not None:
            setup_fn(subparser)
    args = parser.parse_args()
    if args.version:
        version()
    if "func" not in args:
        usage(parser)
    logging.basicConfig(level=LOG_LEVELS[args.log_level])
    args.func(args)
