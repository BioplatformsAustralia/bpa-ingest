import os
from .util import make_logger, csv_to_named_tuple, one
from glob import glob


def get_track_dir(platform, project=None):
    if project is not None:
        rel_path = "../track-metadata/" + platform + "/" + project + "/"
    else:
        rel_path = "../track-metadata/" + platform + "/"
    return os.path.abspath(os.path.join(os.path.dirname(__file__), rel_path))


def get_track_csv(platform, glob_pattern, project=None):
    return one(glob(os.path.join(get_track_dir(platform, project), glob_pattern)))


class GoogleDriveTrackMetadata:
    platform = "google-drive"

    def __init__(self, logger):
        fname = get_track_csv(self.platform, "*" + self.name + ".csv")
        logger.info("Reading track CSV file: " + fname)
        if not hasattr(self, "skip_tracking_rows"):
            self.skip_tracking_rows = 0
        self.track_meta = self.read_track_csv(fname)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple(
            "GoogleDriveTrack", fname, skip=self.skip_tracking_rows
        )
        return dict((t.ccg_jira_ticket.strip().lower(), t) for t in rows)

    def get(self, ticket):
        return self.track_meta.get(ticket.strip().lower())
