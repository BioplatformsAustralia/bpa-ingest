from ...tracking import GoogleDriveTrackMetadata
from ...util import csv_to_named_tuple


class AGTrackMetadata(GoogleDriveTrackMetadata):
    name = "Grasslands"
    skip_tracking_rows = 4

    def read_track_csv(self, fname):
        prefix = "bioplatforms_"

        def remove_bioplatforms_prefix(s):
            if s.startswith(prefix):
                s = s[len(prefix) :]
            return s

        header, rows = csv_to_named_tuple(
            "GoogleDriveTrack",
            fname,
            name_fn=remove_bioplatforms_prefix,
            skip=self.skip_tracking_rows,
        )
        return dict((t.ccg_jira_ticket.strip().lower(), t) for t in rows)
