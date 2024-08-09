from ...tracking import GoogleDriveTrackMetadata


class BSDGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "BPASampleData"
    skip_tracking_rows = 4
