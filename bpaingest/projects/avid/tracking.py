from ...tracking import GoogleDriveTrackMetadata


class AVIDGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "AVID"
    skip_tracking_rows = 4
