from ...tracking import GoogleDriveTrackMetadata


class FungiGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "Fungi"
    skip_tracking_rows = 4
