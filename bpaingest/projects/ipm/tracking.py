from ...tracking import GoogleDriveTrackMetadata


class IPMGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "IPM Omics"
    skip_tracking_rows = 4
