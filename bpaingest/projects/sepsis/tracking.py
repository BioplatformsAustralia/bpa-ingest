from ...util import make_logger, csv_to_named_tuple
from ...libs import ingest_utils


logger = make_logger(__name__)


class SepsisTrackMetadata(object):
    def __init__(self, track_csv_path):
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('SepsisGenomicsMiseqTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    def get(self, bpa_id):
        track_meta = self.track_meta[bpa_id]
        return {
            'data_type': track_meta.data_type,
            'taxon_or_organism': track_meta.taxon_or_organism,
            'strain_or_isolate': track_meta.strain_or_isolate,
            'serovar': track_meta.serovar,
            'growth_media': track_meta.growth_media,
            'replicate': track_meta.replicate,
            'omics': track_meta.omics,
            'analytical_platform': track_meta.analytical_platform.strip(),
            'facility': track_meta.facility,
            'work_order': track_meta.work_order,
            'contextual_data_submission_date': track_meta.contextual_data_submission_date,
            'sample_submission_date': track_meta.sample_submission_date,
            'archive_ingestion_date': track_meta.archive_ingestion_date,
            'archive_id': track_meta.archive_id,
        }


class SepsisGenomicsTrackMetadata(SepsisTrackMetadata):
    def get(self, bpa_id):
        obj = super(SepsisGenomicsTrackMetadata, self).get(bpa_id)
        track_meta = self.track_meta[bpa_id]
        obj['growth_condition_notes'] = track_meta.growth_condition_notes
        return obj


# in the case of analysed data, this is the metadata coming over from the
# BPA Projects Data Transfer Summary on Google Drive, at least for now
# key is the CCG Jira Ticket column
class SepsisAnalysedTrackMetadata(object):
    def __init__(self, track_csv_path):
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('StemcellTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((t.ccg_jira_ticket.strip().lower(), t) for t in rows)

    def get(self, ticket):
        return self.track_meta[ticket.strip().lower()]
