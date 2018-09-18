from ...util import make_logger, csv_to_named_tuple, common_values
from ...libs import ingest_utils
from ...tracking import GoogleDriveTrackMetadata, get_track_csv
from collections import namedtuple

logger = make_logger(__name__)


class SepsisTrackMetadata(object):
    def __init__(self, name):
        fname = get_track_csv('bpam', '*' + name + '*.csv', project='sepsis')
        self.track_meta = self.read_track_csv(fname)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('SepsisTrack', fname)
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    def get(self, bpa_id):
        if bpa_id not in self.track_meta:
            logger.debug("No %s metadata for %s" % (type(self).__name__, bpa_id))
            return {}
        track_meta = self.track_meta[bpa_id]
        return {
            'data_type': track_meta.data_type,
            'taxon_or_organism': track_meta.taxon_or_organism,
            'strain_or_isolate': track_meta.strain_or_isolate,
            'serovar': track_meta.serovar,
            'growth_media': track_meta.growth_media,
            'replicate': track_meta.replicate,
            'omics': track_meta.omics,
            'work_order': track_meta.work_order,
            'contextual_data_submission_date': track_meta.contextual_data_submission_date,
            'sample_submission_date': track_meta.sample_submission_date,
            'archive_ingestion_date': track_meta.archive_ingestion_date,
            'archive_id': track_meta.archive_id,
        }


class SepsisGenomicsTrackMetadata(SepsisTrackMetadata):
    def get(self, bpa_id):
        obj = super(SepsisGenomicsTrackMetadata, self).get(bpa_id)
        track_meta = self.track_meta.get(bpa_id)
        if track_meta:
            obj['growth_condition_notes'] = track_meta.growth_condition_notes
        return obj


class SepsisGoogleTrackMetadata(object):
    name = 'Antibiotic Resistant Pathogen'
    platform = 'google-drive'

    def __init__(self):
        fname = get_track_csv(self.platform, '*' + self.name + '*.csv')
        logger.info("Reading track CSV file: " + fname)
        self.track_meta = self.read_track_csv(fname)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('SepsisGoogleDriveTrack', fname)
        # Sepsis has multiple row for one ticket. See github issue -https://github.com/BioplatformsAustralia/bpa-archive-ops/issues/698
        return [row for row in rows]

    def get(self, ticket):
        # Get all fields with unique values
        common_track_meta = common_values([row._asdict() for row in self.track_meta if row.ccg_jira_ticket.strip().lower() == ticket.strip().lower()])
        # wrapping common values(dict) into a single object
        custom_obj_type = namedtuple("SepsisGoogleDriveTrackCommonMeta", common_track_meta.keys())
        return custom_obj_type(*(common_track_meta.values()))
