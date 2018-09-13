import os
from .util import make_logger, csv_to_named_tuple, one
from glob import glob


logger = make_logger(__name__)


def get_track_dir(platform, project=None):
    if project is not None:
        rel_path = '../track-metadata/' + platform + '/' + project + '/'
    else:
        rel_path = '../track-metadata/' + platform + '/'
    return os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            rel_path))


def get_track_csv(platform, glob_pattern, project=None):
    return one(glob(os.path.join(get_track_dir(platform, project), glob_pattern)))


class GoogleDriveTrackMetadata(object):
    platform = 'google-drive'

    def __init__(self):
        fname = get_track_csv(self.platform, '*' + self.name + '*.csv')
        logger.info("Reading track CSV file: " + fname)
        self.track_meta = self.read_track_csv(fname)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('GoogleDriveTrack', fname)
        tm = dict((t.ccg_jira_ticket.strip().lower(), t) for t in rows)
        print(tm.get('bpaops-96'))
        tickets = []
        for t in rows:
            if t.ccg_jira_ticket in tickets:
                logger.warning("Multiple rows found with ticketID=%s in file %s" % (t.ccg_jira_ticket, fname))
                return Exception()
            tickets.append(t.ccg_jira_ticket)
        return dict((t.ccg_jira_ticket.strip().lower(), t) for t in rows)

    def get(self, ticket):
        return self.track_meta.get(ticket.strip().lower())

    # def get_list(self, ticket):
    #     fname = get_track_csv(self.platform, '*' + self.name + '*.csv')
    #     header, rows = csv_to_named_tuple('GoogleDriveTrack', fname)
    #     return [row for row in rows if row.ccg_jira_ticket.strip().lower() == ticket.strip().lower()]
