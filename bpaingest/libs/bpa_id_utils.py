# -*- coding: utf-8 -*-

import re

from ..util import make_logger

BPA_ID = "102.100.100"

logger = make_logger(__name__)


def get_bpa_id(bpa_idx, add_prefix=False):
    """
    Get a BPA ID, if it does not exist, make it
    It also creates the necessary project.
    :rtype : bpa_id
    """

    if add_prefix is True and bpa_idx is not None:
        bpa_idx = BPA_ID + '.' + bpa_idx

    validator = BPAIdValidator(bpa_idx)
    if validator.is_valid():
        return bpa_idx


class BPAIdValidator(object):
    """
    Given a BPA ID string, check validity.
    """

    RE_ID = re.compile(r"^102\.100\.100\.\d*", re.MULTILINE)

    def __init__(self, bpa_id):
        self.valid_report = None
        self.valid = None
        if bpa_id is not None:
            self.bpa_id = bpa_id.strip()
        else:
            self.bpa_id = None

    def get_id(self):
        """
        Return validated ID
        """
        return self.bpa_id

    def is_valid(self):
        if self.valid is None:
            self.is_valid_bpa_id()
        return self.valid

    def is_valid_bpa_id(self):
        """
        Determines if id is a valid BPA ID
        """

        if self.bpa_id is None:
            self.valid_report = 'BPA ID is None'
            self.valid = False

        # empties
        elif self.bpa_id == '':
            self.valid_report = 'BPA ID is empty string'
            self.valid = False

        # no BPA prefix
        elif self.bpa_id.find(BPA_ID) == -1:
            self.valid_report = 'No "{0}" identifying the string as a BPA ID'.format(BPA_ID)
            self.valid = False

        elif self.RE_ID.match(self.bpa_id) is None:
            self.valid_report = '{} does not match {}'.format(self.bpa_id, self.RE_ID.pattern)
            self.valid = False

        # this function has failed to find a reason why this can't be a BPA ID....
        else:
            self.valid = True
