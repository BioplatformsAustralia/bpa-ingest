# encoding: utf-8

# This code extracted from CKAN to enable bpa-ingest to match the
# behvaiour

# ckan/lib/io_.py
# ckan/lib/munge.p

# It is subject to the CKAN License

# It is open and licensed under the GNU Affero General Public License
# (AGPL) v3.0 whose full text may be found at:
#
#  http://www.fsf.org/licensing/licenses/agpl-3.0.html

# License
# +++++++
#
# CKAN - Data Catalogue Software
# Copyright (c) 2006-2018 Open Knowledge Foundation and contributors
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import sys
import six

import os.path
import re

from six import text_type

# Maximum length of a filename's extension (including the '.')
MAX_FILENAME_EXTENSION_LENGTH = 21

# Maximum total length of a filename (including extension)
MAX_FILENAME_TOTAL_LENGTH = 100

# Minimum total length of a filename (including extension)
MIN_FILENAME_TOTAL_LENGTH = 3

_FILESYSTEM_ENCODING = six.text_type(
    sys.getfilesystemencoding() or sys.getdefaultencoding()
)


def decode_path(p):
    u'''
    Convert a byte path string to a Unicode string.

    Intended to be used for decoding byte paths to existing files as
    returned by some of Python's built-in I/O functions.

    Raises a ``UnicodeDecodeError`` if the path cannot be decoded using
    the filesystem's encoding. Assuming the path was returned by one of
    Python's I/O functions this means that the environment Python is
    running in is set up incorrectly.

    Raises a ``TypeError`` if the input is not a byte string.
    '''

    if not isinstance(p, six.binary_type):
        raise TypeError(u'Can only decode str, not {}'.format(type(p)))
    return six.ensure_binary(p).decode(_FILESYSTEM_ENCODING)



def substitute_ascii_equivalents(text_unicode):
    # Method taken from: http://code.activestate.com/recipes/251871/
    """
    This takes a UNICODE string and replaces Latin-1 characters with something
    equivalent in 7-bit ASCII. It returns a plain ASCII string. This function
    makes a best effort to convert Latin-1 characters into ASCII equivalents.
    It does not just strip out the Latin-1 characters. All characters in the
    standard 7-bit ASCII range are preserved. In the 8th bit range all the
    Latin-1 accented letters are converted to unaccented equivalents. Most
    symbol characters are converted to something meaningful. Anything not
    converted is deleted.
    """
    char_mapping = {
        0xc0: 'A', 0xc1: 'A', 0xc2: 'A', 0xc3: 'A', 0xc4: 'A', 0xc5: 'A',
        0xc6: 'Ae', 0xc7: 'C',
        0xc8: 'E', 0xc9: 'E', 0xca: 'E', 0xcb: 'E',
        0xcc: 'I', 0xcd: 'I', 0xce: 'I', 0xcf: 'I',
        0xd0: 'Th', 0xd1: 'N',
        0xd2: 'O', 0xd3: 'O', 0xd4: 'O', 0xd5: 'O', 0xd6: 'O', 0xd8: 'O',
        0xd9: 'U', 0xda: 'U', 0xdb: 'U', 0xdc: 'U',
        0xdd: 'Y', 0xde: 'th', 0xdf: 'ss',
        0xe0: 'a', 0xe1: 'a', 0xe2: 'a', 0xe3: 'a', 0xe4: 'a', 0xe5: 'a',
        0xe6: 'ae', 0xe7: 'c',
        0xe8: 'e', 0xe9: 'e', 0xea: 'e', 0xeb: 'e',
        0xec: 'i', 0xed: 'i', 0xee: 'i', 0xef: 'i',
        0xf0: 'th', 0xf1: 'n',
        0xf2: 'o', 0xf3: 'o', 0xf4: 'o', 0xf5: 'o', 0xf6: 'o', 0xf8: 'o',
        0xf9: 'u', 0xfa: 'u', 0xfb: 'u', 0xfc: 'u',
        0xfd: 'y', 0xfe: 'th', 0xff: 'y',
        # 0xa1: '!', 0xa2: '{cent}', 0xa3: '{pound}', 0xa4: '{currency}',
        # 0xa5: '{yen}', 0xa6: '|', 0xa7: '{section}', 0xa8: '{umlaut}',
        # 0xa9: '{C}', 0xaa: '{^a}', 0xab: '<<', 0xac: '{not}',
        # 0xad: '-', 0xae: '{R}', 0xaf: '_', 0xb0: '{degrees}',
        # 0xb1: '{+/-}', 0xb2: '{^2}', 0xb3: '{^3}', 0xb4:"'",
        # 0xb5: '{micro}', 0xb6: '{paragraph}', 0xb7: '*', 0xb8: '{cedilla}',
        # 0xb9: '{^1}', 0xba: '{^o}', 0xbb: '>>',
        # 0xbc: '{1/4}', 0xbd: '{1/2}', 0xbe: '{3/4}', 0xbf: '?',
        # 0xd7: '*', 0xf7: '/'
    }

    r = ''
    for char in text_unicode:
        if ord(char) in char_mapping:
            r += char_mapping[ord(char)]
        elif ord(char) >= 0x80:
            pass
        else:
            r += str(char)
    return r


def munge_filename_legacy(filename):
    ''' Tidies a filename. NB: deprecated

    Unfortunately it mangles any path or filename extension, so is deprecated.
    It needs to remain unchanged for use by group_dictize() and
    Upload.update_data_dict() because if this routine changes then group images
    uploaded previous to the change may not be viewable.
    '''
    filename = substitute_ascii_equivalents(filename)
    filename = filename.strip()
    filename = re.sub(r'[^a-zA-Z0-9.\- ]', '', filename).replace(' ', '-')
    filename = _munge_to_length(filename, 3, 100)
    return filename


def munge_filename(filename):
    ''' Tidies a filename

    Keeps the filename extension (e.g. .csv).
    Strips off any path on the front.

    Returns a Unicode string.
    '''
    if not isinstance(filename, text_type):
        filename = decode_path(filename)

    # Ignore path
    filename = os.path.split(filename)[1]

    # Clean up
    filename = filename.lower().strip()
    filename = substitute_ascii_equivalents(filename)
    filename = re.sub(u'[^a-zA-Z0-9_. -]', '', filename).replace(u' ', u'-')
    filename = re.sub(u'-+', u'-', filename)

    # Enforce length constraints
    name, ext = os.path.splitext(filename)
    ext = ext[:MAX_FILENAME_EXTENSION_LENGTH]
    ext_len = len(ext)
    name = _munge_to_length(name, max(1, MIN_FILENAME_TOTAL_LENGTH - ext_len),
                            MAX_FILENAME_TOTAL_LENGTH - ext_len)
    filename = name + ext

    return filename


def _munge_to_length(string, min_length, max_length):
    '''Pad/truncates a string'''
    if len(string) < min_length:
        string += '_' * (min_length - len(string))
    if len(string) > max_length:
        string = string[:max_length]
    return string
