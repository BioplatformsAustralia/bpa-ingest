#!/usr/bin/env python3

import string
import xlrd
import sys
import os


def generate_name(fld):
    return ''.join(t for t in '_'.join(fld.lower().split('(')[0].split()) if t in string.ascii_letters + string.digits + '-_')


def main():
    fname = sys.argv[1]
    wb = xlrd.open_workbook(fname)
    for sheet in wb.sheets():
        header = sheet.row_values(0)
        print('''\
# generated from: {} ({})

from ...schema import SchemaDatasetField


''')
        print('# {}: {}'.format(os.path.basename(fname), sheet.name))
        defns = []
        for cell in header:
            defns.append('''\
{{
    'field_name': {},
    'label': {},
    'python_type': str,
}}'''.format(repr(generate_name(cell)), repr(cell)))
        print('''{}_fields = [SchemaDatasetField(**t) for t in [{}]]'''.format(generate_name(sheet.name), ', '.join(defns)))


if __name__ == '__main__':
    main()
