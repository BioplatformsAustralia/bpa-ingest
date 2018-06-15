#!/usr/bin/env python3

import string
import json
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
        print('# {}: {}'.format(os.path.basename(fname), sheet.name))
        defns = []
        for cell in header:
            defns.append({
                'field_name': generate_name(cell),
                'label': cell,
                'python_type': 'str'
            })
        print('''{}_fields = [SchemaDatasetField(**t) for t in {}]'''.format(generate_name(sheet.name), json.dumps(defns, indent=4, sort_keys=True)))


if __name__ == '__main__':
    main()
