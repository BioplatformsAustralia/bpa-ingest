# _*_ coding: utf-8 _*_
"""
Tool to manage the import of rows from Excel workbooks.

Pass a filename, a sheet_name, a mapping (fieldspec)
Fieldspec maps spread sheet column names to more manageable names. It provides
functions associated with each column type that must be used to massage the data found in the column.

It returns a iterator providing named tuples, each tuple contains key/value pairs, the keys
being the fist column of the fieldspec, the value are found in the column specisied in the second fieldspec field
as mangled by the provided method.
"""

import datetime
from collections import namedtuple, Counter, OrderedDict

import re
import os
import xlrd
import string

from ..util import make_logger


logger = make_logger(__name__)

SkipColumn = namedtuple("SkipColumn", ["column_name", "skip_all"])
skip_column_default = SkipColumn("column_name", False)
FieldDefinition = namedtuple(
    "FieldSpec", ["attribute", "column_name", "coerce", "optional", "units"]
)
field_definition_default = FieldDefinition("<replace>", "<replace>", None, False, None)


def make_field_definition(attribute, column_name, **kwargs):
    return field_definition_default._replace(
        attribute=attribute, column_name=column_name, **kwargs
    )


def make_skip_column(column_name, **kwargs):
    return skip_column_default._replace(column_name=column_name, **kwargs)


class ExcelWrapper(object):
    """
    Parse a excel file and yields namedtuples.
    fieldspec specifies the columns  to be read in, and the name
    of the attribute to map them to on the new type

    field_spec: list of FieldSpec named tuples
    file_name: workbook name
    sheet_name: sheet in workbook
    header_length: first number of lines to ignore
    column_name_row_index: row in which column names are found, typically 0
    """

    def __init__(
        self,
        field_spec,
        file_name,
        sheet_name=None,
        header_length=0,
        column_name_row_index=0,
        suggest_template=False,
        additional_context=None,
    ):
        self._log = []
        self.file_name = file_name
        self.header_length = header_length
        self.column_name_row_index = column_name_row_index
        self.field_spec = field_spec
        assert isinstance(self.field_spec[0], FieldDefinition)
        self.additional_context = additional_context
        self.suggest_template = suggest_template

        self.workbook = xlrd.open_workbook(file_name)
        if sheet_name is None:
            self.sheet = self.workbook.sheet_by_index(0)
        else:
            self.sheet = self.workbook.sheet_by_name(sheet_name)

        self.field_names = self._set_field_names()
        self.missing_headers = []
        self.header, self.name_to_column_map = self.set_name_to_column_map()
        self.name_to_func_map = self.set_name_to_func_map()

    def _error(self, s):
        self._log.append(s)

    def get_errors(self):
        return self._log.copy()

    def _set_field_names(self):
        defs = [t for t in self.field_spec if isinstance(t, FieldDefinition)]
        names = list([spec.attribute for spec in defs])
        if len(list(names)) != len(defs):
            # this is a problem in the bpa-ingest code, not in the passed-in spreadsheet,
            # so we can fail hard here
            raise Exception(
                "duplicate attribute in field definition: %s"
                % [t for (t, c) in Counter(names).items() if c > 1]
            )
        return names

    def set_name_to_column_map(self):
        """
        maps the named field to the actual column in the spreadsheet
        """

        def coerce_header(s):
            if not isinstance(s, str):
                self._error("header is not a string: %s `%s'" % (type(s), repr(s)))
                return str(s)
            return s.strip()

        header = [
            coerce_header(t).strip().lower()
            for t in self.sheet.row_values(self.column_name_row_index)
        ]

        def find_column(column_name):
            # if has the 'match' attribute, it's a regexp
            if hasattr(column_name, "match"):
                return find_column_re(column_name)
            col_index = -1
            try:
                col_index = header.index(column_name.strip().lower())
            except ValueError:
                pass
            return col_index

        def find_column_re(column_name_re):
            for idx, name in enumerate(header):
                if column_name_re.match(name):
                    return idx
            return -1

        def find_all_columns_re(column_name_re):
            all_idx = []
            if not hasattr(column_name_re, "match"):
                raise Exception("Column name must be a regex for find all")
            for idx, name in enumerate(header):
                if column_name_re.match(name):
                    all_idx.append(idx)
            return all_idx

        cmap = {}
        skip_columns = set()

        missing_columns = False
        for spec in self.field_spec:
            if isinstance(spec, SkipColumn):
                if spec.skip_all:
                    skip_columns.update(find_all_columns_re(spec.column_name))
                else:
                    col_index = find_column(spec.column_name)
                    skip_columns.add(col_index)
                continue

            col_index = -1
            col_descr = spec.column_name
            if hasattr(spec.column_name, "match"):
                col_descr = spec.column_name.pattern
            if isinstance(spec.column_name, tuple):
                for c, _name in enumerate(spec.column_name):
                    col_index = find_column(_name)
                    if col_index != -1:
                        break
            else:
                col_index = find_column(spec.column_name)

            if col_index != -1:
                cmap[spec.attribute] = col_index
            else:
                self.missing_headers.append(spec.column_name)
                if not spec.optional:
                    self._error(
                        "Column `{}' not found in `{}' `{}'".format(
                            col_descr, os.path.basename(self.file_name), self.sheet.name
                        )
                    )
                    missing_columns = True
                cmap[spec.attribute] = None

        mapped_columns = set(cmap.values())
        unmapped_columns = []
        for idx, s in enumerate(header):
            if s != "" and idx not in mapped_columns and idx not in skip_columns:
                unmapped_columns.append(idx)
                self._error(
                    "Column `{}' not mapped to an output field in `{}` `{}`".format(
                        s, os.path.basename(self.file_name), self.sheet.name
                    )
                )
        if (len(unmapped_columns) > 0 or missing_columns) and self.suggest_template:
            self.print_template(header)
        return header, cmap

    def print_template(self, header):
        acceptable = set(string.ascii_letters + string.digits + "_")
        skip_fields = (
            # these are unknown
            "id",
            "tax_id",
            # portal is authoritative on these
            "ncbi_submission",
            "ncbi_bioproject",
            "ncbi_sample_accession",
        )
        float_fields = ("latitude", "longitude", "depth")

        def get_field_name(s):
            s = s.lower()
            # delete any (...) comment off the end
            if "(" in s:
                s = s[: s.index("(")]
            # delete any [...] comment off the end
            if "[" in s:
                s = s[: s.index("[")]
            s = s.strip()
            s = s.replace(" ", "_")
            s = s.replace("/", "_")
            s = "".join(t for t in s if t in acceptable)
            s = s.strip("_")
            s = re.sub("_+", "_", s)
            if s == "bpa_id":
                return "sample_id"
            return s

        parens = OrderedDict([("]", "["), (")", "("),])
        exclude_units = ("yyyy-mm-dd", "hh:mm")

        def guess_units(s):
            s = s.strip()
            if s == "":
                return
            if "%" in s:
                return "%"
            last_char = None
            for p in parens:
                if p in s:
                    last_char = p
                    break
            if last_char is None:
                return
            first_char = parens[last_char]
            if first_char not in s:
                return
            units = s[s.index(first_char) + 1 : s.index(last_char)].strip()
            if "free text" in units:
                return
            if units in exclude_units:
                return
            if units == "":
                return
            return units

        template = ["["]
        indent = " " * 12
        for column in (str(t) for t in header):
            field_name = get_field_name(column)
            if field_name == "":
                continue
            if field_name in skip_fields:
                template.append("{}skip('{}'),".format(indent, column))
                continue
            args = [
                "'{}'".format(field_name),
                "'{}'".format(column),
            ]
            cleanup = None
            units = guess_units(column)
            if units is not None:
                args.append("units='{}'".format(units))
            if field_name == "sample_id":
                cleanup = "ingest_utils.extract_ands_id"
            elif field_name in float_fields or units is not None:
                cleanup = "ingest_utils.get_clean_number"
            elif "date" in field_name:
                cleanup = "ingest_utils.get_date_isoformat"
            elif "time" in field_name:
                cleanup = "ingest_utils.get_time"
            if cleanup is not None:
                args.append("coerce=" + cleanup)
            template.append("{}fld({}),".format(indent, ", ".join(args)))
        template.append("]")
        self._error(
            "{} @ {} - suggested template is:\n{}".format(
                self.file_name, self.sheet.name, "\n".join(template)
            )
        )

    def set_name_to_func_map(self):
        """ Map the spec fields to their corresponding functions """

        return dict(
            (t.attribute, t.coerce)
            for t in self.field_spec
            if isinstance(t, FieldDefinition)
        )

    def get_date_mode(self):
        assert self.workbook is not None
        return self.workbook.datemode

    def date_to_string(self, s):
        try:
            date_val = float(s)
            tpl = xlrd.xldate_as_tuple(date_val, self.workbook.datemode)
            return datetime.datetime(*tpl).strftime("%d/%m/%Y")
        except ValueError:
            return s

    def _get_rows(self):
        """ Yields sequence of cells """

        merge_redirect = {}
        for crange in self.sheet.merged_cells:
            rlo, rhi, clo, chi = crange
            source_coords = (rlo, clo)
            for rowx in range(rlo, rhi):
                for colx in range(clo, chi):
                    if rowx == rlo and colx == clo:
                        continue
                    merge_redirect[(rowx, colx)] = source_coords

        for row_idx in range(self.header_length, self.sheet.nrows):
            row = self.sheet.row(row_idx)
            merged_row = []
            for colx, val in enumerate(row):
                coord = (row_idx, colx)
                if coord in merge_redirect:
                    merge_row, merge_col = merge_redirect[coord]
                    merged_row.append(self.sheet.row(merge_row)[merge_col])
                else:
                    merged_row.append(val)
            yield merged_row

    def get_date_time(self, i, cell):
        """ the cell contains a float and pious hope, get a date, if you dare. """

        val = cell.value
        try:
            date_time_tup = xlrd.xldate_as_tuple(val, self.get_date_mode())
            # well ok...
            if (
                date_time_tup[0] == 0
                and date_time_tup[1] == 0
                and date_time_tup[2] == 0
            ):
                val = datetime.time(*date_time_tup[3:])
            else:
                val = datetime.datetime(*date_time_tup)
        except ValueError:
            self._error(
                "column: `%s' -- value `%s' cannot be converted to a date" % (i, val)
            )
        return val

    def get_all(self, typname="DataRow"):
        """Returns all rows for the sheet as namedtuple instances. Filters out any exact duplicates."""

        # row is added so we know where in the spreadsheet this came from
        typ_attrs = [n for n in self.field_names]
        if self.additional_context is not None:
            typ_attrs += list(self.additional_context.keys())
        typ = namedtuple(typname, typ_attrs)

        for row in self._get_rows():
            tpl = []
            for name in self.field_names:
                i = self.name_to_column_map[name]
                # i is None if the column specified was not found, in that case,
                # set the val to None as well
                if i is None:
                    tpl.append(None)
                    continue
                func = self.name_to_func_map[name]
                cell = row[i]
                ctype = cell.ctype
                val = cell.value
                # convert dates to python dates
                if ctype == xlrd.XL_CELL_DATE:
                    val = self.get_date_time(i, cell)
                if ctype == xlrd.XL_CELL_TEXT:
                    val = val.strip()
                # apply func
                if func is not None:
                    val = func(val)
                tpl.append(val)
            if self.additional_context:
                tpl += list(self.additional_context.values())
            yield typ(*tpl)
