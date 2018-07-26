#!/usr/bin/env python3

import io
import sys
import csv
import json
import sqlite3
import argparse
from collections import OrderedDict

class CustomCSVDialect(csv.unix_dialect):
    skipinitialspace = True

def _extract_value(source, key, max_size, default):
    value = source.get(key, default)
    if value is None:
        value = default
    if not type(value) == str:
        value = str(value)
    return value[:max_size]

def print_table(entries, fields, max_cols=100, sep=' | '):
    fields = OrderedDict(fields)

    # Iterate to find the largest entries, initialize with label lengths
    col_sizes = {field: len(str(label)) for field, label in fields.items()}
    for entry in entries:
        for field in fields:
            current_max = col_sizes.get(field, 0)
            col_sizes[field] = max(current_max, len(str(entry.get(field, ''))))
    for field, size in col_sizes.items():
        col_sizes[field] = min(size, max_cols)

    number_of_seps = (len(fields) - 1)
    width = sum(col_sizes.values()) + (number_of_seps * len(sep))

    value_formats = ['{{:<{}}}'.format(col_sizes[field]) for field in fields.keys()]
    template = sep.join(value_formats)

    # Display header
    truncated_headers = map(lambda h: h[:max_cols], fields.values())
    print(template.replace('<', '^').format(*truncated_headers))
    print('-' * width)
    for entry in entries:
        values = [_extract_value(entry, field, max_cols, '') for field in fields.keys()]
        print(template.format(*values))

def parse_data(source):
    if source == '-':
        content = sys.stdin.read()
    else:
        with open(source, 'r') as f:
            content = f.read()

    cols = set()
    try:
        data = json.loads(content)
        for row in data:
            cols.update(row.keys())
        cols = list(cols)
        cols.sort()
    except Exception as e:
        data_stream = io.StringIO(content)
        parsed_csv = csv.DictReader(data_stream, dialect=CustomCSVDialect)
        cols = parsed_csv.fieldnames
        data = list(parsed_csv)
    return cols, data

class TempDB():
    TABLE_NAME = 'data'

    def row_factory(self, cursor, row):
        """alters sqlite row loading to turn records into dictionaries"""
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def __init__(self, records, columns):
        self.records = records
        self.columns = columns

    def __enter__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = self.row_factory
        self.cursor = self.db.cursor()
        self._make_table()
        self._insert_data()
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()

    def _escape(self, v):
        """ Not really escaping... strip quotes from column names for minimalistic protection"""
        return '"{}"'.format(v.replace('"', ''))

    def _make_table(self):
        field_definitions = [
            '{} TEXT'.format(self._escape(f)) for f in self.columns
        ]
        field_list = ', '.join(field_definitions)
        create_table_statement = """
            CREATE TABLE {} (
                {}
            );
        """.format(self.TABLE_NAME, field_list)
        self.cursor.execute(create_table_statement)
        self.db.commit()

    def _insert_data(self):
        col_list = ['{}'.format(self._escape(c)) for c in self.columns]
        param_list = ['?' for c in self.columns]
        insert_statement = """
            INSERT INTO "{}" ({})
            VALUES (
                {}
            )
        """.format(
            self.TABLE_NAME,
            ', '.join(col_list),
            ', '.join(param_list),
        )
        flattened_rows = [
            [ row.get(c, None) for c in self.columns ]
            for row in self.records
        ]
        self.cursor.executemany(insert_statement, flattened_rows)
        self.db.commit()

    def query(self, statement):
        self.cursor.execute(statement)
        result = self.cursor.fetchall()
        cols = [info[0] for info in self.cursor.description]
        return cols, result
    
def main():
    parser = argparse.ArgumentParser(description='Display a dataset in a table (json or csv)')
    parser.add_argument('data_source', nargs='?', default='-', help='Source for the data (defaults to stdin)')
    parser.add_argument('-m', '--max-cols', type=int, default=100, help='Maximum size of a column')
    parser.add_argument('-o', '--only-columns', nargs='*', help='List of columns to retain')
    parser.add_argument('-q', '--query', nargs='?', help='SQL query to run (using table "data")')

    args = parser.parse_args()
    cols, data = parse_data(args.data_source)
    if args.only_columns:
        cols = args.only_columns

    if args.query:
        db = TempDB(data, cols)
        with db:
            cols, data = db.query(args.query)

    print_table(
        data,
        fields=[(col, col) for col in cols],
        max_cols=args.max_cols
    )

if __name__ == '__main__':
    main()
