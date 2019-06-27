#! /usr/bin/env python3
from slicetool.cli import parse_pull_args, Prindenter, Indent, mysqldump_schema_nofk, mysqlload, show_do_query
from slicetool.mysql import Connection
import re
from collections import namedtuple, OrderedDict

class ColumnSchema:
    def __init__(self, prior_column, column_dict):
        self.after = prior_column
        self.field = column_dict['Field']
        self.default = column_dict['Default']
        self.null = column_dict['Null']
        self.type = column_dict['Type']

    def __repr__(self):
        return str(self.__dict__)

class TableSchema:
    def __init__(self, schema_dict):
        self.columns = []
        prev = None
        for x in schema_dict:
            schema = ColumnSchema(prev, x)
            self.columns.append(schema)
            prev = schema.field

    def __repr__(self):
        return str(self.columns)

ColumnChanges = namedtuple("ColumnChanges", "added deleted modified")

# return true if changes were made
def sync_schema(upstream_cursor, downstream_cursor, table_name, printer=Prindenter()):

    # collect schema changes for reporting
    report = ColumnChanges([], [], [])

    printer("[Examining up and downstream schemas for {}]".format(table_name))
    with Indent(printer):
        describe = 'describe {};'.format(table_name)
        up = TableSchema(show_do_query(upstream_cursor, describe, printer=printer))
        down = TableSchema(show_do_query(downstream_cursor, describe, printer=printer))

    up_columns = { x.field : x for x in up.columns }
    down_columns = { x.field : x for x in down.columns }

    add = { k:v for k,v in up_columns.items() if k not in down_columns }
    delete = { k:v for k,v in down_columns.items() if k not in up_columns }

    upstream_creates_q = "show create table {}".format(table_name)
    upstream_creates = show_do_query(upstream_cursor, upstream_creates_q)

    if add:
        with Indent(printer):
            printer("Adding {} columns downstream".format(len(add)))

            for new_col, schema in add.items():

                create = next(filter(lambda x : re.search(new_col, x),
                    upstream_creates[0]['Create Table'].split('\n'))).strip(',').strip()

                add_query = "ALTER TABLE {} ADD COLUMN {} ".format(table_name, create)
                if schema.after:
                    add_query += "AFTER {}".format(schema.after)
                else:
                    add_query += "FIRST"
                add_query += ";"
                show_do_query(downstream_cursor, add_query)
                # TODO: import the new table values explicitly, rather than letting the sync catch them

                report.added.append(new_col)

    if delete:
        with Indent(printer):
            printer("Deleting {} columns downstream".format(len(delete)))
            for removed_col in delete.keys():
                drop_query = "ALTER TABLE {} DROP COLUMN {};".format(table_name, removed_col)
                show_do_query(downstream_cursor, drop_query)
                report.deleted.append(removed_col)

    down = TableSchema(show_do_query(downstream_cursor, describe, printer=printer))
    down_columns = { x.field : x for x in down.columns }

    # check for necessary modifications in upstream column order
    for up in up_columns.values():
        down = next(filter(lambda x : x.field == up.field, down_columns.values()))

        if up.field != down.field:
            raise ValueError("Unable to compare schemas for table {} column {} != {}".format(
                table_name,
                up.field,
                down.field))
        else:
            with Indent(printer):
                if up.after != down.after or up.default != down.default or up.null != down.null or up.type != down.type:
                    printer("Modifying column: {}".format(up.field))
                    printer("Old:\n {}".format(down))
                    printer("New:\n {}".format(up))

                    modify = next(filter(lambda x : re.search(up.field, x),
                        upstream_creates[0]['Create Table'].split('\n'))).strip(',').strip()

                    modify_query = "ALTER TABLE {} MODIFY COLUMN {} ".format(table_name, modify)
                    if up.after:
                        modify_query += "AFTER {}".format(up.after)
                    else:
                        modify_query += "FIRST"
                    modify_query += ";"
                    show_do_query(downstream_cursor, modify_query)

                    column_report = OrderedDict()
                    column_report["column"] = up.field
                    column_report["from"] = up.__dict__
                    column_report["to"] = down.__dict__
                    report.modified.append(column_report)
                else:
                    printer("Column: {} has no schema changes".format(up.field))

    return report

def pull_schema(args, upstream_connection, printer=Prindenter()):

    target_db = args.downstream.database

    with Connection(args.downstream) as downstream_connection:
        with downstream_connection.cursor() as cursor:
            show_tables = 'show tables;'
            result = show_do_query(cursor, show_tables, printer=printer)
            table_ct = len(result)

    if table_ct > 0:
        printer("{} is a nonempty downstream database. "
                "If you want me to create a new database in its place, you'll have to drop and create it yourself.".format(target_db))
                # if you'd rather I nuke it for you, you're trusting me too much

    else:

        tmp_file = 'schema_nofk.sql'

        # dump schema to a file
        mysqldump_schema_nofk(args.upstream, tmp_file, printer=printer)

        # load from a file
        mysqlload(args.downstream, tmp_file, printer=printer)

def main(args):
    printer = Prindenter(indent=0)

    printer('Pulling schema from {} to {}'.format(args.upstream.host, args.downstream.host))

    with Connection(args.upstream) as upstream_connection, Indent(printer):
        pull_schema(args, upstream_connection, printer=printer)

    printer('Done')

# used as entrypoint in setup.py
def pull():
    cli_args = parse_pull_args('Start with an empty downstream database and replace it with the frail empty shell of an upstream database (schema only, no foreign keys).')
    main(cli_args)

# called when this script is run directly
if __name__ == '__main__':
    pull()
