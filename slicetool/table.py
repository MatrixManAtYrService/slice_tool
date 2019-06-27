import traceback
import json
from collections import OrderedDict
from slicetool.mysql import Connection
from slicetool.cli import Prindenter, Indent, show_do_query, pretty_shorten
from slicetool.schema import sync_schema
from slicetool.ids import Interval


# not all columns can be concatenated (i.e. NULL)
# this gets the list of columns and figures out how to make them concatenatabale
def examine_columns(cursor, table_name, printer=Prindenter()):

    printer(f"[Examining Columns on {cursor.connection.db}.{table_name}]")
    with Indent(printer):
        result = show_do_query(cursor,
               f"""
                SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_TYPE, COLLATION_NAME
                FROM information_schema.columns
                WHERE table_schema='{cursor.connection.db}'
                AND table_name='{table_name}';
                """,
                printer=printer)

        column_conversions= []

        for column in result:

            # make the column representation concatenate-friendly
            converted = f"`{column['COLUMN_NAME']}`"

            if column['IS_NULLABLE'] == 'YES':
                converted = f"IFNULL({converted}, 'NULL')"

            if column['COLLATION_NAME'] and column['COLLATION_NAME'] not in ['NULL', 'utf8_general_ci']:
                converted = f"BINARY {converted}"

            if 'binary(' in column['COLUMN_TYPE']:
                converted = f"hex({converted})"

            # your data may deviate in new and exciting ways
            # handle them here ...

            with Indent(printer):
                printer(converted)
            column_conversions.append(converted)

        return column_conversions

def show_create(cursor, table_name, printer=Prindenter()):

    printer(f"[Extracting creation SQL from {cursor.connection.db}.{table_name}]")
    with Indent(printer):
        result = show_do_query(cursor,
               f"""
                SHOW CREATE TABLE {table_name};
                """,
                printer=printer)

        return result[0]['Create Table'].strip()

def create_twin_if_not_exists(upstream_cursor, downstream_cursor, table_name, printer=Prindenter()):

    printer(f"[Checking for table existence: {downstream_cursor.connection.db}.{table_name}]")
    with Indent(printer):
        result = show_do_query(downstream_cursor,
               f"""
                SELECT *
                FROM information_schema.tables
                WHERE table_schema = '{downstream_cursor.connection.db}'
                    AND table_name = '{table_name}'
                LIMIT 1;
                """,
                printer=printer)

        if any(result):
            printer("It exists, moving on")
        else:
            printer("It does not exist, creating it")
            sql = show_create(upstream_cursor, table_name, printer=printer)
            result = show_do_query(downstream_cursor, sql, printer=printer)


# One side of a Twin (see below)
class One:
    def __init__(self, table_name, cursor, id_col, printer=Prindenter()):

        # Initialize values also found on Twin and that don't disagree between upstream and downstream
        self.id_col = id_col
        self.name = table_name

        # column descriptions with concatentate-friendly modifications
        self.columns = examine_columns(cursor, table_name, printer=printer)

        # how many rows?
        target = f'max({self.id_col})'
        printer(f"[Finding {target} for {cursor.connection.db}.{self.name}]")
        query = f'select {target} from {self.name};'
        with Indent(printer):
            result = show_do_query(cursor, query, printer=printer)
            self.max_id = result[0][target] or 0

# A table which exists both downstream and upstream, but may differ in data or host configuration
class Twin:
    def __init__(self, table_name, downstream_cursor, upstream_cursor, id_col, printer=Prindenter()):

        self.name = table_name
        self.id_col = id_col

        printer(f"[Upstream {table_name}]")
        with Indent(printer):
            self.upstream = One(table_name, upstream_cursor, id_col, printer=printer)

        create_twin_if_not_exists(upstream_cursor, downstream_cursor, table_name, printer=printer)

        # separate properties
        printer(f"[Downstream {table_name}]")
        with Indent(printer):
            self.downstream = One(table_name, downstream_cursor, id_col, printer=printer)

        self.successful_schema_sync = False # set true when sync completes

    def is_synced(self, upstream_cursor, downstream_cursor, printer=Prindenter()):
        with Indent(printer):
            get_checksum = f'checksum table {self.name};'

            result = show_do_query(upstream_cursor, get_checksum, printer=printer)
            upstream_checksum = result[0]['Checksum']

            result = show_do_query(downstream_cursor, get_checksum, printer=printer)
            downstream_checksum = result[0]['Checksum']

            if upstream_checksum != downstream_checksum:
                return False
            else:
                return True
                printer(f"{self.name} is identical on either side")

    # called when we're out of ideas, provide messages like: "(after taking 15 minutes syncing rows by MD5)"
    def is_synced_warn(self, upstream_cursor, downstream_cursor, message='', printer=Prindenter()):
        equality_found = self.is_synced(upstream_cursor, downstream_cursor, printer=printer)
        if not equality_found:
            summary = f"{self.name} : DIFFERS {message} (Attempts exhausted, were changes made during sync?)"
            # also, could there be a change re: time/date that was ignored by check_columns?

            printer.append_summary(summary)
        else:
            printer.append_summary(f"{self.name} : IDENTICAL {message}")

        return equality_found

    def report_if_schema_changed(table, report, printer):
        changes = False
        if report:
            summary = OrderedDict()
            if report.added:
                summary["additions"] = report.added
                changes = True
            if report.deleted:
                summary["removals"] = report.deleted
                changes = True
            if report.modified:
                summary["modifications"] = report.modified
                changes = True

            message = f"{table.name} : schema changed \n    {json.dumps(summary)}"
            if changes:
                printer.append_summary(message)
                printer(message)
        return changes

    def try_sync_schema(self, upstream_cursor, downstream_cursor, throw=True, printer=Prindenter()):

        printer(f"[Comparing upstream/downstream schemas for table: {self.name}]")
        with Indent(printer):

            def go(table, upstream_cursor, downstream_cursor, printer):
                schema_changes = sync_schema(upstream_cursor, downstream_cursor, self.name, printer=printer)
                if not Twin.report_if_schema_changed(self, schema_changes, printer):
                    table.successful_schema_sync = True

            if not self.successful_schema_sync:
                if throw:
                    go(self, upstream_cursor, downstream_cursor, printer)
                else:
                    try:
                        go(self, upstream_cursor, downstream_cursor, printer)
                    except Exception:
                        printer("Error occurred while syncing schema, but errors were suppressed")
                        printer("Will retry schema sync after data sync")
                        printer(traceback.format_exc())
            if self.successful_schema_sync:
                printer("...schemas are in sync".format(self.name))
            else:
                printer("...schemas are NOT in syc".format(self.name))

def md5_row_ranges(cursor, table, condition, granularity, printer=Prindenter()):

    if granularity <= 1:
        raise ValueError("Variable granularity scanner called, but a trivial granule size was provided")

    converted_columns_str = ",".join(table.columns)

    shortened_condition = pretty_shorten(condition)[:-1]

    printer(f"[ Fingerprinting {cursor.connection.db}.{table.name} in row-ranges of size {granularity}\n"
            f"  where {table.id_col} in {shortened_condition} ]")
    with Indent(printer):

        result = show_do_query(cursor,
                f"""
                SELECT MD5(GROUP_CONCAT(row_fingerprint ORDER BY id)) AS range_fingerprint,
                       row_group * {granularity} as range_begin,
                       (row_group + 1) * {granularity} - 1 as range_end
                FROM
                    (SELECT MD5(CONCAT_WS('|', {converted_columns_str})) as row_fingerprint,
                            FLOOR({table.id_col}/{granularity}) as row_group,
                            {table.id_col} as id
                    FROM {table.name}
                    WHERE {condition}
                    ORDER BY {table.id_col}) as r
                GROUP BY row_group;
                """, printer=printer)

        # organize fingerprints by interval
    return { Interval(row['range_begin'], row['range_end']) : row['range_fingerprint'] for row in result }


# fingerprint individual rows within multiple scopes for later comparison
def md5_rows(cursor, table, condition, granularity, printer=Prindenter()):

    if granularity > 1:
        raise ValueError("Individual row scanner called, but a nontrivial row-range size was provided")

    converted_columns_str = ",".join(table.columns)

    shortened_condition = pretty_shorten(condition)[:-1]
    printer(f"[ Fingerprinting each row in {cursor.connection.db}.{table.name}\n"
            f"  where {table.id_col} in {shortened_condition} ]")
    with Indent(printer):

        result = show_do_query(cursor,
                f"""
                    SELECT {table.id_col} as id, MD5(CONCAT_WS('|', {converted_columns_str})) as fingerprint
                    FROM {table.name}
                    WHERE {condition}
                    ORDER BY {table.id_col};
                """,
                printer=printer)

        return { row[table.id_col] : row['fingerprint'] for row in result }

# get a date for use in pul_modifications_since
def get_last_touched_date(table, column, db, printer=Prindenter()):
    printer(f"[ Finding most recent modification date from {db.args.host}.{db.args.database}.{table}.{column} ]")
    with Indent(printer):
        with Connection(db.args) as connection:
            with connection.cursor() as cursor:
                most_recent_sql = f'select max({column}) from {table};'
                most_recent = show_do_query(cursor, most_recent_sql, printer=printer)[0][f"max({column})"]
        printer(f"Found: {most_recent}")
    return most_recent
