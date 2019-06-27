import textwrap
import json
import sh
from collections import namedtuple, OrderedDict
from sortedcontainers import SortedDict

import slicetool.db as Db
import slicetool.ids as Ids
import slicetool.table as Table
import slicetool.constants as Constants
from slicetool.cli import Prindenter, Indent, mysqldump_data_batches, mysqldump_data, \
                          mysqlload, mysqldump_schema_nofk, show_do_query
from slicetool.mysql import Connection
from slicetool.schema import sync_schema

import IPython

# return value indicates whether data was actually transferred
def pull_missing_ids(table, db_pair, cli_args, batch_rows, condition=None, printer=Prindenter()):

    def make_space_downstream(printer):
        with Connection(db_pair.downstream.args) as downstream_connection:
            with downstream_connection.cursor() as cursor:
                if condition:
                    delete = f'delete from {table.name} where {table.id_col} > {table.upstream.max_id} and {condition};'
                else:
                    delete = f'delete from {table.name} where {table.id_col} > {table.upstream.max_id};'
                with Indent(printer):
                    result = show_do_query(cursor, delete, printer=printer)

    if table.downstream.max_id == table.upstream.max_id:
        printer("Nothing to sync")
        return False


    # check for downstream changes beyond max_id for upstream db and clobber them (this is a one-way sync)
    elif table.downstream.max_id > table.upstream.max_id:
        printer("Downstream db has more rows, deleting them.")
        make_space_downstream(printer)
    else:
        printer("Upstream db has more rows, pulling them.")

        # dump to a file
        if table.downstream.max_id == None or table.downstream.max_id == 0:
            # if the target table is empty, dump everything
            mysqldump_data_batches(cli_args.upstream,
                                   table.name,
                                   batch_rows,
                                   table.upstream.max_id,
                                   id_col=table.id_col,
                                   condition=condition,
                                   printer=printer)

        else:
            # otherwise, dump just the rows whose ids aren't in the target
            mysqldump_data_batches(cli_args.upstream,
                                   table.name,
                                   batch_rows,    # batch size
                                   table.upstream.max_id, # max id
                                   min_id=table.downstream.max_id + 1,
                                   id_col=table.id_col,
                                   condition=condition,
                                   printer=printer)

        printer("Making space downstream")
        make_space_downstream(printer)

        # load from a file
        printer("Loading updated rows")
        mysqlload(cli_args.downstream, table.name, printer=printer)

    return True

# syncs based on row cardinality using the first key
# if changes persist, groups by that key, sorts by the rest, and syncs based on md5 of md5's of rows in group
def multikey(table, db_pair, cli_args, keycolumns, condition=None, printer=Prindenter()):

    # given a query result for both up and downstream, sync the rows where check_col differs
    def group_sync(id_col, check_col, upstream, downstream, made_changes, printer=Prindenter()):

        group_fingerprints_by_id = {}

        def populate(name, query_result, key, store):
            for row in query_result:
                try:
                    store.setdefault(row[key], {})
                    store[row[key]][name] = row[check_col]
                except KeyError:
                    IPython.embed()

        populate('up', upstream, id_col, group_fingerprints_by_id)
        populate('down', downstream, id_col, group_fingerprints_by_id)

        to_delete = []
        to_write = []

        for id, stream in group_fingerprints_by_id.items():
            if 'up' not in stream:
                to_delete.append(id)
            elif 'down' not in stream:
                to_write.append(id)
            else:
                if stream['up'] != stream['down']:
                    to_delete.append(id)
                    to_write.append(id)

        if any(to_write):
            write_condition = f"{id_col} in ({','.join(map(str, to_write))})"
        else:
            write_condition = None

        if any(to_delete):
            delete_condition = f"{id_col} in ({','.join(map(str, to_delete))})"
        else:
            delete_condition = None

        if write_condition:
            printer(f"Found {str(len(to_write))} groups to pull down from upstream")
            made_changes = True
            mysqldump_data(cli_args.upstream,
                           table.name,
                           write_condition,
                           printer=printer)
        else:
            printer(f"Nothing to pull down from upstream")

        if delete_condition:
            printer("Making space downstream")
            made_changes = True
            with Indent(printer):
                with Connection(db_pair.downstream.args) as downstream_connection:
                    with downstream_connection.cursor() as cursor:
                        result = show_do_query(cursor, f'delete from {table.name} where {delete_condition};', printer=printer)
        else:
            printer(f"Downstream space is open for new data")

        if write_condition:
            # load from a file
            printer("Loading rows")
            mysqlload(cli_args.downstream, table.name, printer=printer)

    # group the table by 'top_key' and hash the groups
    def fingerprint_groups(cursor, table, top_key, sub_keys, printer=Prindenter()):

        printer(f"[ Fingerprinting {cursor.connection.db}.{table.name} grouped by {top_key} ]")
        with Indent(printer):

            all_columns = ",".join(table.upstream.columns)
            subkey_columns = ",".join(sub_keys)
            return show_do_query(cursor,
                                f"""
                                 SELECT {top_key},
                                     MD5(GROUP_CONCAT({all_columns}
                                         ORDER BY {subkey_columns})) AS group_fingerprint
                                 FROM {table.name}
                                 GROUP BY {top_key};
                                 """, printer=printer)

    made_changes = False

    # sync based on row cardinality
    ids = f"SELECT {keycolumns[0]}, count(*) as group_size FROM {table.name} group by 1;"

    with db_pair.upstream.connection.cursor() as upstream_cursor:
        upstream = show_do_query(upstream_cursor, ids, printer=printer)

        with Connection(db_pair.downstream.args) as downstream_connection:
            with downstream_connection.cursor() as downstream_cursor:
                downstream = show_do_query(downstream_cursor, ids, printer=printer)

    printer(f"[ Using {keycolumns[0]} as a key to sync missing rows on table {table.name} ]")
    with Indent(printer):
        group_sync(keycolumns[0], 'group_size', upstream, downstream, made_changes, printer=printer)


    # if changes persist, sync based on row contents

    with db_pair.upstream.connection.cursor() as upstream_cursor:
        with Connection(db_pair.downstream.args) as downstream_connection:
            with downstream_connection.cursor() as downstream_cursor:
                if table.is_synced(upstream_cursor, downstream_cursor, printer=printer):
                    return made_changes

                else:
                    upstream = fingerprint_groups(upstream_cursor, table, keycolumns[0], keycolumns[1:], printer=printer)
                    downstream = fingerprint_groups(downstream_cursor, table, keycolumns[0], keycolumns[1:], printer=printer)

                    printer(f"[ Using {keycolumns[0]} as a key to find mismatched data on table {table.name} ]")
                    with Indent(printer):
                        group_sync(keycolumns[0], 'group_fingerprint', upstream, downstream, made_changes, printer=printer)

                    return made_changes


# return value indicates whether data was actually transferred
def pull_modifications_since(date, table, column, db_pair, cli_args, condition=None, printer=Prindenter()):

    printer("syncing rows from {}.{} with {} newer than {}".format(db_pair.upstream.args.database, table.name, column, date))
    if condition:
        printer("... where {}".format(condition))
    with Indent(printer):
        with db_pair.upstream.connection.cursor() as upstream_cursor:
            if condition:
                newer_than_sql = f'select {table.id_col} from {table.name} where {column} > \'{date}\' and {condition};'
            else:
                newer_than_sql = f'select {table.id_col} from {table.name} where {column} > \'{date}\';'
            newer_than_result = show_do_query(upstream_cursor, newer_than_sql, printer=printer)
        if newer_than_result:
            ids_to_sync = [ x[table.id_col] for x in newer_than_result ]
            printer("Found {} such rows".format(len(ids_to_sync)))

            id_lists = Ids.partition(Constants.batch_conditions, ids_to_sync)
            conditions = []
            for ids in id_lists:
                ids_str = ",".join([str(x) for x in ids])
                conditions.append(f"{table.id_col} in ({ids_str})")

            with Indent(printer):
                printer("Proceeding in {} batches".format(len(conditions)))
                for condition in conditions:

                    # dump upstream data
                    mysqldump_data(cli_args.upstream, table.name, condition, printer=printer)

                    # clear old rows from downstream
                    delete = 'delete from {} where {};'.format(table.name, condition)
                    with Connection(db_pair.downstream.args) as downstream_connection:
                        with downstream_connection.cursor() as cursor:
                            show_do_query(cursor, delete, printer=printer)

                    # load new rows into downstream
                    mysqlload(cli_args.downstream, table.name, printer=printer)
            return True

        else:
            printer("No recent modifications found")
            return False


def identical(table, preposition, printer):
    table.needs_work = False
    message = "{} : IDENTICAL ({})".format(table.name, preposition)
    with Indent(printer):
        printer.append_summary(message) # only append a summary for this table if it's the final word
        printer(message)
    return table

def has_changes(table, preposition, printer):
    table.needs_work = True
    with Indent(printer):
        printer("{} : still HAS CHANGES ({})".format(table.name, preposition))
    return table

def has_changes_final(table, preposition, printer):
    table.needs_work = True
    message = "{} : still HAS CHANGES ({})".format(table.name, preposition)
    with Indent(printer):
        printer.append_summary(message) # only append a summary for this table if it's the final word
        printer(message)
    return table

def unknown(table, preposition, printer):
    table.needs_work = False
    message = "{} : UNVERIFIED ({})".format(table.name, preposition)
    with Indent(printer):
        printer.append_summary(message) # only append a summary for this table if it's the final word
        printer(message)
    return table

def pre_general(table_name, db_pair, cli_args, id_col, batch_rows, condition=None, printer=Prindenter()):

    # keep track of which syncs were performed
    presync_types = []

    # Check to see if work needs to be done
    with Connection(db_pair.downstream.args) as downstream_connection:
        with downstream_connection.cursor() as downstream_cursor:
            with db_pair.upstream.connection.cursor() as upstream_cursor:

                table = Table.Twin(table_name, downstream_cursor, upstream_cursor, id_col, printer=printer)

                table.try_sync_schema(upstream_cursor, downstream_cursor, throw=False, printer=printer)

                # TODO : move modified_time / last_touched checks into Table.Twin
                # before id_sync touches the table, get the downstream last modified time
                if '`modified_time`' in table.upstream.columns:
                    last_touched = Table.get_last_touched_date(table_name, '`modified_time`', db_pair.downstream, printer=printer)

                # pull latest based id
                if f'`{table.id_col}`' in table.upstream.columns:
                    printer(f"[syncing (on '{table.id_col}') table: {table.name}]")
                    with Indent(printer):
                        if pull_missing_ids(table, db_pair, cli_args, batch_rows, condition=condition, printer=printer):
                            presync_types.append("missing-id comparison")

                # pull latest based on modified time
                if '`modified_time`' in table.upstream.columns:
                    printer("[syncing (on 'modified_time') table: {}]".format(table_name))
                    with Indent(printer):
                        if pull_modifications_since(last_touched, table, 'modified_time', db_pair, cli_args, condition=condition, printer=printer):
                            presync_types.append("modified_time comparison")

                # prepare report.  What was done and where does that leave us?
                if not any(presync_types):
                    presync_types.append("not finding any changes")
                preposition = "after " + " & ".join(presync_types)

                if cli_args.lite:
                        reportfunc = unknown
                        printer("Skipped interim equality check due to lite mode")
                else:
                    printer("[Interim equality check for table {}]".format(table_name))
                    if table.is_synced(upstream_cursor, downstream_cursor, printer=printer):
                        reportfunc = identical
                    else:
                        reportfunc = has_changes

                return reportfunc(table, preposition, printer=printer)

# for use with composite keys
# groups by the first one, syncs first based on group size, then scans row ranges for data changes
def composite_key_sync(table_name, db_pair, cli_args, keys, condition=None, printer=Prindenter()):

    if condition:
        printer("WARNING, use of 'condition' here is untested")

    # Check to see if work needs to be done
    with Connection(db_pair.downstream.args) as downstream_connection:
        with downstream_connection.cursor() as downstream_cursor:
            with db_pair.upstream.connection.cursor() as upstream_cursor:

                table = Table.Twin(table_name, downstream_cursor, upstream_cursor, keys[0], printer=printer)

                table.try_sync_schema(upstream_cursor, downstream_cursor, throw=False, printer=printer)

                # do not assume that the use_col is a primary key--it may not be
                delattr(table, 'id_col')
                delattr(table.upstream, 'id_col')
                delattr(table.downstream, 'id_col')

                syncs_completed = []

                if table.is_synced(upstream_cursor, downstream_cursor, printer=printer):
                    return identical(table, "not finding any changes", printer=printer)

                multikey(table, db_pair, cli_args, keys, condition=condition, printer=printer)
                syncs_completed.append('multikey sync ')

                if table.is_synced(upstream_cursor, downstream_cursor, printer=printer):
                    return identical(table, f"after {','.join(syncs_completed)}", printer=printer)

                return has_changes_final(table, f"after {','.join(syncs_completed)}, "
                                                "because this function is not fully implemented", printer=printer)

# original caller will provide zoom_levels like: [100,10,1] and a table like "foo_table"
# then we examine the table and replace it with Table.Twin (see above)
# then we scan the whole thing and recurse with zoom_levels like :

#                                             {   1 : None,
#                                                10 : None,
#                                               100 : None }
#                                              7651 : [(0-7651)] } # suppose a max_row_is of 7651

# then we recurse and provide zoom_levels like: {   1 : None,
#                                                10 : None,
#                                               100 : [(0-99), (700-799)],
#                                              7651 : [(0-7651)] }

# then we recurse and provide zoom_levels like: { 1 : None,
#                                              10 : [(0-9), (60-69), (770-779)],
#                                             100 : [(0-99), (700-799)],
#                                            7651 : [(0-7651)] }

# then we recurse and provide zoom_levels like: { 1 : [1, 3, 65, 66, 67, 772]
#                                              10 : [(0-9), (60-69), (770-779)],
#                                             100 : [(0-99), (700-799)],
#                                            7651 : [(0-7651)] }

# then we see that there are no 'None' rows, so we stop recursing and just sync id's: [1, 3, 65, 66, 67, 772]
def general(table, zoom_levels, db_pair, cli_args, id_col='id', batch_rows=Constants.batch_rows, condition=None, printer=Prindenter()):

    # prepare for recursion if not already in it

    if type(table) == str:
        printer("[Examining table: {}]".format(table))
        with Indent(printer):
            try:
                table = pre_general(table, db_pair, cli_args, id_col, batch_rows, condition=condition, printer=printer)
            except sh.ErrorReturnCode_1 as err:

                # handle schema mismatches with a sledgehammer
                # TODO: allow user to provide path to migration scripts,
                # run outstanding ones if they show up in migration_tracker
                if "Column count doesn't match" in str(err):

                    printer("Upstream schema differs, pulling it down")

                    with Indent(printer):
                        # get upstream schema
                        filename='newschema_{}.sql'.format(table)
                        mysqldump_schema_nofk(cli_args.upstream,
                            filename, restrict_to_table=table, printer=printer)

                        # drop downstream table
                        drop = 'drop table {};'.format(table)
                        with Connection(db_pair.downstream.args) as downstream_connection:
                            with downstream_connection.cursor() as downstream_cursor:
                                show_do_query(downstream_cursor, drop, printer=printer)

                        # recreate downstream table
                        mysqlload(cli_args.downstream, filename, printer=printer)

                    # try again
                    printer("[New schema loaded, downstream table is empty]")
                    table = pre_general(table, db_pair, cli_args, id_col, condition=condition, printer=printer)
                else:
                    raise

    if type(zoom_levels) == list:
        # set up for recursion
        if table.needs_work:
            printer("Sync: 'general' received magnification list instead of zoom_level map, building zoom_level map...", end='')
            with Indent(printer):
                # prepare the zoom-level map
                zoom_levels = SortedDict({ x : None for x in zoom_levels })

                # append the outermost zoom level (completed in general)
                zoom_levels[table.upstream.max_id] = [ Ids.Interval(0,table.upstream.max_id) ]
        else:
            printer("Sync: 'general' finished early: presync was sufficient")
            return

        printer("done\n")

        # begin recursion
        printer("[Sync: 'general' top-level recursion]")
        with Indent(printer):
            return general(table, zoom_levels, db_pair, cli_args, condition=condition, printer=printer)

    # if control gets this far, recursion has begun

    granularity = None
    scopes = None
    # examine the scope map by decreasing magnification
    # find the transition from unknowns to knowns
    for ((smaller_granularity, smaller_scope), (larger_granularity, larger_scope)) \
            in reversed(list(zip(zoom_levels.items(), zoom_levels.items()[1:]))):

        if not smaller_scope:
            scopes = larger_scope             # we'll be filling these out
            granularity = smaller_granularity # by breaking them into pieces this big
            break

    if not scopes:
        printer("Zoom-level map fully populated, no more 'general' recursions will follow")

        conditions = []
        final_size = zoom_levels.keys()[0]

        final_scopes = list(zoom_levels.values()[0])
        final_scopes.sort()

        if final_size <= 1 and type(final_scopes[0]) == int:
            printer("Scanned down to individual rows")
            row_lists = Ids.partition(Constants.batch_fingerprints, final_scopes)

            for rows in row_lists:
                conditions.append("{} in ({})".format(table.id_col, ",".join([str(x) for x in rows])))

        elif final_size > 1 and isinstance(final_scopes[0], Ids.Interval):
            printer("Scanned down to row-ranges of size {}".format(final_size))
            interval_lists = Ids.partition(Constants.batch_fingerprints, final_scopes)

            conditions = []
            for intervals in interval_lists:
                conditions.append(
                    " OR ".join(["{} BETWEEN {} AND {}".format(table.id_col, i.start, i.end) for i in intervals]))

        else:
            raise ValueError("Can't decide whether to transfer rows, or row-ranges")

        printer("[Transfer proceeding in {} batches]".format(len(conditions)))
        with Indent(printer):

            for condition in conditions:

                # dump upstream data
                mysqldump_data(cli_args.upstream, table.name, condition, printer=printer)

                # clear old rows from downstream
                delete = 'delete from {} where {};'.format(table.name, condition)
                with Connection(db_pair.downstream.args) as downstream_connection:
                    with downstream_connection.cursor() as cursor:
                        show_do_query(cursor, delete, printer=printer)

                # load new rows into downstream
                mysqlload(cli_args.downstream, table.name, printer=printer)

        with Connection(db_pair.downstream.args) as downstream_connection:
            with downstream_connection.cursor() as downstream_cursor:
                with db_pair.upstream.connection.cursor() as upstream_cursor:
                    table.is_synced_warn(upstream_cursor, downstream_cursor, message='(after general sync)', printer=printer)
                    table.try_sync_schema(upstream_cursor, downstream_cursor, throw=True, printer=printer)

    # if we found a row with unpopulated scopes, then we have more scanning to do
    else:
        printer("[Given {} larger-granules, making smaller granules of size {} and fingerprinting them]".format(
                 len(scopes), granularity))
        next_scopes = []
        with Indent(printer):
            with Connection(db_pair.downstream.args) as downstream_connection:
                with downstream_connection.cursor() as downstream_cursor:
                    with db_pair.upstream.connection.cursor() as upstream_cursor:

                        # new sessions, reset group_concat (default is oddly low)
                        db_pair.reup_maxes(downstream_cursor, upstream_cursor, printer=printer)

                        #for scope in scopes:
                        #    next_scopes += list(Db.find_diffs(upstream_cursor, downstream_cursor, table, scope, granularity,
                        #                                       printer=printer))
                        # rather than making a round trip for each one, lets do them all at once

                        next_scopes += list(Db.find_diffs(upstream_cursor, downstream_cursor, table, scopes, granularity,
                                                          condition=condition, printer=printer))
                        printer('') # Db.find_diffs ends without a newline... add one

        # if no ranges were found to contain diffs
        if len(next_scopes) == 0: # note that any([0]) is False, but len([0]) == 0 is True
                                  # we want the latter, else we ignore row 0
            message = textwrap.dedent("""
            Found no ranges with diffs.  Nothing to do.
            If the tables were truly identical, TABLE CHECKSUM would have
            prevented sync from gettin this far.
            Perhaps some columns were ignored during the scan?
            (e.g. timestamps, as an ugly hack to avoid thinking about time zones)
            """)
            printer(message)
            printer.append_summary("{} : IDENTICAL? (TABLE CHECKSUM failed but a custom MD5 scan found no diffs)".format(table.name))

        # if no ranges were found to contain diffs
        else:
            zoom_levels[granularity] = next_scopes
            printer("[Another 'general' recursion]")
            with Indent(printer):
                return general(table, zoom_levels, db_pair, cli_args, condition=condition, printer=printer)
