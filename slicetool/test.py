from collections import OrderedDict
from slicetool.cli import parse_pull_args, Prindenter, Indent, mysqldump_data_batches,\
                          mysqldump_data, mysqlload, show_do_query
from slicetool.mysql import Connection

import slicetool.sync as Sync


def get_steps(db_pair, cli_args, printer=Prindenter()):

    def sync(table, zoom_levels):
        return Sync.general(table, zoom_levels, db_pair, cli_args, printer=printer),

    steps = OrderedDict()

    steps['foo_tokens'] = lambda : pull_foo(db_pair, cli_args, printer = printer)
    steps['foo_ref']    = None # this table also handled by pull_foo
    steps['baz']        = lambda : sync('baz', [10])
    steps['bar']        = lambda : sync('bar', [100, 1])

    return steps


# this logic assumes that only certain rows in foo_ref and foo_tokens actually need to be synced
def pull_foo(db_pair, cli_args, printer=Prindenter()):

    # grab only the foo token indices that are relevant
    with db_pair.upstream.connection.cursor() as upstream_cursor:

        get_ids = '''select id, foo_token_id from foo_ref where name like 'relevant%';'''

        result = show_do_query(upstream_cursor, get_ids, printer=printer)
        foo_token_ids =  ', '.join([str(x['foo_token_id']) for x in result])
        foo_ref_ids =  ', '.join([str(x['id']) for x in result])

    # dump just those rows
    mysqldump_data(cli_args.upstream, 'foo_ref', 'id in ({});'.format(foo_ref_ids), printer=printer)
    mysqldump_data(cli_args.upstream, 'foo_tokens', 'id in ({});'.format(foo_token_ids), printer=printer)

    # clear old rows
    with Connection(db_pair.downstream.args) as downstream_connection:
        with downstream_connection.cursor() as cursor:

            show_do_query(cursor, 'truncate foo_ref;', printer=printer)
            show_do_query(cursor, 'truncate foo_tokens;', printer=printer)

    # load new rows
    mysqlload(cli_args.downstream, 'foo_ref', printer=printer)
    mysqlload(cli_args.downstream, 'foo_tokens', printer=printer)

    printer.append_summary("foo_ref : UP TO DATE for select rows")
    printer.append_summary("foo_tokens : UP TO DATE for select rows")

    printer("foo_tokens and foo_ref are up to date where it matters")
