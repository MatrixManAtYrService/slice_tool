from collections import OrderedDict
from slicetool.cli import parse_pull_args, Prindenter, Indent, mysqldump_data_batches,\
                       mysqldump_data, mysqlload, show_do_query

from slicetool.mysql import Connection

import slicetool.sync as Sync

def get_steps(db_pair, cli_args, printer=Prindenter()):

    def sync(table, zoom_levels):
        return Sync.general(table, zoom_levels, db_pair, cli_args, printer=printer),

    steps = OrderedDict()
    steps ['foo']                = lambda : sync('foo', [ 1000, 50, 1 ])
    steps ['bar']                = lambda : sync('bar', [ 1000, 50 ])
    steps ['baz']                = lambda : sync('baz', [ 1 ])
    steps ['special']            = lambda : special(cli_args, db_pair, printer = printer)
    steps ['special_uri']      = None # This table also handled by special

    return steps

def pull_subset_special(cli_args, db_pair, printer=Prindenter()):

    printer("Partial-syncing special and special-uri")
    with Indent(printer):

        # grab latest special rows
        with db_pair.upstream.connection.cursor() as upstream_cursor:

            get_ids = '''select id, special_id from special_uri where uri like '%subset%';'''

            result = show_do_query(upstream_cursor, get_ids, printer=printer)
            special_uri_ids =  ', '.join([str(x['id']) for x in result])
            special_ids =  ', '.join([str(x['special_id']) for x in result])

        mysqldump_data(cli_args.upstream, 'special_uri', 'id in ({});'.format(special_uri_ids), printer=printer)
        mysqldump_data(cli_args.upstream, 'special', 'id in ({});'.format(special_ids), printer=printer)

        # clear old rows
        with Connection(db_pair.downstream.args) as downstream_connection:
            with downstream_connection.cursor() as cursor:

                show_do_query(cursor, 'truncate special_uri;', printer=printer)
                show_do_query(cursor, 'truncate special;', printer=printer)

        # load latest rows
        mysqlload(cli_args.downstream, 'special_uri', printer=printer)
        mysqlload(cli_args.downstream, 'special', printer=printer)

    printer.append_summary("special_uri : Up to date for endpoints LIKE '%subset%'")
    printer.append_summary("special : Up to date for endpoints LIKE '%subset%'")
