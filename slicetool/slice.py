#! /usr/bin/env python3
from slicetool.billing_meta import get_steps as billing_meta_steps
from slicetool.billing_billing import get_steps as billing_billing_steps
from slicetool.billingUi_meta import get_steps as billingUi_meta_steps
from slicetool.test import get_steps as test_steps
from slicetool.cli import parse_pull_args, Prindenter, Indent
from slicetool.mysql import Connection
import slicetool.db as Db

# accepts cli_args and a function to call which provides steps for syncing a slice from remote to downstream
def main(cli_args, get_steps, slice_name):

    printer = Prindenter(indent=0)

    printer('Syncing a {} slice from {}.{} to {}.{}'.format(
        slice_name, cli_args.upstream.host, cli_args.upstream.database,
                    cli_args.downstream.host, cli_args.downstream.database))

    with Indent(printer):
        printer('[Upstream connection parameters]')
        with Indent(printer):
            printer(cli_args.upstream.__dict__)

        printer('[Downstream connection parameters]')
        with Indent(printer):
            printer(cli_args.downstream.__dict__)

    printer("[Database configuration check]")
    with Connection(cli_args.upstream) as upstream_connection, Indent(printer):

        # collect database-level info into an object
        with Connection(cli_args.downstream) as downstream_connection:
            with downstream_connection.cursor() as downstream_cursor:
                with upstream_connection.cursor() as upstream_cursor:
                    db_pair = Db.Twin(upstream_cursor, downstream_cursor, printer=printer)

        db_pair.downstream.args = cli_args.downstream

        db_pair.upstream.args = cli_args.upstream
        db_pair.upstream.connection = upstream_connection


        printer("[Database sync]")
        with Indent(printer):
            # do the sync-steps for each table in the slice
            for table_name, sync_func in get_steps(db_pair, cli_args, printer=printer).items():
                printer(f'[Table: {table_name}]')
                with Indent(printer):
                    if sync_func:
                        sync_func()
                        printer("")
                    else:
                        with Indent(printer):
                            printer("skipped explicitly by slice definition")
                            printer("")

    printer('Done')
    printer.print_summary()

# used as entrypoint in setup.py
def billing_meta():
    cli_args = parse_pull_args('update a downstream (stale) billing-slice of meta with upstream freshness')
    main(cli_args, billing_meta_steps, "billing slice of meta")

# used as entrypoint in setup.py
def billing_billing():
    cli_args = parse_pull_args('update a downstream (stale) billing-slice of billing with upstream freshness')
    main(cli_args, billing_billing_steps, "billing slice of billing")

# used as entrypoint in setup.py
def billingUI_meta():
    cli_args = parse_pull_args('update a downstream (stale) billingUi-slice of meta with upstream freshness')
    main(cli_args, billingUi_meta_steps, "billingUI slice of meta")

# used as entrypoint in setup.py
def test():
    cli_args = parse_pull_args('update a downstream (stale) test database with upstream freshness')
    main(cli_args, test_steps, "test slice of things_upstream")

# called when this script is run directly
if __name__ == '__main__':
    test()
