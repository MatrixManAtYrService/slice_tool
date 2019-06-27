#! /usr/bin/env python3
from slicetool.cli import parse_single_db_args, Prindenter, Indent, show_do_query
from slicetool.mysql import Connection
from slicetool.sync import TwinTable

def strip_uk(target_args, printer=Prindenter()):
    target_db = target_args.database

    with Connection(target_args) as connection:
        with connection.cursor() as cursor:
            unique_keys = '''
                          SELECT DISTINCT constraint_name, table_name
                          FROM information_schema.table_constraints
                          WHERE constraint_type = 'UNIQUE'
                          AND table_schema = '{}';
                           '''.format(target_db)
            result = show_do_query(cursor, unique_keys, printer=printer)

            for row in result:
                uk = row['constraint_name']
                table = row['table_name']
                drop_constraint = '''
                                  DROP INDEX {} ON {};
                                  '''.format(uk, table)
                with Indent(printer):
                    result = show_do_query(cursor, drop_constraint, printer=printer)

def main(args):
    printer = Prindenter(indent=0)

    printer('Dropping unique keys from database: {}'.format(target_args.database))
    strip_uk(args, args, printer=printer)
    printer('Done')

# used as entrypoint in setup.py
def strip():
    cli_args = parse_single_db_args('take an existing database and remove its unique keys')
    main(cli_args)

# called when this script is run directly
if __name__ == '__main__':
    strip()
