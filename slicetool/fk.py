#! /usr/bin/env pythone
from slicetool.cli import parse_single_db_args, Prindenter, Indent, show_do_query
from slicetool.mysql import Connection
from slicetool.sync import TwinTable

def strip_fk(mysql_args, printer=Prindenter()):
    target_db = cli_args.database

    with Connection(mysql_args) as connection:
        with connection.cursor() as cursor:
            foreign_keys = '''
                           SELECT CONSTRAINT_NAME, TABLE_NAME
                           FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
                           WHERE CONSTRAINT_SCHEMA = '{}'
                           '''.format(target_db)
            result = show_do_query(cursor, foreign_keys, printer=printer)

            for row in result:
                fk = row['CONSTRAINT_NAME']
                table = row['TABLE_NAME']
                drop_constraint = '''
                                  ALTER TABLE {} DROP FOREIGN KEY {};
                                  '''.format(table, fk)
                with Indent(printer):
                    result = show_do_query(cursor, drop_constraint, printer=printer)

def main(args):
    printer = Prindenter(indent=0)

    printer('Dropping foreign keys from database: {}'.format(args.database))
    strip_fk(args, printer=printer)
    printer('Done')

# used as entrypoint in setup.py
def strip():
    cli_args = parse_single_db_args('Take an existing database and remove its foreign keys')
    main(cli_args)

# called when this script is run directly
if __name__ == '__main__':
    strip()
