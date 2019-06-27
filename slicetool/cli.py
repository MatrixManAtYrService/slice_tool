import sys
import textwrap
import argparse
import subprocess
import os
import re
import datetime
from pprint import pformat
from sh import bash, awk, netstat, mysql
from slicetool.mysql import LocalArgs, RemoteArgs

# Cli Parsing Helpers
# ===================

# local socket will be used if hostname='localhost'
# examine local system to find it
def get_local_socket():

    # try to determine the mysql socket path
    local_socket = ""
    if "linux" in sys.platform:
        local_socket= str(awk(netstat('-ln'), '/mysql(.*)?\.sock/ { print $9 }')).strip()
    elif sys.platform == "darwin":
        local_socket= str(awk(netstat('-an'), '/mysql(.*)?\.sock/ { print $5 }')).strip()

    # if we don't find a file, make it a required parameter
    if not os.path.exists(local_socket):
        local_socket=None

    return local_socket

# post-process argparse for a single connection direction into local/remote mysql args
def aggregate_mysql(args, prefix='upstream_'): # or downstream_

    argdict = upstream_args = { 'user'     : getattr(args, prefix + 'user'),
                                'password' : getattr(args, prefix + 'password'),
                                'host'     : getattr(args, prefix + 'host'),
                                'database' : getattr(args, prefix + 'database'),
                                'socket'   : getattr(args, prefix + 'socket'),
                                'cipher'   : getattr(args, prefix + 'cipher') }

    if argdict['host'] == 'localhost':
        return LocalArgs(**upstream_args)
    else:
        return RemoteArgs(**upstream_args)

# post-process argparse into local/remote upstream & downstream args
# original fields remain
def aggregate_updown_mysql(args):

    args.upstream = aggregate_mysql(args, 'upstream_')
    if args.no_upstream_cipher:
        args.upstream.cipher = None

    args.downstream = aggregate_mysql(args, 'downstream_')
    return args

# Parsing Command Line Aguments
# =============================

def parse_pull_args(desc=None):

    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--upstream-user')
    parser.add_argument('--upstream-password')
    parser.add_argument('--upstream-host',        default='db-usprod-shard0.corp.clover.com')
    parser.add_argument('--upstream-database',    default='meta')
    parser.add_argument('--upstream-socket',      default=get_local_socket())
    parser.add_argument('--upstream-cipher',      default='DHE-RSA-AES256-SHA',
                                                  help="to list available ciphers run: `openssl ciphers`")
    parser.add_argument('--no-upstream-cipher',   action='store_true')

    parser.add_argument('--downstream-user',      default='root')
    parser.add_argument('--downstream-password',  default='test')
    parser.add_argument('--downstream-host',      default='localhost')
    parser.add_argument('--downstream-database',  default='meta')
    parser.add_argument('--downstream-socket',    default=get_local_socket())
    parser.add_argument('--downstream-cipher',    default=None,
                                                  help="omit for no cipher")

    parser.add_argument('--lite',                 action='store_true', help='sync based on id and modified_time only (faster, but less reliable)')

    if len(sys.argv) < 2 :
        parser.print_help(sys.stderr)
        sys.exit(1)
    else:
        return aggregate_updown_mysql(parser.parse_args())

def parse_single_db_args(desc=None):

    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--user',      default='root')
    parser.add_argument('--password',  default='test')
    parser.add_argument('--host',      default='localhost')
    parser.add_argument('--database',  default='meta')
    parser.add_argument('--socket',    default=get_local_socket())
    parser.add_argument('--cipher',    default=None)

    if len(sys.argv) < 2 :
        parser.print_help(sys.stderr)
        sys.exit(1)
    else:
        return aggregate_mysql(parser.parse_args())

# Printing Messages to the Caller
# ===============================

# print status to stderr so that only the requested value is written to stdout
# (the better for consumption by a caller in code)
# default to a four-space indent
class Prindenter:
    def __init__(self, indent=4, file=sys.stderr):
        self.summary = []
        self.indent = indent
        self.at_line_begin = True
        self.file = file

    # for storing end-of-run report
    def append_summary(self, msg):
        self.summary.append("[{}] {}".format(str(datetime.datetime.now()), msg))

    # for printing: end-of-run report
    def print_summary(self):
        for msg in self.summary:
            self(msg)

    def __call__(self, msg, end='\n'):

        if self.at_line_begin:
            this_indent = self.indent
        else:
            this_indent =  0

        if end is '':
            self.at_line_begin = False
        else:
            self.at_line_begin = True

        print(textwrap.indent(msg.__str__(), ' ' * this_indent), file=self.file, end=end)

# Increments the intent depth for a Prindenter
class Indent:
    def __init__(self, printer):
        self.printer = printer

    def __enter__(self):
        self.printer.indent += 4

    def __exit__(self, type, value, traceback):
        self.printer.indent -= 4


# Executing External Commands
# ===========================

# print a bash command and its result
def run_in_bash(command,
                run=lambda cmd : bash(['-c', cmd]),
                printer=Prindenter()):

    with Indent(printer):
        printer('[Command]')
        with Indent(printer):
            printer(command)
            #printer(shorten(command))
        printer('[Output]')
        with Indent(printer):
            # execute and print output
            result = run(command)
            #printer(shorten(repr(result)))
            printer(repr(result))
    return result

def mysqldump_data(mysql_args, table_name, condition, append=False, printer=Prindenter()):

    outfile = table_name + '.sql'
    printer('[Dumping {} from {}.{} where {} into {}/{}]'.format(table_name,
                                                                 mysql_args.host,
                                                                 mysql_args.database,
                                                                 shorten(condition, length=20),
                                                                 os.getcwd(),
                                                                 outfile))

    # build command string
    format_args = { 'table'     : table_name,
                    'condition' : condition.replace('\n',''),
                    'file'      : outfile ,
                    'cipher'    : mysql_args.cipher }

    # if batch processing, append to file instead of making a new one
    if append:
        redirect = '>>'
    else:
        redirect = '>'

    format_args.update(mysql_args.__dict__) # use vars from slicetool.mysql.(Local|Remote)Args
    command = ' '.join(['mysqldump',
                        '--compress',
                        '-h{host}' if format_args['host'] != 'localhost' else '',
                        '-u{user}',
                        '-p\'{password}\'' if format_args['password'] else '',
                        '--ssl-cipher={cipher}' if format_args['cipher'] else '',
                        '{database}',
                        '{table}',
                        '--no-create-info',
                        '--lock-tables=false',
                        '--set-gtid-purged=OFF',
                        '--where=\'{condition}\'',
                        redirect, '{file}',
                       ]
                      ).format(**format_args)

    return run_in_bash(command, printer=printer)

# split a dump in pieces to avoid connection timeout issues
def mysqldump_data_batches(mysql_args, table_name, batch_size, max_id,
                                  min_id=0, id_col='id', condition=None, printer=Prindenter()):

    boundaries = list(range(min_id, max_id, batch_size))
    intervals = [ (x, x + batch_size - 1) for x in boundaries ]

    printer(f"[Dump proceeding across {len(intervals)} batches with size < {batch_size}]")
    with Indent(printer):

        first_batch = True
        for interval in intervals:

            # modify the condition for a smaller dump
            if condition:
                start = interval
                restricted_condition = f"{condition} and {id_col} >= {interval[0]} and {id_col} <= {interval[1]}"
            else:
                restricted_condition = f"{id_col} >= {interval[0]} and {id_col} <= {interval[1]}"

            # dump in append mode for all but first batch
            if first_batch:
                first_batch = False
                mysqldump_data(mysql_args,
                               table_name,
                               restricted_condition,
                               printer=printer)
            else:
                mysqldump_data(mysql_args,
                                      table_name,
                                      restricted_condition,
                                      append=True,
                                      printer=printer)

def mysqldump_schema_nofk(mysql_args, outfile, restrict_to_table=None, printer=Prindenter()):

    printer('[Dumping the schema without foreign keys '
            'from {}.{} into {}/{}]'.format(mysql_args.host,
                                            mysql_args.database,
                                            os.getcwd(),
                                            outfile))

    with Indent(printer):
        printer("You usually want those foreign keys.\n"
                "If you later use it for anything but slice testing,\n"
                "be sure to first rebuild this schema.")

    # build command string
    format_args = { 'file' : outfile }

    format_args['table'] = restrict_to_table

    format_args.update(mysql_args.__dict__) # use key-names from argparse
    command = ' '.join(['mysqldump',
                        '-h{host}' if format_args['host'] != 'localhost' else '',
                        '-u{user}',
                        '-p\'{password}\'' if format_args['password'] else '',
                        '--ssl-cipher={cipher}' if format_args['cipher'] else '',
                        '{database}',
                        '{table}' if format_args['table'] else '',
                        '--lock-tables=false',
                        '--set-gtid-purged=OFF',
                        '--no-data',

                        # see https://stackoverflow.com/a/50010817/1054322 for more about this
                        '|', 'sed', ''' '$!N;s/^\(\s*[^C].*\),\\n\s*CONSTRAINT.*FOREIGN KEY.*$/\\1/;P;D' '''
                        '|', 'grep', '-v', '\'FOREIGN KEY\'',

                        '>', '{file}',
                       ]
                      ).format(**format_args)

    return run_in_bash(command, printer=printer)


def mysqlload(mysql_args, table_or_file_name, printer=Prindenter()):

    # derive file name if table name was provided
    if re.match(r'.*\.sql$', table_or_file_name):
        infile = table_or_file_name
    else:
        infile = table_or_file_name + '.sql'

    printer('[Loading {} from {} into {}]'.format(infile,
                                                 '{}/{}'.format(os.getcwd(), infile),
                                                 mysql_args.database))

    # build command string
    format_args =  { 'file' : infile }
    format_args.update(mysql_args.__dict__) # use key-names from argparse
    command = ' '.join(['mysql',
                        '-h{host}' if format_args['host'] != 'localhost' else '',
                        '-u{user}',
                        '-p\'{password}\'' if format_args['password'] else '',
                        '--ssl-cipher={cipher}' if format_args['cipher'] else '',
                        '-D{database}',
                        '-e\'source {file};\'',
                       ]
                      ).format(**format_args)

    return run_in_bash(command,
                       printer=printer)

# constrain displayed output to a window of this size
max_line = 150
max_rows = 20

# trim str(data) by length only
def shorten(data, length=max_line):
    string = str(data)
    if len(string) <= length:
        return string
    else:
        return string[0:length] + '...'

# trim pretty-printed data (frequently multi-line) by line length and num-lines
def pretty_shorten(data, length=max_rows, width=max_line):

    output = ''

    if type(data) != str:
        pretty_string = pformat(data, indent=2)
    else:
        pretty_string = data

    for idx, line in enumerate(pretty_string.split('\n')):
        output+=shorten(line, length=width) + '\n'
        if idx > length:
            output+='...'
            break

    return output

# pretty printer for query execution
def show_do_query(cursor,
        query,
        do=lambda cursor, query: cursor.execute(query),
        get=lambda cursor: cursor.fetchall(),
        printer=Prindenter()):

    printer('[MySQL @ {}, database: {}]'.format(cursor.connection.host, cursor.connection.db))
    with Indent(printer):

        if type(query) == list:
            queries = query
        else:
            queries = [query]

        printer('[Queries]')
        with Indent(printer):
            for q in queries:
                    printer(pretty_shorten(textwrap.dedent(q), width=5000))
                    do(cursor, q)

        printer('[Result]')
        with Indent(printer):
            result = get(cursor)
            printer(pretty_shorten(result))

    return result
