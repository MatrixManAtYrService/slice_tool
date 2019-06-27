from math import floor
from collections import namedtuple
from sortedcontainers import SortedDict
from slicetool.cli import Prindenter, Indent, show_do_query, pretty_shorten
import slicetool.table as Table
import slicetool.ids as Ids
import slicetool.constants as Constants

# One database
class One:
    def __init__(self, cursor, printer=Prindenter()):
        self.concat = get_group_concat(cursor, printer=printer)

# both database
class Twin:
    def __init__(self, upstream_cursor, downstream_cursor, printer=Prindenter()):

        printer("[Upstream db]")
        with Indent(printer):
            self.upstream = One(upstream_cursor, printer=printer)

        printer("[Downstream db]")
        with Indent(printer):
            self.downstream = One(downstream_cursor, printer=printer)

        self.concat = GroupConcat(min(self.downstream.concat.md5s, self.upstream.concat.md5s),
                                  min(self.downstream.concat.bytes, self.upstream.concat.bytes))

    # we discovered safe values earlier, re-assert them
    def reup_maxes(self, downstream_cursor, upstream_cursor, printer=Prindenter()):
        printer("[Setting group_concat_max_len to known-good value discovered in earlier session]")
        with Indent(printer):
            set_group_concat(upstream_cursor, self.upstream.concat.bytes, printer=printer)
            set_group_concat(downstream_cursor, self.downstream.concat.bytes, printer=printer)

# granular scanning relies on group_concat, which silently truncates the output once it reaches
# group_concat_max_len bytes long.
# Interrogate the target server to see how many rows we can get away with.
GroupConcat = namedtuple('GroupConcat', 'md5s bytes')
def get_group_concat(cursor, try_set=10000000 * 33, printer=Prindenter()):
    # 32 bytes for the md5 plus 1 for the comma times a million rows

    # limiting it here because I'd prefer too many small queries over a few monsters
    # that ties up the server with no gaps.  This may be unnecessarily cautious, go bigger at your own risk.

    # hell, this is all at your own risk

    printer("How many rows is {} willing to hash at a time?".format(cursor.connection.host))
    with Indent(printer):

        # try to ask for enough space for 1 million rows at a time
        printer("Asking for lots lof space...")
        result = show_do_query(cursor, "set session group_concat_max_len = {};".format(try_set), printer=printer)

        # but accept what we're given
        printer("Taking what we can get...")
        result = show_do_query(cursor, "show variables where Variable_name = 'group_concat_max_len';" , printer=printer)
        max_group_concat_bytes = int(result[0]['Value'])

        # and see how many rows that is
        printer("How many of these will fit?")
        result = show_do_query(cursor, "select length(concat(md5('foo'),',')) as md5_bytes;" , printer=printer);
        md5_bytes = int(result[0]['md5_bytes'])

        rows = floor(max_group_concat_bytes / md5_bytes)

    printer("{} is willing to hash {} rows at a time.".format(cursor.connection.host, rows))
    return GroupConcat(rows, max_group_concat_bytes)

def set_group_concat(cursor, value, printer=Prindenter()):
    return show_do_query(cursor, "set session group_concat_max_len = {};".format(value), printer=printer)

# walk the table and return any ids/intervals with diffs
# use granularity = 1 to return a generator for rows-with-diffs in the specified scope
# use granularity > 1 to return a generator for row-rangess-with-diffs in the specified scope
#def find_diffs(upstream_cursor, downstream_cursor, table, scope, granularity, printer=Prindenter()):
def find_diffs(upstream_cursor, downstream_cursor, table, scopes, granularity, condition=None, printer=Prindenter()):

    # what are we scanning?
    if granularity <= 1:
        scan = Table.md5_rows
        thing = "row"
    else:
        scan = Table.md5_row_ranges
        thing = "range"

    start = min([x.start for x in scopes])
    end = max([x.end for x in scopes])

    # translate inbound scopes into mysql conditions
    conditions=[]
    for scope in scopes:
        conditions.append(f"{table.id_col} BETWEEN {scope.start} AND {scope.end}")

    batched_conditions = []
    for batch in Ids.partition(Constants.batch_fingerprints, conditions):
        if condition:
            batched_condition = condition + " AND (" + " OR ".join(batch) + ")"
        else:
            batched_condition = " OR ".join(batch)
        batched_conditions.append(batched_condition)

    if condition:
        total_condition = condition + " AND (" + " OR ".join(conditions) + ")"
    else:
        total_condition = " OR ".join(conditions)

    num_batches = len(batched_conditions)

    # display diff-density visually
    def visualize(found_change):

        if found_change is not None:
            printer('!', end='')
        else:
            printer('.', end='')

        if visualize.col < 100:
            visualize.col += 1
        else:
            visualize.col = 0
            printer('') # carriage returns every 100

    total_shortened_condition = pretty_shorten(total_condition)[:-1]

    printer("[Generating {thing} fingerprint of size {granularity} where {total_shortened_condition}]".format(**vars()))
    for ct, conditions in enumerate(batched_conditions):
        with Indent(printer):
            printer("")
            printer(f"[ Batch {ct + 1} of {num_batches} ]")

            downstream_fingerprints = SortedDict()
            upstream_fingerprints = SortedDict()

            downstream_fingerprints.update(scan(downstream_cursor, table.downstream, conditions, granularity, printer=printer))
            upstream_fingerprints.update(scan(upstream_cursor, table.upstream, conditions, granularity, printer=printer))

            scanned = list(set(downstream_fingerprints.keys()).union(upstream_fingerprints.keys()))
            scanned.sort()

            scanned_num = len(scanned)
            printer("[Examining {scanned_num} {thing} fingerprints]".format(**vars()))
            with Indent(printer):

                # reset visualizer
                visualize.col=0

                # yield only things (range or row) with diff
                for address in scanned:
                    found_change = None
                    try:
                        downstream_fingerprint = downstream_fingerprints[address]
                        upstream_fingerprint = upstream_fingerprints[address]
                        if downstream_fingerprint != upstream_fingerprint:
                            found_change = address
                    except KeyError:
                        found_change = address
                    visualize(found_change)
                    if found_change is not None:
                        yield found_change
