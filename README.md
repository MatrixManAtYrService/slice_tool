# Slicetool

--

Warning:  most of this code works, but it has been recently extracted from a different project, some cleanup is needed to ensure that the tests run and perhaps a few other things. It will have some polish soon, but for now expect some rough edges.

--

Slicetool is a framework for custom server-to-server database sync functions.  The primary question is this:  do you really need to transfer *all* the data?  If not, then this project might help you only transfer the data you need.
There are two main ways slicetool helps you transfer fewer data:

- **Custom Sync Functions** - If you only need certain rows from an otherwise large table, you can write a query to determine those rows and then ask slicetool to transfer only those rows.
- **Diff-Based Sync Function**  - If you want the whole (updated) table, and have an old version, slicetool can identify which rows have changed and then transfer only those rows.

# A Reinvented Wheel

Table replication is not a new idea.  There are more mature tools that will probably do a more efficient job.  Consider [mysql replication](https://dev.mysql.com/doc/refman/8.0/en/replication.html) or [pt-table-sync](https://www.percona.com/doc/percona-toolkit/LATEST/pt-table-sync.html) for instance.  If this project can be said to be necessary at all, it is only if:

 - The idea of running without your customary foreign keys doesn't scare you

AND(

 - Your user on the upstream server can query the data
 - Your user on the downstream server can create and modify databases
 - You are not able to use other, established replication strategies

)
OR

- Your use-case only demands that a subset of the actual tables be present / up-to-date

# Prerequisites

For conceptual prerequisites, see the reinvented wheel section.

Python dependencies are handled by setuptools, see the installation section.

Otherwise, you'll need mysql-client to be configured locally.  If you ever use a hostname like `localhost` you'll need mysql server running locally too.

# Installation

There are two ways, see:
 - [System Install](docs/system_install.md)
 - [virtualenv Install](docs/venv_install.md)

Some default credentials live in [cli.py](slicetool/cli.py), you may want to change them, or you can supply whatever you need at the command line each time.

# Usage

Slicetool is comprised of several commands.  They are listed [here](setup.py).  Each of them supports interactive help, so just run:

    the_command --help

To see how it works

# How it works

## Mapping table names to sync functions

If you examine [test.py](slicetool/test.py) or [billing_meta.py](slicetool/billing_meta.py) you will see a something like this:

    steps['table_a'] = lambda : custom_function(arg1, arg2)
    steps['table_b'] = None
    steps['table_c'] = lambda : sync('table_c', [1000,1])
    steps['table_d'] = lambda : sync('table_d', [100])

In this example, `table_a` is a table where we don't want to sync the entire table, so a custom function is provided by whoever customized this particular sync.  `table_b` is also synced by `custom_function()` so it is listed for readability, but since it specifies a get_steps function of None, nothing will actually be done.  Omitting it from the step list would be equivalent.

`table_c` and `table_d` have no custom sync function defined, so they use the general purpose one.  First, `sync()` pulls any rows whose id's are larger than than the max(id) of the downstream table. Then it scans the tables for changes and transfers only the rows that have changed.  The numerical parameters control the chunk size (in rows) for this scan.

## Custom scan zoomlevels

In the above example `table_c` is scanned first in 1000 row chunks.  Then, only the chunks with changes are scanned row-at-a-time.  Finally, only those rows are transferred.

`table_d` on the other hand, is scanned in 100 row chunks, and then those chunks are transferred directly--without further analysis.  This makes sense when changes are likely to be consecutive.

As you use slicetool you will notice which phases take the most time.  If you think too much time is being spent on row comparison, supply fewer chunk-sizes and have the smallest be a larger number.  If you think the transfer-time is unnecessarily high, consider reducing the smallest chunk size.  These parameters are also useful for expressing whether server CPU time or network bandwidth is more valueable in your case.

For details about this algorithm, a good place to start would be `general_sync()` in [sync.py](slicetool/sync.py)

## Rerun-friendly

In the event of a failure (say you loose power after nuking a row-range but before replacing it with updated data) your downstream database may end up with problems.  The general sync function will identify damage of this sort as a diff and sync new rows to fix it.  Because of this, you can adopt a when-in-doubt-just-rerun it attitude towards slicetool.

There are other errors (like a schema mismatch) that will reliably reoccur.  These will require human intervention.

# Gotchas

## Non-concat-friendly columns

At the core of the row-scan algorithm is a query like this:

    SELECT MD5(GROUP_CONCAT(row_fingerprint ORDER BY id)) AS range_fingerprint,
           row_group * 100 as range_begin,
           (row_group + 1) * 100 - 1 as range_end
    FROM
        (SELECT MD5(CONCAT(id,IFNULL(val, 'NULL'))) as row_fingerprint,
                FLOOR(id/100) as row_group,
                id as id
        FROM bar
        WHERE id >= 0 AND id <= 1003
        ORDER BY id) as r
    GROUP BY row_group;

Note that MySQL doesn't know how to concatenate an int and a NULL, so it is necessary to cast null values to something like "NULL" before the values are hashed and compared.  So far as I can tell this must be done in client code, coulmn at a time.  See `examine_columns()` in [sync.py](slicetool/sync.py) for more about how this works.

If you have column types that are unlikely to behave well when stringified and concatenated, you should check the above function to make sure that the default checks handle your cases.  Otherwise, you may need to add one.

## Schema changes

Slicetool uses pymysql to scan for changes, but transfer-wise, the heavy lifting is done by calling `mysqldump` through a bash shell.  This cuts out python as an unnecessary middleman in the transfer.  Any errors that would come up while loading the dump will halt the sync.  Typically, this happens when the upstream schema has changed.

If you have access to the migration script, consider running it and then resyncing.  If not, you can drop the downstream table, recreate it with the output of `show create <tablename>` against the upstream table, and then let slicetool fill the gap.

## Unique Keys

I've only seen this a few times, but since slicetool syncs tables in chunks, there is a possibility that even though the eventual state of the table *is* consistent with a unique constraint, the transitional state is not. Usually I just drop the constraint and rerun the sync.

## Live updates

If you are syncing from a continually updated source database, a change may occur during the sync process.  After a sync, `TABLE CHECKSUM` is run, and since the inbound change happened after the scan, slicetool will think that something went wrong with the sync.  In this case, a warning will be printed at the end of the run.

TODO: visualize the change density after a run (not just during) so that users can get a feel for the distribution of changes to a table.  Then they'll know whether this type of error is worth worrying about.

## Time Zones

If you have columns with timestamps, and your upstream/downstream database servers are in different time zones, then slice tool will indentify every row as having a change because the timestamps will fail to match.  To avoid this, ensure that both servers are on the same timezone.

Probably what you want is to append this to your mysql config file:

    default_time_zone='+00:00'

## Syncing Too Much

If there's a table that stores throttles or active-server indicators and your slice syncs it, then when you use the synced database you'll use those values too.  One way to avoid this is to add a custom "sync" function that nukes the throttles and resets the server indicators for you.

# Recommended Workflow

If you're interested in a database called `foo`, then reserve your local database `foo` for the real thing--foreign keys and all.  Then create two empty local databases: `foo0` and `foo1`.  Then use `pull_schema` to initialize them with the foreign-keyless schema.  Then pull your slice into `foo0` from the remote upstream target.  Then make a copy by pulling into `foo1` with `foo0` as a target.

    remote:    foo
                |
                |
                |
                v
    local:    foo0 --> foo1

Then, use foo1 in your application (be sure to do `show grants for user foo_user` (5.6) or `show grants for foo_user` (5.7+) and replicate the grants with the new database name.  This way you can quickly "undo" changes made to `foo1` by repeating the `foo0` -> `foo1` sync.  Also, if you want to re-sync your data, you can still work on `foo1` while `foo0` is being synced.  (A local-to-local sync still takes some time, but it's typically much faster than a drop-and-create.)

Having two databases is also useful if you want to take advantage of the `--lite` flag, which does its best to sync the data without bothering with full table scans (which take a long time).  The idea is that you do a lite sync to `foo0` periodically (maybe set up a cron job to do it once per hour).  You can't really trust the integrity of that sync, because it relies on every change also updating the `modified date` accordingly (and in the right time zone).  Then, when you actually *need* a good sync, disable the cron job and pull a slice without the `--lite` flag.  This will scan the tables and ensure a proper sync.  Running `--lite` periodically will have moved the bulk of the data, so your non-lite sync will only have to bother with the data that the lite sync missed.

If you always want to have one usable table, but are ok lagging up to 24 hours behind (this is my case), then you can have `foo0` and `foo1` take turns being the *sync-in-progress* database.  So on even days `foo0` would get periodic lite updates and then at midnight it would get a full update, at which time `foo1` would start getting the periodic updates, and `foo0` would be the table to work with.

# Testing

See [test.sh](test/test.sh) for a test workflow.  If you intend to run it, be sure to modify any credentials used in that file beforehand.  If you're running it on a mac, you'll also want to replace the calls to `md5sum` with just `md5` too.
