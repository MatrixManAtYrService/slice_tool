# On a server I know, remote connections get axed if they take too long
# pull this many rows per connection to fly under the radar
batch_rows = 100000

# mysqldump, as invoked through bash, fails if the command is too long
# limit the length of `where id between A and B or id between C and D and....`
batch_conditions = 1000

# server-side fingerprinting can be cpu intensive, smaller batches mean we give it time to breath in between
batch_fingerprints= 1000
