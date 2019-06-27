import pymysql

class LocalArgs:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class RemoteArgs:
    def __init__(self, **kwargs):
        kwargs.pop('socket', None)
        self.__dict__.update(kwargs)

# for use like so:

#     with Connection(args) as conn:
#         with conn.cursor() as cursor:
#             cursor.dostuff()            # connected here

#         other_stuff()                   # orig cursor closed
#
#         with conn.cursor() as cursor:
#             cursor.dostuff()            # new cursor
#
#     other_stuff()                       # disconnected here

class ConnectionBase:
    def __init__(self, args):
        self.args = args
        self.database = args.database

    # get a cursor for this connection
    def cursor(self):
        cursor = self.connection.cursor()

        try:
            # a server I know doesn't like to have the database name in the connection string
            # so I just specify a database on cursor creation
            cursor.execute('use {};'.format(self.args.database))

        except pymysql.err.InterfaceError as err:
            if "(0, '')" in str(err):
                print("Are you trying to use a closed connection?")
            raise

        # close the cursor when we exit a 'with' block
        cursor.__exit__ = lambda self : self.close()

        return cursor

    def __exit__(self, type, value, traceback):
        self.connection.close()

class Connection(ConnectionBase):
    def __enter__(self):

        if isinstance(self.args, LocalArgs):
            self.connection = pymysql.connect(user=self.args.user,
                                              passwd=self.args.password,
                                              autocommit=True,
                                              cursorclass=pymysql.cursors.DictCursor)

            # store this explicitly since it doesn't get populated on connection
            self.connection.db = self.args.database
            return self

        elif isinstance(self.args, RemoteArgs):
            # build ssl args
            if self.args.cipher != None:
                ssl = { 'cipher' : self.args.cipher }
            else:
                ssl = None

            self.connection = pymysql.connect(host=self.args.host,
                                              user=self.args.user,
                                              passwd=self.args.password,
                                              ssl=ssl,
                                              autocommit=True,
                                              cursorclass=pymysql.cursors.DictCursor)

            # store this explicitly since it doesn't get populated on connection
            self.connection.db = self.args.database
            return self

        else:
            raise ValueError("No known connection type for: {}".format(type(args)))
