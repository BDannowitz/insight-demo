#!/usr/bin/python
"""productions.

Usage:
    ./productions.py [--revision=<revision>] [--server=<server>] [--roadset=<roadset>]
    ./productions.py -h | --help
    ./productions.py --version

Options:
    --revision=<revision>       Which rivisions would you like to list [Default: R004]
    --server=<server>           Server host of productions
    --roadset=<roadset>         Roadset number of productions
"""


import MySQLdb as mdb
from docopt import docopt
from servers import server_dict

roadset_dict = {'49': {'lower': 8184, 'upper': 8896},
                '56': {'lower': 8897, 'upper': 8908},
                '57': {'lower': 8910, 'upper': 10420},
                '59': {'lower': 10421, 'upper': 10912},
                '61': {'lower': 10914, 'upper': 11028},
                '62': {'lower': 11040, 'upper': 12522}}

revision_list = ['R000', 'R001', 'R003', 'R004', 'R005']


def get_productions(revision='R004', server=None, roadset=None):
    """
    Retrieve a dictionary of lists of productions for a certain revision.
    Example output:
        output_dict = { server_name1: [ production1, production2, ... ],
                        server_name2: [ production22, production25, ...],
                        ...
                      }
    """
    prod_dict = {}

    # For each server that we have defined...
    for server_entry in server_dict:
        # ...or server specified in the function call
        if (server is None) or (server_entry == server):

            # Make a dictionary entry
            prod_dict[server_entry] = []
            try:
                # Connect to specified server and schema
                db = mdb.connect(read_default_file='../.my.cnf',
                                 read_default_group='guest', host=server_entry,
                                 port=server_dict[server_entry]['port'])

                cur = db.cursor()
    
                # Query the server for all productions of specified revisions
                #   and append them to the list for the given server
                for revision_entry in revision_list:
                    if (revision is None) or (revision_entry == revision):
                        query = "SHOW DATABASES LIKE 'run\_______\_" + revision_entry + "'"
                        cur.execute(query)
                        rows = cur.fetchall()

                        for row in rows:
                            run = int(row[0][4:10])
                            # If you're looking for a specific roadset, it is filtered out here
                            for roadset_entry in roadset_dict:
                                if ((roadset is None) and (row[0] not in prod_dict[server_entry])) or \
                                   ((roadset == roadset_entry) and \
                                   (roadset_dict[roadset_entry]['lower'] <= run <= roadset_dict[roadset_entry]['upper'])):
                                    prod_dict[server_entry].append(row[0])

                if db:
                    db.close()

            except mdb.Error, e:
                try:
                    print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                except IndexError:
                    print "MySQL Error: %s" % str(e)

    return prod_dict


def table_exists(server, schema, table):
    """
    Takes a server, schema, and table name
    Returns:
        1 if table exists (case-sensitive)
        0 if table does not exist
        -1 if query or connection error occurs
    """

    exists = -1

    try:

        db = mdb.connect(read_default_file='../.my.cnf', 
                         read_default_group='guest',
                         db=schema, 
                         host=server,
                         port=server_dict[server]['port'])
        cur = db.cursor()
        cur.execute("SHOW TABLES LIKE '" + table + "'")
        exists = 1 if cur.rowcount > 0 else 0

    except mdb.Error, e:

        print "Error %d: %s" % (e.args[0], e.args[1])
        return -1

    return exists


def main():
    
    # Use docopt to get command line arguments
    arguments = docopt(__doc__, version="productions v0.1")
    
    print arguments
    prod_dict = get_productions(arguments['--revision'], arguments['--server'], arguments['--roadset'])

    # Print out the results
    for server in prod_dict:
        print server
        for prod in prod_dict[server]:
            print prod

if __name__ == "__main__":
    main()
