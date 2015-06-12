#!/usr/bin/python

import MySQLdb as mdb
import sys
import re

from servers import server_dict
from productions import table_exists

# This dictionary stores values used in classifying a
#   spill as being good or bad.
# Different 'roadsets' (57, 59, 62) have different criteria
spill_cut = {}
spill_cut['57'] = {'targetPos': (1, 7),
                   'TSGo': (1e3, 8e3),
                   'Accepted': (1e3, 8e3),
                   'AfterInh': (1e3, 3e4),
                   'Acc/After': (0.2, 0.9),
                   'NM3ION': (2e12, 1e13),
                   'G2SEM': (2e12, 1e13),
                   'QIESum': (4e10, 1e12),
                   'Inhibit': (4e9, 1e11),
                   'Busy': (4e9, 1e11),
                   'DutyFactor': (15, 60),
                   'BadSpillRanges': ()}
spill_cut['59'] = spill_cut['57']
spill_cut['62'] = {'targetPos': (1, 7),
                   'TSGo': (1e2, 6e3),
                   'Accepted': (1e2, 6e3),
                   'AfterInh': (1e2, 1e4),
                   'Acc/After': (0.2, 1.05),
                   'NM3ION': (2e12, 1e13),
                   'G2SEM': (2e12, 1e13),
                   'QIESum': (4e10, 1e12),
                   'Inhibit': (4e9, 2e11),
                   'Busy': (4e9, 1e11),
                   'DutyFactor': (10, 60),
                   'BadSpillRanges': ()}


def get_bad_spills(server, schema):

    # Initialize the spillID collection sets
    bad_spill_set = set()
    spill_set = set()

    # Determine which roadset to use
    found = re.findall('roadset(\d+)_', schema)
    if len(found) > 0:
        roadset = found[0]
    else:
        print 'No roadset number found in schema name ' + schema + '. Using 57.'
        roadset = '57'

    if roadset not in spill_cut:
        print 'Unknown roadset' + roadset + ". Using roadset 57 spill criteria."
        roadset = '57'

    # Begin Classifying Spills
    # 1. Query for a specific 'bad spill criteria'
    # 2. Retrieve returned set of spillID's
    # 3. Add offending spillID's to bad_spill_set
    try:
        # Connect to specified server and schema
        db = mdb.connect(read_default_file='../.my.cnf', read_default_group='guest',
                         db=schema,
                         host=server,
                         port=server_dict[server]['port'])
        cur = db.cursor()

        #########################

        query = """
                SELECT DISTINCT s.spillID
                FROM Spill s INNER JOIN Target t USING(spillID)
                WHERE s.targetPos != t.value
                AND t.name='TARGPOS_CONTROL'
                """
        cur.execute(query)
        print str(cur.rowcount) + ' Spills where Spill.targetPos != Target.TARGPOS_CONTROL'
        rows = cur.fetchall()
        spill_set.clear()
        for row in rows:
            bad_spill_set.add(row[0])

        #########################

        query = """
                SELECT DISTINCT s.spillID
                FROM Spill s
                WHERE s.targetPos NOT BETWEEN %d AND %d
                """
        cur.execute(query % (spill_cut[roadset]['targetPos'][0],
                             spill_cut[roadset]['targetPos'][1]))
        print (str(cur.rowcount) + ' spills where spill.targetpos not between ' +
               str(spill_cut[roadset]['targetPos'][0]) + ' and ' + str(spill_cut[roadset]['targetPos'][1]))
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])

        #########################
        
        query = """
                SELECT DISTINCT spillID
                FROM Scaler
                WHERE scalerName = 'TSGo'
                AND spillType='EOS'
                AND value NOT BETWEEN %d AND %d
                """
        cur.execute(query % (spill_cut[roadset]['TSGo'][0],
                             spill_cut[roadset]['TSGo'][1]))

        print (str(cur.rowcount) + ' spills where Scaler\'s TSGo not between ' +
               str(spill_cut[roadset]['TSGo'][0]) + ' and ' + str(spill_cut[roadset]['TSGo'][1]))
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])

        #########################
        
        query = """
                SELECT DISTINCT spillID
                FROM Scaler
                WHERE spillType = 'EOS' AND
                      scalerName = 'AcceptedMatrix1'
                AND value NOT BETWEEN %d AND %d
                """
        cur.execute(query % (spill_cut[roadset]['Accepted'][0],
                             spill_cut[roadset]['Accepted'][1]))
        print (str(cur.rowcount) + ' spills where Scaler\'s AcceptedMatrix1 not between ' +
               str(spill_cut[roadset]['Accepted'][0]) + ' and ' +
               str(spill_cut[roadset]['Accepted'][1]))
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])

        #########################

        query = """
                SELECT DISTINCT spillID
                FROM Scaler
                WHERE spillType = 'EOS' AND
                      scalerName = 'AfterInhMatrix1'
                AND value NOT BETWEEN %d AND %d
                """
        cur.execute(query % (spill_cut[roadset]['AfterInh'][0],
                             spill_cut[roadset]['AfterInh'][1]))
        print (str(cur.rowcount) + ' spills where Scaler\'s AfterInhMatrix1 not between ' +
               str(spill_cut[roadset]['AfterInh'][0]) + ' and ' + str(spill_cut[roadset]['AfterInh'][1]))
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])

        #########################

        query = """
                SELECT DISTINCT t1.spillID
                FROM (
                    SELECT spillID, value AS 'AcceptedMatrix1'
                    FROM Scaler
                    WHERE spillType = 'EOS' AND
                          scalerName = 'AcceptedMatrix1' ) t1
                INNER JOIN
                    (
                    SELECT spillID, value AS 'AfterInhMatrix1'
                    FROM Scaler
                    WHERE spillType = 'EOS' AND
                          scalerName = 'AfterInhMatrix1' ) t2
                USING(spillID)
                WHERE IF(AfterInhMatrix1>0,AcceptedMatrix1/AfterInhMatrix1,0) NOT BETWEEN %f AND %f
                """
        cur.execute(query % (spill_cut[roadset]['Acc/After'][0],
                             spill_cut[roadset]['Acc/After'][1]))
        print (str(cur.rowcount) + ' spills where Scaler\'s Accepted / AfterInhibit not between ' +
               str(spill_cut[roadset]['Acc/After'][0]) + ' and ' + str(spill_cut[roadset]['Acc/After'][1]))
        rows = cur.fetchall()
        spill_set.clear()
        for row in rows:
            bad_spill_set.add(row[0])

        #########################
        
        query = """
                SELECT DISTINCT spillID
                FROM Beam
                WHERE name = 'S:G2SEM' AND
                        value NOT BETWEEN %d AND %d
                """
        cur.execute(query % (spill_cut[roadset]['G2SEM'][0],
                             spill_cut[roadset]['G2SEM'][1]))
        print (str(cur.rowcount) + ' spills where Beam\'s G2SEM not between ' +
               str(spill_cut[roadset]['G2SEM'][0]) + ' and ' + str(spill_cut[roadset]['G2SEM'][1]))
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])

        #########################
        
        query = """
                SELECT DISTINCT spillID
                FROM BeamDAQ
                WHERE QIESum NOT BETWEEN %d AND %d
                """
        cur.execute(query % (spill_cut[roadset]['QIESum'][0],
                             spill_cut[roadset]['QIESum'][1]))
        print (str(cur.rowcount) + ' spills where BeamDAQ\'s QIESum not between ' +
               str(spill_cut[roadset]['QIESum'][0]) + ' and ' + str(spill_cut[roadset]['QIESum'][1]))
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])
        
        #########################

        query = """
                SELECT DISTINCT spillID
                FROM BeamDAQ
                WHERE inhibit_block_sum NOT BETWEEN %d AND %d
                """
        cur.execute(query % (spill_cut[roadset]['Inhibit'][0],
                             spill_cut[roadset]['Inhibit'][1]))
        print (str(cur.rowcount) + ' spills where BeamDAQ\'s Inhibit not between ' +
               str(spill_cut[roadset]['Inhibit'][0]) + ' and ' + str(spill_cut[roadset]['Inhibit'][1]))
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])

        #########################

        query = """
                SELECT DISTINCT spillID
                FROM BeamDAQ
                WHERE trigger_sum_no_inhibit NOT BETWEEN %d AND %d
                """
        cur.execute(query % (spill_cut[roadset]['Busy'][0],
                             spill_cut[roadset]['Busy'][1]))
        print (str(cur.rowcount) + ' spills where BeamDAQ\'s Busy not between ' +
               str(spill_cut[roadset]['Busy'][0]) + ' and ' + str(spill_cut[roadset]['Busy'][1]))
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])
        
        #########################

        query = """
                SELECT DISTINCT spillID
                FROM BeamDAQ
                WHERE dutyfactor53MHz NOT BETWEEN %d AND %d
                """
        cur.execute(query % (spill_cut[roadset]['DutyFactor'][0],
                             spill_cut[roadset]['DutyFactor'][1]))
        print (str(cur.rowcount) + ' spills where BeamDAQ\'s Duty Factor not between ' +
               str(spill_cut[roadset]['DutyFactor'][0]) + ' and ' + str(spill_cut[roadset]['DutyFactor'][1]))
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])

        #########################

        if len(spill_cut[roadset]['BadSpillRanges']) > 0:
            query = """
                    SELECT DISTINCT spillID
                    FROM Spill
                    WHERE
                    """

            for spill_range in spill_cut[roadset]['BadSpillRanges']:
                query += " spillID BETWEEN " + str(spill_range[0]) + " AND " + str(spill_range[1]) + " OR "
            query = query[:-4]
            cur.execute(query)
            print str(cur.rowcount), 'Spills in ranges', spill_cut[roadset]['BadSpillRanges']
            rows = cur.fetchall()
            for row in rows:
                bad_spill_set.add(row[0])

        #########################
        
        query = """
                        SELECT DISTINCT spillID FROM Target
                        WHERE name='TARGPOS_CONTROL'
                        GROUP BY spillID
                        HAVING COUNT(*) > 1
                    UNION
                        SELECT DISTINCT spillID FROM Spill
                        GROUP BY spillID
                        HAVING COUNT(*) > 1
                    UNION
                        SELECT DISTINCT spillID
                        FROM Scaler
                        WHERE spillType='EOS' AND scalerName='TSGo'
                        GROUP BY spillID
                        HAVING COUNT(*) > 1
                    UNION
                        SELECT DISTINCT spillID
                        FROM Scaler
                        WHERE spillType='EOS' AND scalerName='AfterInhMatrix1'
                        GROUP BY spillID
                        HAVING COUNT(*) > 1
                    UNION
                        SELECT DISTINCT spillID
                        FROM Scaler
                        WHERE spillType='EOS' AND scalerName='AcceptedMatrix1'
                        GROUP BY spillID
                        HAVING COUNT(*) > 1
                    UNION
                        SELECT DISTINCT spillID
                        FROM BeamDAQ
                        GROUP BY spillID
                        HAVING COUNT(*) > 1
                    UNION
                        SELECT DISTINCT spillID
                        FROM Beam
                        WHERE name='F:NM3ION'
                        GROUP BY spillID
                        HAVING COUNT(*) > 1
                    UNION
                        SELECT DISTINCT spillID
                        FROM Beam
                        WHERE name='S:G2SEM'
                        GROUP BY spillID
                        HAVING COUNT(*) > 1
                """
        cur.execute(query)
        print str(cur.rowcount) + ' Spills with duplicate values'
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])

        #########################
        
        query = """
                    SELECT DISTINCT s.spillID
                    FROM Spill s LEFT JOIN
                        ( SELECT DISTINCT spillID FROM Target t
                          WHERE name='TARGPOS_CONTROL'
                        ) t
                    USING(spillID)
                    WHERE t.spillID IS NULL
                UNION
                    SELECT s.spillID
                    FROM Spill s LEFT JOIN
                        ( SELECT DISTINCT spillID
                          FROM Scaler
                          WHERE spillType='EOS' AND scalerName='TSGo'
                        ) sc USING(spillID)
                    WHERE sc.spillID IS NULL
                UNION
                    SELECT s.spillID
                    FROM Spill s LEFT JOIN
                        ( SELECT DISTINCT spillID
                          FROM Scaler
                          WHERE spillType='EOS' AND scalerName='AfterInhMatrix1'
                        ) sc USING(spillID)
                    WHERE sc.spillID IS NULL
                UNION
                    SELECT s.spillID
                    FROM Spill s LEFT JOIN
                        ( SELECT DISTINCT spillID
                          FROM Scaler
                          WHERE spillType='EOS' AND scalerName='AcceptedMatrix1'
                        ) sc USING(spillID)
                    WHERE sc.spillID IS NULL
                UNION
                    SELECT s.spillID
                    FROM Spill s LEFT JOIN
                        ( SELECT DISTINCT spillID
                          FROM BeamDAQ
                        ) b USING(spillID)
                    WHERE b.spillID IS NULL
                UNION
                    SELECT DISTINCT s.spillID
                    FROM Spill s LEFT JOIN
                        ( SELECT DISTINCT spillID
                          FROM Beam
                          WHERE name='S:G2SEM' ) b
                    USING (spillID)
                    WHERE b.spillID IS NULL
                UNION
                    SELECT DISTINCT t.spillID
                    FROM Spill s RIGHT JOIN
                        ( SELECT DISTINCT spillID FROM Target t
                          WHERE name='TARGPOS_CONTROL'
                        ) t
                    USING(spillID)
                    WHERE s.spillID IS NULL
                UNION
                    SELECT sc.spillID
                    FROM Spill s RIGHT JOIN
                        ( SELECT DISTINCT spillID
                          FROM Scaler
                          WHERE spillType='EOS'
                                AND (scalerName='TSGo' OR
                                     scalerName='AfterInhMatrix1' OR
                                     scalerName='AcceptedMatrix1')
                        ) sc USING(spillID)
                    WHERE s.spillID IS NULL
                UNION
                    SELECT b.spillID
                    FROM Spill s RIGHT JOIN
                        ( SELECT DISTINCT spillID
                          FROM BeamDAQ
                        ) b USING(spillID)
                    WHERE s.spillID IS NULL
                UNION
                    SELECT DISTINCT b.spillID
                    FROM Spill s RIGHT JOIN
                        ( SELECT DISTINCT spillID
                          FROM Beam
                          WHERE name='S:G2SEM' ) b
                    USING (spillID)
                    WHERE s.spillID IS NULL
                """
        cur.execute(query)
        print str(cur.rowcount) + ' Spills with missing value(s)'
        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(row[0])
        
        # All done! Close up shop.
        if db:
            db.close()

    except mdb.Error, e:

        print "Error %d: %s" % (e.args[0], e.args[1])
        return 1

    return bad_spill_set


def write_bad_spills_to_table(server, schema, bad_spill_set, clear=False):
"""
Getting a bad spill can take a lot of time. 
Use this to store result to a MySQL table.
"""

    try:
        # Connect to specified server and schema
        db = mdb.connect(read_default_file='../.my.cnf', read_default_group='guest',
                         db=schema,
                         host=server,
                         port=server_dict[server]['port'])
        cur = db.cursor()

        # Create table to store the "bad spill" spillID's
        if not table_exists(server, schema, "bad_spills"):
            cur.execute("CREATE TABLE bad_spills (spillID INT PRIMARY KEY)")

        # Empty the table if requested
        if clear:
            cur.execute("DELETE FROM bad_spills")

        # Create the INSERT query
        if len(bad_spill_set) > 0:
            query = "INSERT INTO bad_spills (spillID) VALUES "
            for bad_spill in bad_spill_set:
                query += "(" + str(bad_spill) + "), "
            query = query[:-2]

            # Execute the assembled query
            cur.execute(query)

        if db:
            db.close()

    except mdb.Error, e:

        print "Error %d: %s" % (e.args[0], e.args[1])
        return 1

    return 0


def get_bad_spills_from_table(server, schema):
"""Once a bad spill set is stored, it can be loaded back up"""

    bad_spill_set = set()

    try:
        # Connect to specified server and schema
        db = mdb.connect(read_default_file='../.my.cnf', read_default_group='guest',
                         db=schema,
                         host=server,
                         port=server_dict[server]['port'])
        cur = db.cursor()

        # Query for and store all spillID's from the table
        query = "SELECT DISTINCT spillID FROM bad_spills"
        cur.execute(query)

        rows = cur.fetchall()
        for row in rows:
            bad_spill_set.add(int(row[0]))

        if db:
            db.close()

    except mdb.Error, e:

        print "Error %d: %s" % (e.args[0], e.args[1])
        return 1

    return bad_spill_set


def clear_bad_spill_dimuons(server, schema, table, bad_spill_set):
"""
If you have an anlysis table and a bad spill set
Call this function to have all dimuons from bad spillID's deleted
"""

    # Assemble DELETE query from bad_spill_set
    query = """DELETE FROM %s.%s
               WHERE spillID IN("""

    if len(bad_spill_set) > 0:
        for bad_spill in bad_spill_set:
            query = query + str(bad_spill) + ', '
        query = query[:-2] + ')'
    else:
        print 'Empty bad spill set.'
        return 0

    try:
        # Connect to specified server and schema
        db = mdb.connect(read_default_file='../.my.cnf', read_default_group='guest',
                         db=schema,
                         host=server,
                         port=server_dict[server]['port'])
        cur = db.cursor()

        # Execute assembled query, store the number of rows deleted
        cur.execute(query % (schema, table))
        deleted_count = cur.rowcount

        if db:
            db.close()

    except mdb.Error, e:

        print "Error %d: %s" % (e.args[0], e.args[1])
        return -1

    # Return how many rows were deleted
    return deleted_count


def main():
    # Assign any general purpose direct calls of spill.py here
    print 'Hello World!'
    print table_exists('e906-db3.fnal.gov', 'user_dannowitz_analysis_Mar30', "bad_spills")

if __name__ == '__main__':
    main()
