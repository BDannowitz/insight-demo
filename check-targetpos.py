#!/usr/bin/python
"""check-targetpos. Uses a trained Random Forest Classifier to predict the target
        positions based on Scaler and Beam values.

Usage:
    ./check-targetpos.py --schema=<schema> [--server=<server>] [--limit=<limit>] [--email <email>...]
                         [--model=<model>]
    ./check-targetpos.py -h | --help
    ./check-targetpos.py --version

Options:
    --schema=<schema>           Name of production to test
    --server=<server>           Server host of productions [default: e906-db1.fnal.gov]
    --limit=<limit>             Threshold of prediction accuracy before an alarm email is sent [default: 75]
    --email=<email>             Email address of the person to alert.
    --model=<model>             Name of pickled RFC object to restore and use [default: models/target_rfc_roadset62.pkl]
    -h, --help                  Print this help
    --version                   Print version number
"""

import sys

import pandas as pd
import numpy as np
import MySQLdb as mdb
from sklearn.ensemble import RandomForestClassifier    
from sklearn import cross_validation   
import joblib

sys.path.append('modules')
from docopt import docopt
from servers import server_dict
from spill import get_bad_spills
from visualization import confusion


def get_dataframe_from_sql(server, schema):

    query = """
            SELECT s.spillID, scalerName AS `name`, value, targetPos
            FROM Scaler 
            INNER JOIN Spill s
                USING(spillID) 
            WHERE scalerName IS NOT NULL AND 
                s.spillID!=0 AND
                s.spillID NOT BETWEEN 412300 AND 427000 AND
                s.spillID NOT BETWEEN 423921 AND 424180 AND
                s.spillID NOT BETWEEN 482574 AND 484924 AND 
                spillType='EOS'
            
            UNION ALL
            
            SELECT s.spillID, name, value, targetPos 
            FROM Beam
            INNER JOIN Spill s              # Source of targetPos
                USING(spillID)
            WHERE name IS NOT NULL AND
                LEFT(name,3)!='F:M' AND     # Exclude features that are always NULL
                name!='F:NM2SEM' AND        # 
                name!='U:TODB25' AND        #
                name!='S:KTEVTC' AND        #
                s.spillID!=0 AND
                s.spillID NOT BETWEEN 412300 AND 427000 AND
                s.spillID NOT BETWEEN 423921 AND 424180 AND
                s.spillID NOT BETWEEN 482574 AND 484924 
            """

    try:
        db = mdb.connect(read_default_file='./.my.cnf',     # Keep my login credentials secure
                         read_default_group='guest',        # Read-only access to important data
                         host=server,
                         db=schema,
                         port=server_dict[server]['port'])

        data_df = pd.read_sql(query, db)

        if db:
            db.close()

    except mdb.Error, e:

        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)

    return data_df


def check_for_features(df_columns, feature_list):

    flag = True
    for feature in feature_list:
        if feature not in df_columns:
            flag = False
            print "Missing feature: " + feature

    return flag


def relabel(label_array):
    # Collapse target position 4 and 2 both into category 2
    # Then shift over the rest
    label_array[label_array == 4] = 2
    label_array[label_array == 5] = 4
    label_array[label_array == 6] = 5
    label_array[label_array == 7] = 6
                                   
    return label_array


def per_target_accuracy(hist2d_pts, names):

    for i in range(len(names)):
        rowsum = np.sum(hist2d_pts.T[i])
        if rowsum>0:
            print names[i] + ":   \t" + str(round((hist2d_pts[i][i] / np.sum(hist2d_pts.T[i]))*100,2)) + "%"
        else:
            print names[i] + ":   \tN/A"


def main():

    useful_feature_list = ['G:RD3161',
                           'AfterInhMATRIX5',
                           'G:RD3162',
                           'PrescaleMATRIX5',
                           'AfterInhMATRIX3',
                           'PrescaleMATRIX3',
                           'AcceptedMATRIX1',
                           'RawNIM1',
                           'AfterInhMATRIX1',
                           'TsBusy',
                           'AfterInhNIM2',
                           'PrescaledTrigger',
                           'TSGo',
                           'AfterInhMATRIX4',
                           'PrescaleMATRIX1',
                           'RawMATRIX4',
                           'RawTriggers',
                           'RawMATRIX5']

    # Use docopt to get command line arguments
    arguments = docopt(__doc__, version="check-targetpos v0.1")

    print arguments

    # Get relevant fields from 
    data_df = get_dataframe_from_sql(arguments['--server'], arguments['--schema'])

    # Caste 'value' as a float
    data_df[['value']] = data_df[['value']].astype(float)

    # Get rid of entries from bad spills
    bad_spill_set = get_bad_spills(arguments['--server'],arguments['--schema'])
    data_df = data_df.query('spillID not in @bad_spill_set')

    # Get the data into 'wide' format
    pivoted_df = data_df.pivot('spillID', 'name', 'value')

    # Get rid of entries with sentinel values
    _ = pivoted_df.replace(-9999,np.nan).dropna(axis=0,how='any', inplace=True)
    
    # Get labels from the original dataframe
    targpos_df = data_df[['spillID','targetPos']].drop_duplicates().sort('spillID')
    
    # Combine data with labels into one dataframe
    full_df = pd.merge(pivoted_df, targpos_df, how='left', right_on='spillID', left_index=True)
    full_df = full_df.set_index('spillID')
    full_df.to_csv('testrun.csv')

    # Check to see if this dataframe has all the features we need
    if not check_for_features(full_df.columns.values, useful_feature_list):
        print "Not all features exist in %s on %s.\n" % (arguments['--schema'], arguments['--server'])
        print "Please either check the schema or revise the RFC and features list.\n"
        print "These exist in the schema:\n", full_df.columns.values
        print "These are the required fields:\n", useful_feature_list
        return 1

    if 'S:G2SEM' not in full_df.columns.values:
        print "We require 'S:G2SEM' for beam intensity normalization.\n"
        print "Please check the schema.\n\n"
        return 1
    
    # Store beam intensity and remove it from the data frame
    #beam_intensity = full_df[['S:G2SEM']].values

    # Extract the labels for the data
    labels = full_df.values[:,-1]
    # Combine Empty and None labels into label=2. Shift the rest down
    labels = relabel(labels)

    # Extract the data -- only the fields we want and in the order we want them 
    full_df = full_df[useful_feature_list]

    data    = full_df.values
   
    print full_df.columns

    print full_df.head()

    # Rescale the data to beam intensity
    #    Beam intensity is a big number (O(10^12)), so multiply by a big constant
    #    to bring it back up to normal feature ranges 
    #data    = ( data / beam_intensity ) * 5000000000000
    
    rfc = joblib.load(arguments['--model'])
    result = rfc.predict(data)

    print result

    print("RF prediction accuracy = {0:5.1f}%\n\n".format(100.0 * rfc.score(data, labels)))

    names = ['Hydrogen','Empty/None','Deuterium','Carbon','Iron','Tungsten']
    pts = confusion(labels, result, names)
    per_target_accuracy(pts, names)

    return 0

if __name__=='__main__':
    main()


