#!/usr/bin/python
"""check-targetpos. Uses a trained Random Forest Classifier to predict the target
        positions based on Scaler and Beam values.
   If the

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

def main():
    # Use docopt to get command line arguments
    arguments = docopt(__doc__, version="check-targetpos v0.1")

    print arguments

    query = """
            SELECT s.spillID, scalerName AS `name`, value, targetPos
            FROM Scaler 
            INNER JOIN Spill s
                USING(spillID) 
            WHERE scalerName IS NOT NULL AND 
                s.spillID!=0 AND
                s.spillID NOT BETWEEN 416709 AND 423255 AND
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
                s.spillID NOT BETWEEN 416709 AND 423255 AND
                s.spillID NOT BETWEEN 482574 AND 484924
            """

    try:
        db = mdb.connect(read_default_file='./.my.cnf',     # Keep my login credentials secure
                         read_default_group='guest',        # Read-only access to important data
                         host=arguments['--server'],
                         db=arguments['--schema'],
                         port=server_dict[arguments['--server']]['port'])

        data_df = pd.read_sql(query, db)

        if db:
            db.close()

    except mdb.Error, e:

        print "Error %d: %s" % (e.args[0], e.args[1])

    data_df[['value']] = data_df[['value']].astype(float)
    pivoted_df = data_df.pivot('spillID', 'name', 'value')
    pivoted_df = pivoted_df.replace(-9999,np.nan).dropna(axis=0,how='any')
    zero_std_series = (pivoted_df.describe().ix['std'] == 0)
    # Get an array of all the features with zero standard deviations
    zero_std_features = zero_std_series[zero_std_series == True].index.values
    _ = pivoted_df.drop(zero_std_features, axis=1, inplace=True)
    targpos_df = data_df[['spillID','targetPos']].drop_duplicates().sort('spillID')
    full_df = pd.merge(pivoted_df, targpos_df, how='left', right_on='spillID', left_index=True)
    full_df = full_df.set_index('spillID')
    
    labels = full_df[['targetPos']].values
    data = full_df.drop('targetPos', axis=1).values
    rfc = joblib.load(arguments['--model'])
    result = rfc.predict(data)

    print("RF prediction accuracy = {0:5.1f}%".format(100.0 * rfc.score(data, labels)))

    return 0

if __name__=='__main__':
    main()


