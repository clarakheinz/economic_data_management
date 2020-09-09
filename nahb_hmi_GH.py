# -*- coding: utf-8 -*-
"""
Created on Tue May 19 08:52:12 2020

@author: clara.bickley
"""

import pandas as pd
import numpy as np
import psycopg2
import datetime

## we will grab HMI from an excel file download link, this is named with the date of the release so this will need to be input everytime
## first date is 4-digit year, dash, 2-digit month
## second date is 4-digit year with no space then 2-digit month
def hmi_grab_clean(date1, date2):
    df = pd.read_excel('https://www.nahb.org/-/media/NAHB/news-and-economics/docs/housing-economics/hmi/{0}/t1-national-and-regional-HMI-{1}.xls'.format(date1, date2), skiprows=2)[:4] 
    df = df.T[3:]
    df = df.reset_index()
    df.rename({'index':'year', 0 : "month",3:"hmi"}, axis=1, inplace=True)
    df['year'] = df['year'].replace(r"Unnamed*", np.nan, regex=True)
    df['year'] = pd.to_datetime(df['year'].fillna(method='ffill'), format='%Y').dt.year
    df['month'] = pd.to_datetime(df['month'].str.replace('.',''), format='%b').dt.month
    df['date'] = pd.to_datetime(df[['year','month']].assign(day=1))
    df.loc[:, 'region_id'] = 9
    df.loc[:, 'id'] = range(289,289+len(df))
    df = df.reindex(columns = ['id','date','region_id','hmi'])
    df = df.iloc[-5:,:]
    return df

## define connection that will print the errors that arise
def connect_db(database, user, password, port):
    try:
        ## connect to the database
        print("Connecting to PostgreSQL db...")
        conn = psycopg2.connect(database=database, user=user, password=password,port=port)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return 1
    print("Your connection was successful")
    return conn

def upsert_mogrify(conn, df, table):
    """
    Using cursor.mogrify() to build the bulk insert query with insert on conflict
    then cursor.execute() to execute the query
    """
    # grab name of df
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL query to execute
    cursor = conn.cursor()
    values = [cursor.mogrify("(%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
    query  = "INSERT INTO %s(%s) VALUES "% (table, cols) + ",".join(values) + \
        "ON CONFLICT (id) DO UPDATE SET %s = EXCLUDED.%s" % (str(cols[-3:]), str(cols[-3:]))
    try:
        cursor.execute(query, tuples)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("upsert_mogrify() done")

def get_dates():
    date1 = str(datetime.datetime.now().year) + '-0' + str(datetime.datetime.now().month)
    if datetime.datetime.now().month < 11:
        date2 = str(datetime.datetime.now().year) + '0' + str(datetime.datetime.now().month - 1)
    else:
        date2 = str(datetime.datetime.now().year) + str(datetime.datetime.now().month - 1)
    return date1, date2

def main():
    ## set connection to db
    conn = connect_db(database, user, passw, port)
    
    ## grab and clean nahb datasets
    date1, date2 = get_dates()
    nahb_hmi = hmi_grab_clean(date1,date2)
    
    ## commence upserting
    upsert_mogrify(conn, nahb_hmi, 'nahb_hmi')
    
    conn.close()
    print('Updates complete.')
if __name__ == "__main__":
    main()