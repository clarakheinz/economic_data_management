# -*- coding: utf-8 -*-
"""
Created on Tue May 19 14:07:37 2020

@author: clara.bickley
"""

import pandas as pd
import psycopg2
import datetime

## rmi link is accessed with 4-digit year and a alphanumeric quarter, i.e. q1
def rmi_grab_clean(year, quarter, last_psql_id):
    df = pd.read_excel('https://www.nahb.org/-/media/NAHB/news-and-economics/docs/housing-economics/RMI/{0}/q{1}/rmi-national-q{2}-{3}-excel.xlsx'.format(year, quarter, quarter, year), skiprows=3)
    df = df.iloc[:3,2:]
    df = df.T.reset_index()
    df.columns=['index',"Year",'Quarter',"rmi"]
    df.drop(['index'], axis=1, inplace=True)
    ## this file only has the current RMI datapoint, we need to know what the last record id is going to be ahead of running this
    df.loc[:,'id'] = 1 + last_psql_id
    df.loc[:, 'region_id'] = 9
    df['Quarter'] = df['Quarter'].str.replace('Q','')
    df.loc[:,'date'] = df.loc[:,'Year'].astype(str) + df.loc[:,'Quarter']
    df.loc[:,'date'] = pd.to_datetime(df.loc[:,'date']) + pd.offsets.QuarterEnd(0)\
        + pd.offsets.MonthBegin(-1)
    df = df.reindex(columns=['id','date','region_id','rmi'])
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

def get_year_prev_quarter():
    date = datetime.datetime.now()
    quarter = (date.month - 1) // 3 + 1
    if quarter == 1:
        prev_quarter = 4
        year = date.year - 1
    else:
        prev_quarter = quarter - 1
        year = date.year
    return prev_quarter, year

def main():
    ## set connection to db
    conn = connect_db(database, user, passw, port)

    ## grab rmi
    quarter, year = get_year_prev_quarter()
    nahb_rmi = rmi_grab_clean(year, quarter, 21)
    
    ## commence upserting
    upsert_mogrify(conn, nahb_rmi, 'nahb_rmi')
    
    conn.close()
    print('Updates complete.')
if __name__ == "__main__":
    main()