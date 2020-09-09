# -*- coding: utf-8 -*-
"""
Created on Wed May 13 08:50:05 2020

@author: clara.bickley
"""

import pandas as pd
import numpy as np
from fredapi import Fred
import psycopg2

fred = Fred(api_key=APIKEY)
series_want = {'PERMITNSA':'Total_Permits','PERMIT1NSA':'SF_Permits',
               'PERMITNENSA':'NE Permits','PERMITMWNSA':'MW Permits',
               'PERMITSNSA':'So Permits','PERMITWNSA':'W Permits',
               'HOUSTNSA':'Total_Starts','HOUST1FNSA':'SF_Starts',
               'HSN1FNSA':'nsa_sales','HOUSTNENSA':'NE Starts',
               'HOUSTMWNSA':'MW Starts','HOUSTSNSA':'So Starts',
               'HOUSTWNSA':'W Starts','MSPNHSUS':'median_home_prices',
               'CPIAUCNS':'cpiu','DSPIC96':'rdpi'}

## function for collecting our data
def collecting(series_want):
    df = {}
    for s, v in series_want.items():
        df[v] = fred.get_series(s)
    df = pd.DataFrame(df)
    df.rename_axis('date', inplace=True)
    df.reset_index(inplace=True)
    df['date'] = pd.to_datetime(df['date'], format="%m/%d/%Y")
    collected_df = df.copy()
    return collected_df

## partial dictionary match function
def partial_dict(s, dict_map):
    for key in dict_map.keys():
        if key in s: 
            return dict_map[key]
    return np.nan


## function for cleaning and arranging the dataframes as needed for sql update
## will need to slice just each series like permits vs starts before passing this function
def cleaning(df):
    ## construction type dictionary for data
    const_map = {'Total':1,'SF':2,'MF':3,'NE':1,'MW':1,'So':1,'W':1}
    ## region id dictionary for data
    region_map = {'Total':9,'SF':9,'MF':9,'NE':1,'MW':2,'So':3,'W':4}
    
    df.dropna(axis=0, inplace=True)
    
    if len(df.columns) >= 4: ## melting, creating mf cols, this should only be for starts and permits really
        df.loc[:,'MF'] = df.iloc[:,1] - df.iloc[:,2] ## create multifamily col
        ## melt cols like total, sf, mf, etc
        df = pd.melt(df, id_vars=['date'], value_vars=df.columns[1:], value_name=df.columns[1].split('_')[1].lower(), var_name = 'region_id')
        ## create const type col
        df.loc[:,'const_type_id'] = df['region_id'].apply(lambda x: partial_dict(x, const_map))
        ## create integer region id
        df.loc[:,'region_id'] = df['region_id'].apply(lambda x: partial_dict(x, region_map))
        ## create an id col
        df = df.sort_values(by=['date','region_id'])
        df.loc[:,'id'] = range(1, 1+len(df))
        ## reorder cols to match postgresql table cols
        df = df.reindex(columns=['id','date','region_id','const_type_id',df.columns[2]])
    elif len(df.columns) == 3: ## how to clean the new home sales data series as it is a bit  of a special case
        df.loc[:,'region_id'] = 9
        df = df.sort_values(by=['date'])
        df.loc[:,'id'] = range(1, 1+len(df))
        df = df.reindex(columns=['id','region_id','date', df.columns[1], df.columns[2]])
    else: ## clean the inflation, rdpi, and any other single var series
        df.loc[:,'region_id'] = 9
        df = df.sort_values(by=['date'])
        df.loc[:,'id'] = range(1, 1+len(df))
        df = df.reindex(columns=['id','region_id','date', df.columns[1]])
    return df ## give back the df

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

def upsert_mogrify(conn, df, table, update_col):
    """
    Using cursor.mogrify() to build the bulk insert query with insert on conflict
    then cursor.execute() to execute the query
    """
    # grab name of df
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    if len(df.columns) == 5:    
        # SQL quert to execute
        cursor = conn.cursor()
        values = [cursor.mogrify("(%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        query  = "INSERT INTO %s(%s) VALUES "% (table, cols) + ",".join(values) + "ON CONFLICT (id) DO UPDATE SET %s = EXCLUDED.%s" % (update_col, update_col)
    elif len(df.columns) == 4:
        # SQL query to execute
        cursor = conn.cursor()
        values = [cursor.mogrify("(%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        query  = "INSERT INTO %s(%s) VALUES "% (table, cols) + ",".join(values) + "ON CONFLICT (id) DO UPDATE SET %s = EXCLUDED.%s" % (update_col, update_col)

        
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


def main():
    ## set connection to db
    conn = connect_db(database,user,passw,port)
    
    ## grab econ data from FRED
    econ_data = collecting(series_want)

    ## grab each economic df, clean it and slice just the most recent data
    ## permits has 3 regions with 1 construction type, and 1 region with \
    ## 3 construction types, so we grab 21 records 
    permits = cleaning(econ_data.iloc[:,:7])[-21:]
    ## starts is just like permits, so we grab 21 records
    starts = cleaning(econ_data.iloc[:,np.r_[0,7:9,10:14]])[-21:] 
    ## the rest of the dataframes only have 1 region and 1 const type so \
    ## we snag the last 3 records
    new_sf_home_data = cleaning(econ_data.iloc[:,np.r_[0,9,14]])[-3:]
    real_disposable_income = cleaning(econ_data.iloc[:,np.r_[0,16]])[-3:]
    cpiu_inflation = cleaning(econ_data.iloc[:,np.r_[0,15]])[-3:]
    
    ## commence upserting
    upsert_mogrify(conn, permits, 'permits', 'permits')
    upsert_mogrify(conn, starts, 'starts', 'starts')
    upsert_mogrify(conn, new_sf_home_data, 'new_sf_home_data', 'nsa_sales')
    upsert_mogrify(conn, new_sf_home_data, 'new_sf_home_data', 'median_home_prices')
    upsert_mogrify(conn, real_disposable_income, 'real_disposable_income', 'rdpi')
    upsert_mogrify(conn, cpiu_inflation, 'cpiu_inflation', 'cpiu')
    
    conn.close()
    print('Updates complete.')
if __name__ == "__main__":
    main()
