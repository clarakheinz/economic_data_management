# -*- coding: utf-8 -*-
"""
Created on Tue May 26 08:53:39 2020

@author: clara.bickley
"""

import pandas as pd
import numpy as np
import psycopg2
import camelot

def col_clean(col):
    col = col.replace(',','')
    col = int(col)
    return col

def nar_grab_clean(url):
    ## region map for region_id
    region_id_map={'U.S':9,'Northeast':1,'Midwest':2,'South':3,'West':4,'U.S.1':9,'Northeast.1':1,'Midwest.1':2,'South.1':3,'West.1':4}
    pdf = camelot.read_pdf(url, flavor='stream')
    df = pdf[0].df[3:]
    ## work with sales data in pdf
    df_sales = df.iloc[:18,:]
    df_sales.columns = df_sales.iloc[0]
    df_sales = df_sales[5:]
    df_sales.rename({'':'month'}, axis=1, inplace=True)
    df_sales['month'] = df_sales['month'].str.replace(' r','').str.replace(' p','').str.strip()
    df_sales.columns = ['year','month','U.S','Northeast','Midwest','South','West','U.S.1','Northeast.1','Midwest.1','South.1','West.1','Inventory','Supply']
    df_sales['year'] = pd.to_datetime(df_sales['year'], format='%Y').dt.year
    df_sales['month'] = pd.to_datetime(df_sales['month'], format='%b').dt.month
    df_sales['date'] = pd.to_datetime(df_sales[['year','month']].assign(day=1))
    ## create the two diff sales cols
    saar = pd.melt(df_sales, id_vars=['date'], value_vars=['U.S','Northeast','Midwest','South','West'], value_name='saar_sales',var_name=['region_id'])
    nsa = pd.melt(df_sales, id_vars=['date'], value_vars=['U.S.1','Northeast.1','Midwest.1','South.1','West.1'], value_name='nsa_sales',var_name=['region_id'])
    ## clean saar
    saar['saar_sales'] = saar['saar_sales'].apply(col_clean)
    saar['region_id'] = saar['region_id'].map(region_id_map)
    ## clean nsa
    nsa['nsa_sales'] = nsa['nsa_sales'].apply(col_clean)
    nsa['region_id'] = nsa['region_id'].map(region_id_map)   
    ## merge sales back
    sales_merge = pd.merge(saar, nsa, on=['date','region_id'], how='inner')
    ## work with prices data
    prices = df.iloc[26:44,:]
    prices = prices.iloc[3:-1,np.r_[1:13]]
    prices.columns = ['year','month','U.S','Northeast','Midwest','South','West','U.S.1','Northeast.1','Midwest.1','South.1','West.1']
    prices['month'] = pd.to_datetime(prices['month'].str.replace(' r','').str.replace(' p',''), format='%b').dt.month
    prices['year'] = pd.to_datetime(prices['year'],format='%Y').dt.year
    prices['date'] = pd.to_datetime(prices[['year','month']].assign(day=1))
    ## create two diff price type cols
    median = pd.melt(prices, id_vars=['date'], value_vars=['U.S','Northeast','Midwest','South','West'], value_name='median_sales_price',var_name=['region_id'])
    mean = pd.melt(prices, id_vars=['date'], value_vars=['U.S.1','Northeast.1','Midwest.1','South.1','West.1'], value_name='mean_sales_price',var_name=['region_id'])
    ## clean median
    median['median_sales_price'] = median['median_sales_price'].apply(col_clean)
    median['region_id'] = median['region_id'].map(region_id_map)
    ## clean mean
    mean['mean_sales_price'] = mean['mean_sales_price'].apply(col_clean)
    mean['region_id'] = mean['region_id'].map(region_id_map)
    ## merge back together
    price_merge = pd.merge(median, mean, on=['date','region_id'], how='inner')
    ## merge final df of sales and prices
    final_df = pd.merge(sales_merge, price_merge, on=['date','region_id'])
    ## create id from date and region_id
    final_df['id'] = (final_df['region_id'].astype(str) + final_df['date'].astype(str).str.replace('-','')).astype(int)
    ## reorder cols to fit in with psql table incase we ever need to reupload the entire table
    final_df = final_df.reindex(columns = ['id','region_id','date','saar_sales','mean_sales_price','median_sales_price','nsa_sales'])
    return final_df

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
    # SQL quert to execute
    cursor = conn.cursor()
    values = [cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
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
   ## set url
   url = input("What is this month's URL?:\n")
    
   ## set connection to db
   conn = connect_db(database,user,passw,port)
    
   ## grab and clean NAR data
   existing_homes = nar_grab_clean(url)
   ## commence upserting
   upsert_mogrify(conn, existing_homes, 'existing_home_data', 'nsa_sales')
   upsert_mogrify(conn, existing_homes, 'existing_home_data', 'saar_sales')
   upsert_mogrify(conn, existing_homes, 'existing_home_data', 'median_sales_price')
   upsert_mogrify(conn, existing_homes, 'existing_home_data', 'mean_sales_price')
    
   conn.close()
   print('Updates complete.')
   
if __name__ == "__main__":
    main()