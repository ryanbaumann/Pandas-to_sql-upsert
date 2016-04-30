import os
import sys
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import threading
from timeit import default_timer as timer

os.path.dirname(os.path.abspath(__file__))


def clean_df_db_dups(df, tablename, engine, dup_cols=[]):
    """
    Remove rows from a dataframe that already exist in a database
    Required: 
        df : dataframe to remove duplicate rows from
        engine: SQLAlchemy engine object 
        tablename: tablename to check duplicates in
        dup_cols: list or tuple of column names to check for duplicate row values
    Returns
        Unique list of values from dataframe compared to database table
    """

    args = 'SELECT %s FROM %s' %(', '.join(['"{0}"'.format(col) for col in dup_cols]), tablename)
    df.drop_duplicates(dup_cols, keep='last', inplace=True)
    df = pd.merge(df, pd.read_sql(args, engine), how='left', on=dup_cols, indicator=True)
    df = df[df['_merge'] == 'left_only']
    df.drop(['_merge'], axis=1, inplace=True)
    return df


def to_sql_newrows(df, pool_size, *args, **kargs):
    """
    Extend the Python pandas to_sql() method to thread database insertion

    Required: 
        df : pandas dataframe to insert new rows into a database table
        POOL_SIZE : your sqlalchemy max connection pool size.  Set < your db connection limit.
                    Example where this matters: your cloud DB has a connection limit.
    *args:
        Pandas to_sql() arguments.  

        Required arguments are:
            tablename : Database table name to write results to
            engine : SqlAlchemy engine

        Optional arguments are:
            'if_exists' : 'append' or 'replace'.  If table already exists, use append.
            'index' : True or False.  True if you want to write index values to the db.


    Credits for intial threading code:
        http://techyoubaji.blogspot.com/2015/10/speed-up-pandas-tosql-with.html
    """

    CHUNKSIZE = 1000
    INITIAL_CHUNK = 100
    if len(df) > CHUNKSIZE:
        #write the initial chunk to the database if df is bigger than chunksize
        df.iloc[:INITIAL_CHUNK, :].to_sql(*args, **kargs)
    else:
        #if df is smaller than chunksize, just write it to the db now
        df.to_sql(*args, **kargs)

    workers, i = [], 0

    for i in range((df.shape[0] - INITIAL_CHUNK)/CHUNKSIZE):
        t = threading.Thread(target=lambda: df.iloc[INITIAL_CHUNK+i*CHUNKSIZE:INITIAL_CHUNK+(i+1)*CHUNKSIZE].to_sql(*args, **kargs))
        t.start()
        workers.append(t)
        
    df.iloc[INITIAL_CHUNK+(i+1)*CHUNKSIZE:, :].to_sql(*args, **kargs)
    [t.join() for t in workers]


def setup(engine, tablename):
    engine.execute("""DROP TABLE IF EXISTS "%s" """ % (tablename))

    engine.execute("""CREATE TABLE "%s" (
                  "A" INTEGER,
                  "B" INTEGER,
                  "C" INTEGER,
                  "D" INTEGER,
                  CONSTRAINT pk_A_B PRIMARY KEY ("A","B")) 
                  """ % (tablename))

if __name__ == '__main__':
    DB_TYPE = 'postgresql'
    DB_DRIVER = 'psycopg2'
    DB_USER = 'admin'
    DB_PASS = 'password'
    DB_HOST = 'localhost'
    DB_PORT = '5432'
    DB_NAME = 'pandas_upsert'
    POOL_SIZE = 50
    TABLENAME = 'test_upsert'
    SQLALCHEMY_DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s' % (DB_TYPE, DB_DRIVER, DB_USER,
                                                          DB_PASS, DB_HOST, DB_PORT, DB_NAME)
    ENGINE = create_engine(
        SQLALCHEMY_DATABASE_URI, pool_size=POOL_SIZE, max_overflow=0)

    print 'setting up db'
    setup(ENGINE, TABLENAME)

    try:
        i=0
        prev = timer()
        start = timer()
        for i in range(10):
            print 'running test %s' %(str(i))
            df = pd.DataFrame(
                np.random.randint(0, 500, size=(100000, 4)), columns=list('ABCD'))
            df = clean_df_db_dups(df, TABLENAME, ENGINE, dup_cols=['A', 'B'])
            print 'row count after drop db duplicates is now : %s' %(df.shape[0])
            df.to_sql(TABLENAME, ENGINE, if_exists='append', index=False)
            end = timer()
            elapsed_time = end - prev
            prev = timer()
            print 'completed loop in %s sec!' %(elapsed_time)
            i += 1
        end = timer()
        elapsed_time = end - start
        print 'completed singlethread insert loops in %s sec!' %(elapsed_time)
        inserted = pd.read_sql('SELECT count("A") from %s' %(TABLENAME), ENGINE)
        print 'inserted %s new rows into database!' %(inserted.iloc[0]['count'])

        print '\n setting up db'
        setup(ENGINE, TABLENAME)
        print '\n'

        i=0
        prev = timer()
        start = timer()
        for i in range(10):
            print 'running test %s' %(str(i))
            df = pd.DataFrame(
                np.random.randint(0, 500, size=(100000, 4)), columns=list('ABCD'))
            df.drop_duplicates(['A', 'B'], keep='last', inplace=True)
            df.to_sql('temp', ENGINE, if_exists='replace', index=False)
            connection = ENGINE.connect() 
            args1 = """ INSERT INTO "test_upsert"
                        SELECT * FROM 
                        (SELECT a.* 
                        FROM "temp" a LEFT OUTER JOIN "test_upsert" b 
                            ON (a."A" = b."A" and a."B"=b."B")
                            WHERE b."A" is null) b"""
            result = connection.execute(args1)
            args2 = """ DROP Table If Exists "temp" """
            connection.execute(args2)
            connection.close()
            end = timer()
            elapsed_time = end - prev
            prev = timer()
            print 'completed loop in %s sec!' %(elapsed_time)
            i += 1
        end = timer()
        elapsed_time = end - start
        print 'completed staging insert loops in %s sec!' %(elapsed_time)
        inserted = pd.read_sql('SELECT count("A") from %s' %(TABLENAME), ENGINE)
        print 'inserted %s new rows into database!' %(inserted.iloc[0]['count'])
              
    except KeyboardInterrupt:
        print("Interrupted... exiting...")
