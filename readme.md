# Pandas to-sql 'Upsert' : Why

Read more detail on the blog: https://www.ryanbaumann.com/blog/2016/4/30/python-pandas-tosql-only-insert-new-rows

Frequently in data analysis workflows, data is ingested from multiple sources into an application (python in this case), analzed in-memory using a library such as Pandas, Scipy, Numpy, or SAS, and then the results need to be written to a database.  If my workflow ingests data that is already in the database, I don't want to write the duplicate rows.

The goal of this library is to extend the Python Pandas to_sql() function to be:

1) Muti-threaded (improving time-to-insert on large datasets)

2) Allow the to_sql() command to run an 'insert if does not exist' to the 
database

3) Perform the data duplication check 'in-memory' 

# Pandas to-sql 'Upsert' : Challenges

1) Each database type (and version) supports different syntax for creating 'insert if not exists in table' commands, commonly known as an 'upsert'

2) There is no native dataframe 'comparison' functions in Pandas.  Data must be compared using a combination of merge/concat/join statements, then filtered.

# Pandas to-sql 'Upsert' : Methodology

1) Get list of rows from database that are in the current dataframe

2) Remove rows that are in dataframe 1 but not in dataframe 2

3) Write the confirmed new rows to the table

4) Use python 'Threading' library to multiprocess the database write

# Pre-Build Instructions
1) Install Python 2.7.x. Add both the root directory and the /Scripts directory to system PATH (tested on 2.7.11) https://www.python.org/downloads/release/python-2711/


# Build Instructions
1) Git clone this repository

2) Open a cmd window and CD to root directory of this repo.

3) Run command "pip install -r requirements.txt".

# Caveats & To Do's

1) For concurrency, an "Upsert" (Update or Insert) function should still be performed.  If multiple workers can write to the same database table at the same time, the time between checking the database for duplicates and writing the new rows to the database can be significant.  This is a big to-do.

2) Memory limitations - if your analysis table contains more rows than can fit into for worker Python Pandas memory, you will need to select only rows that exist in your dataframe in the read_sql() statement.

3) The clean_df_db_dups() method only speeds up the database insertion if duplicate rows in the dup_cols are found.  If no duplicate rows are found, the methods should be comparable.

4) Ensure that your dataframe column names and the database table column names are compatible - otherwise you will throw sqlalchemy errors related to a column name existing in your dataframe but not in your existing database table.

5) Multi-threading the pd.DataFrame.to_sql() method is still a big opportunity to increase the speed of data insertion for really large data insertion jobs -- for another blog post!



