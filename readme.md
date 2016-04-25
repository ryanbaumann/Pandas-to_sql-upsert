#Pandas to-sql 'Upsert' : Why
Frequently in data analysis workflows, data is ingested from multiple sources into an application (python in this case), analzed in-memory using a library such as Pandas, Scipy, Numpy, or SAS, and then the results need to be written to a database.  If my workflow ingests data that is already in the database, I don't want to write the duplicate rows.

The goal of this library is to extend the Python Pandas to_sql() function to be:

1) Muti-threaded (improving time-to-insert on large datasets)
2) Allow the to_sql() command to run an 'insert if does not exist' to the 
database
3) Perform the data duplication check 'in-memory' 

#Pandas to-sql 'Upsert' : Challenges

1) Each database type (and version) supports different syntax for creating 'insert if not exists in table' commands, commonly known as an 'upsert'

2) There is no native dataframe 'comparison' functions in Pandas.  Data must be compared using a combination of merge/concat/join statements, then filtered.

#Pandas to-sql 'Upsert' : Methodology

1) Get list of rows from database that are in the current dataframe
2) Remove rows that are in dataframe 1 but not in dataframe 2
3) Write the confirmed new rows to the table
4) Use python 'Threading' library to multiprocess the database write

#Pre-Build Instructions
1) Install Python 2.7.x. Add both the root directory and the /Scripts directory to system PATH (tested on 2.7.11) https://www.python.org/downloads/release/python-2711/


#Build Instructions
1) Git clone this repository

2) Open a cmd window and CD to root directory of this repo.

3) Run command "pip install -r requirements.txt".



