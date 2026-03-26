# Databricks notebook source
'''
This file provides the access to the common functions used for interfacing with an Azure SQL database:
- Creating the SQLAlchemy engine to facilitate the authentication, communication
- Writing to the database
'''

# COMMAND ----------

from datetime import datetime
import pandas as pd
import pyodbc
import sqlalchemy
import urllib

# COMMAND ----------

def configure_connection(conn_string):
  '''
  Creates a SQLAlchemy engine to connect to a remote SQL database
  Arguments: 
  -  conn_string: a string (supplied by the Azure resource but modified by us*) containing the database connection details
  -  * Note: the default driver identified in the Azure supplied connection string is ODBC Driver 13, however Ubuntu 18 
  -  (the cluster OS) only supports ODBC Driver 17.  Therefore, ODBC driver 17 is downloaded to the cluster via an init script
  -  registerd in ./init_scripts, and the connection string has been manually updated accordingly.
  Returns:
  -  engine:  a SQLAlchemy engine instance (or None), to be used as an intermediary between Python and the remote db.
  '''
  try:
    quoted = urllib.parse.quote_plus(conn_str)
    engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    return engine
  except Exception as e:
    desc = 'error configuring SQL engine'
    log_error(runner, desc, e)
  return None


def create_spark_schema(d, kw='spark'):
  '''
  Extracts the spark dtypes from the passed dictionary.
  Arguments:
  - d: a dictionary like:
      { key: {table_type: dtype} }
  - kw (default='spark'): a string representing the table_type key
  Returns:
  - spark_schema: a pySpark StructType object
  '''
  spark_schema = StructType() 
  for k,v in d.items():
    spark_schema.add(k, eval(v[kw]), True)
  return spark_schema


def create_sql_schema(d, kw='sql'):
  '''
  Extracts the sql dtypes from the passed dictionary.
  Arguments:
  - d: a dictionary like:
      { key: {table_type: dtype} }
  - kw (default='sql'): a string representing the table_type key
  Returns:
  - sql_schema: a string of comma delimited key and datatype pairs like:
      "<key1> <sql dtype>, <key2> <sql dtype>, ..."
  '''
  sql_schema = ""
  for k,v in d.items():
    sql_schema += k + ' ' + v[kw] + ', '  
  return sql_schema[:-2]


def create_sqlalchemy_args(d, kw='sqlalchemy'):
  '''
  Extracts the sqlalchemy dtypes from the passed dictionary.
  Arguments:
  - d: a dictionary like:
      { key: {table_type: dtype} }
  - kw (default='sqlalchemy'): a string representing the table_type key
  Returns:
  - a list of sqlalchemy.Column objects
  '''
  return [sqlalchemy.Column(k, eval(v[kw])) for k,v in d.items()]


def create_table(engine, table, runner, check=False):
  '''
  Inspects the SQL database for the specified table, creating it if necessary.
  Arguments:
  - engine: a sqlalchemy engine object
  - table: a string representing the name of the SQL table to load
  - runner:  A mount_database.Runner object used to hold client specific details
  - check: a flag to control printing details to the console
  Returns:
  - True if the table exists (or was created), False otherwise  
  '''
  try:
    insp = sqlalchemy.inspect(engine)
    if insp.has_table(table):
      msg = 'Found table %s' % table
    else:
      msg = 'Creating table %s' % table
      metadata.create_all(engine)

    log_message(runner, msg)  
    if check:
      print('table exists: ', insp.has_table(table))

    return True
  except Exception as e:
    msg = "Unable to find or create table: %s: " % e
    log_message(runner, e)
    return False


def generate_schemas(path, dataset, table, metadata):
  '''
  Wrapper around individual schema generating functions.  
    See individual functions for details.
  Arguments:
  - path: the path to the schema.json file, e.g. filemap.SCHEMA
  - dataset: a string identifying the dataset being processed, 
      typically an output of the utilities.get_context() function
  - table: a string representing the SQL table name
  - metadata: a sqlalchemy.MetaData() object
  Returns:
  - spark_schema: a pySpark StructType object
  - sql_schema:  a string of comma delimited key and datatype pairs like:
      "<key1> <sql dtype>, <key2> <sql dtype>, ..."
  - sqlalchemy_schema: a sqlalchemy.Table() object
  '''
  d = get_schema_details(path)
  spark_schema = create_spark_schema(d[dataset])
  
  sql_schema = create_sql_schema(d[dataset])
  
  args = create_sqlalchemy_args(d[dataset])  
  sqlalchemy_schema = sqlalchemy.Table(table, metadata, *args)
  
  return spark_schema, sql_schema, sqlalchemy_schema


def spark_read_file(path, adls_url, filename, spark_schema, check=False):
  '''
  Reads the saved dataset into a pySpark dataframe.
  Arguments:
  - path: a list of strings represnting the path in ADLS2 to the persisted csv file.
  - adls_url: a string representing the url to the ADLS2 instance
  - filename: a string representing the csv file to load
  - spark_schema: a pySpark StructType object
  - check: a flag to control printing details to the console
  Returns:
  - spark_df: a pySpark dataframe
  '''  
  url = os.path.join(adls_url, *path, filename)
  spark_df = spark.read.format('csv').option("header", "true").schema(spark_schema).load(url)

  if check:
    print("number of rows: ", spark_df.count())
    print('*' * 50)
    print("dataframe head: ", spark_df.head())

  return spark_df


def write_to_db(dataframe, engine, table, method = 'replace', chunksize=None, dtype = None):
  '''
  Inserts a given dataframe into a remote database table using the connection engine provided.
  Arguments:
  -  dataframe: the Pandas dataframe to write to the database
  -  engine: a SQLAlchemy object used to interface with the SQL server
  -  table: a string representing the name of the table where data will be written
  -  method: a string representing the 'if_exists' argument required to be passed to
      the Pandas.dataframe.to_sql() method.  When set to 'replace', the new data will
      overwrite the previous data.  When set to 'append', the new data will be appended.
  Returns:
  -  Nothing
  '''
  try:    
    dataframe.to_sql(table, con = engine, if_exists = method, index=False, chunksize=chunksize, dtype=dtype)
  except Exception as e:
    desc = 'error writing to database'
    log_error(runner, desc, e)
    
    
