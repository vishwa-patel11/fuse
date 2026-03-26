# Databricks notebook source
'''
Measures for FileHealth
'''

# COMMAND ----------

from itertools import chain
import os
import pandas as pd
import sqlalchemy

from functools import reduce
from pyspark.sql import DataFrame

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %run ./mount_datalake

# COMMAND ----------

# MAGIC %run ./logger

# COMMAND ----------

# MAGIC %run ./db_interface

# COMMAND ----------

# # establish context
try:
  client = dbutils.widgets.get('client')
  dataset = dbutils.widgets.get('dataset')
  process = dbutils.widgets.get('process')
except:
  pass
  client = 'DAV'
  dataset = 'FileHealth'
  process = '/Shared/DAV/Curate/FileHealth_2.3'

# establish file storage directories
filemap = Filemap(client)
  
# establish context
logfile_name, logfile_loc = copy_logfile(filemap.LOGS)

# instantiate logger
logger = create_logger(logfile_loc)

# #instantiate a Runner object
runner = Runner(process, logfile_loc, logfile_name, filemap.LOGS)

logger.critical("%s - logging for notebook: %s" % (str(datetime.now()), process))
update_adls(logfile_loc, filemap.LOGS, logfile_name)

# COMMAND ----------

# constants
FH_PERIODS = ['fy']#, 'fytd', 'r12']  
FOLDER = 'FileHealth/subsets4'

# helpers
def union_all(*dfs):
  return reduce(DataFrame.union, dfs)



# COMMAND ----------

# collect data

try:  
  dfs = {}  
  for fhp in FH_PERIODS:
    dir_path = os.path.join(filemap.CURATED, FOLDER, fhp)
    years = os.listdir(dir_path)
    for year in years:
      file_path = os.path.join(dir_path, year, 'fh').split('/dbfs')[-1]
      dfs[year+fhp] = spark.read.parquet(file_path)
  df = union_all(*dfs.values())
  
  # cast date
  cols = ['GiftDate', 'calcDate', 'EndofRptPeriod']
  for c in cols:
    df = df.withColumn(c, F.to_date(F.col(c)))
    
except Exception as e:
  desc = 'error collecting data'
  log_error(runner, desc, e)
  
print(df.count(), len(df.columns))
df.show(n=3)

# COMMAND ----------

# df = df.toPandas()
# print(df.shape)
# df.head()

# COMMAND ----------



# COMMAND ----------

# # df.to_parquet(os.path.join(filemap.CURATED, 'FileHealth', 'DAV_FH_fytest.parquet'), index=False)
# filename = 'FyTest'
# path = os.path.join(filemap.CURATED, 'FileHealth', filename).split('/dbfs')[-1]
# df.write.parquet(path, mode='overwrite')

# path = os.path.join(filemap.CURATED, 'FileHealth', 'FyTest')
# files = os.listdir(path)
# print(len(files))
# for f in files:
#   if '.parquet' not in f:
#     if os.path.isfile(os.path.join(path, f)):
#       os.remove(os.path.join(path, f))
#     else:
#       os.rmdir(os.path.join(path, f))
# print(len(files))
    


# COMMAND ----------



# COMMAND ----------

# prepare dataframe for writing to ADLS2 
try:
  mapper = {  
  'DateType': 'DATE',
  'DoubleType': 'FLOAT',
  'IntegerType': 'INTEGER',
  'StringType': 'VARCHAR(100)',
  'TimestampType': 'DATE',
  }
  df_schema = {dfs.name: mapper[str(dfs.dataType)] for dfs in df.schema}
  df_schema = [' '.join([k,v]) for k,v in df_schema.items()]

  sql_schema = ''
  for x in df_schema:
    sql_schema += (x + ', ') 

  sql_schema = sql_schema[:-2]
  

 
  
except Exception as e:
  desc = 'error preparing dataframe for writing to disk'
  log_error(runner, desc, e)


sql_schema

# COMMAND ----------

# create context
DATALAKE_URL = os.environ["ADLS2_URL"]
ADLS2_NAME = os.environ["ADLS2_NAME"]
ADLS2_KEY = os.environ["ADLS2_KEY"]

spark.conf.set("fs.azure.account.key." + ADLS2_NAME, ADLS2_KEY)
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# identify database and table name
DATABASE = 'msdbidb'
TABLE = "FH_Test"
print('TABLE: ', TABLE)


# create sqlalchemy engine
conn_str = os.environ["DB_CONN_STR"]
engine = configure_connection(conn_str)
metadata = sqlalchemy.MetaData()


# COMMAND ----------

# create table if necessary

success = False
tries = 0
while not success:
  print('number of tries: ', tries)
  try:
    table_exists = create_table(engine, TABLE, runner, check=True)
    success = True
  except:
    tries += 1





# COMMAND ----------

spark_url = "com.microsoft.sqlserver.jdbc.spark"
if table_exists:
  # write to SQL DB
  df.write.mode("overwrite") \
      .format(spark_url) \
      .option("url", f"jdbc:sqlserver://msdbi.database.windows.net:1433;databaseName={DATABASE};") \
      .option("dbtable", TABLE) \
      .option("user", os.environ["SS_UID"]) \
      .option("password", os.environ["SS_PWD"] ) \
      .option("createTableColumnTypes", sql_schema) \
      .save()

# COMMAND ----------

# if table_exists:
#   path = ["Curated", client, "CampPerf"]
#   spark_df = spark_read_file(path, DATALAKE_URL, "CampPerf.csv", spark_schema, check=True)
#   # write to SQL DB
#   spark_df.write.mode("overwrite") \
#       .format("jdbc") \
#       .option("url", f"jdbc:sqlserver://msdbi.database.windows.net:1433;databaseName={DATABASE};") \
#       .option("dbtable", TABLE) \
#       .option("user", os.environ["SS_UID"]) \
#       .option("password", os.environ["SS_PWD"] ) \
#       .option("createTableColumnTypes", sql_schema) \
#       .save()