# Databricks notebook source
'''
Measures for FileHealth
'''

# COMMAND ----------

import os
import pandas as pd


# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %run ./mount_datalake

# COMMAND ----------

# MAGIC %run ./logger

# COMMAND ----------

# # establish context
try:
  client = dbutils.widgets.get('client')
  dataset = dbutils.widgets.get('dataset')
  process = dbutils.widgets.get('process')
except:
  pass
  client = 'NJH'
  dataset = 'FileHealth'
  process = '/Shared/NJH/Curate/FileHealth_2.3'

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

FH_PERIODS = ['fy', 'fytd', 'r12']  
FOLDER = 'FileHealth/subsets3'

# COMMAND ----------

# collect data

try:  
  df = pd.DataFrame()
  
  for fhp in FH_PERIODS:
    dfx = pd.DataFrame()
    years = os.listdir(os.path.join(filemap.CURATED, FOLDER, fhp))

    for year in years:
      _df = pd.DataFrame()
      path = '/'.join([filemap.CURATED, FOLDER, fhp, year])

      for f in os.listdir(path):
        _df = _df.append(pd.read_parquet(os.path.join(path, f))) 

      dfx = dfx.append(_df)   

    df = df.append(dfx)

except Exception as e:
  desc = 'error collecting data'
  log_error(runner, desc, e)
  
print(df.shape)
df.head()

# COMMAND ----------

# prepare dataframe for writing to ADLS2 
try:
  
  # identify columns from schema
  schema = get_schema_details(filemap.SCHEMA)
  dec = flatten_schema(schema[dataset])
  
  # Assert the data shape and type are as expected for inserting to SQL db
  df = assert_declarations(df, dec, runner, view=True)
  
  FILENAME = '%s_FH_dataset.csv' % client
  df.to_csv(os.path.join(filemap.CURATED, FILENAME), index=False)
  
except Exception as e:
  desc = 'error preparing dataframe for writing to disk'
  log_error(runner, desc, e)

df.shape
  

# COMMAND ----------



# COMMAND ----------

