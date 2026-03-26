# Databricks notebook source
'''
Aggregate data for FileHealth
'''

# COMMAND ----------

from datetime import datetime
import os
import pandas as pd


# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %run ./mount_datalake

# COMMAND ----------

# # establish context
try:
  client = dbutils.widgets.get('client')
  dataset = dbutils.widgets.get('dataset')
  process = dbutils.widgets.get('process')
except:
  pass
  client = 'MC'
  dataset = 'FileHealth'
  process = ''

# establish file storage directories
filemap = Filemap(client)

# COMMAND ----------

# constants

FH_PERIODS = ['fy', 'fytd', 'r12', 'cy', 'cytd']  
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
  raise e
  
print(df.shape)
df.head()

# COMMAND ----------

# prepare dataframe for writing to ADLS2 
try:
  
  # identify columns from schema
  schema = get_schema_details(filemap.SCHEMA)
  dec = flatten_schema(schema[dataset])
  
  FILENAME = '%s_FH_dataset.csv' % client
  df.to_csv(os.path.join(filemap.CURATED, FILENAME), index=False)
  
except Exception as e:
  raise e

df.shape
  

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))

# COMMAND ----------

