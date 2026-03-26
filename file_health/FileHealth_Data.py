# Databricks notebook source
# MAGIC %md # File Health Data (ETL1)
# MAGIC

# COMMAND ----------

# MAGIC %md ## Runs and Imports

# COMMAND ----------

# DBTITLE 1,imports
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import os
import pandas as pd
import shutil
import time

import warnings
warnings.filterhttps://adb-1969403299592450.10.azuredatabricks.net/explore/data?o=1969403299592450$0warnings("ignore")


# COMMAND ----------

# DBTITLE 1,run mount_datalake
# MAGIC %run ./mount_datalake

# COMMAND ----------

# DBTITLE 1,run utilities
# MAGIC %run ./utilities

# COMMAND ----------

# DBTITLE 1,run parser
# MAGIC %run ./parser

# COMMAND ----------

# DBTITLE 1,run FileHealth_AllClients
# MAGIC %run ./FileHealth_AllClients

# COMMAND ----------

# MAGIC %md ## Establish Client, Context, and Data

# COMMAND ----------

# DBTITLE 1,Establish Client/Context
# # establish context
try:
  client = dbutils.widgets.get('client')
  dataset = dbutils.widgets.get('dataset')
  process = dbutils.widgets.get('process')
  FH_PERIOD = dbutils.widgets.get('fh_period')
except:
  client = 'SICL'
  dataset = 'FileHealth'
  process = client
  FH_PERIOD = 'fy'

# establish file storage directories
filemap = Filemap(client)

# COMMAND ----------

print('FH_PERIOD: ', FH_PERIOD)

# COMMAND ----------

# DBTITLE 1,FH_COLS definition
# Constants
FH_COLS = [
  'ReportPeriod', 'EndofRptPeriod',
  'GiftFiscal', 'Period', 'calcDate', 
  'FHGroupDetail', 'FHGroup'
]

# COMMAND ----------

# DBTITLE 1,Read in df from StagedForFH_wFilters
# read inputs
try:
  encoding = "ISO-8859-1"
  file = 'StagedForFH_wFilters.csv'
  df = pd.read_csv(filemap.STAGED + file, encoding=encoding)#.sample(frac=.01)
  df['GiftDate'] = pd.to_datetime(df['GiftDate'], format='%Y-%m-%d')
except Exception as e:
  raise(e)

print(df.shape)
df.head()

# COMMAND ----------

df.GiftDate.describe()

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md ## Format and Process Data

# COMMAND ----------

# DBTITLE 1,Format dataframe and pre-process
# Format dataframe and pre-process

try:
  schema = get_schema_details(filemap.SCHEMA)
  dec = flatten_schema(schema[dataset])
  
  mrm = df['GiftDate'].max().month
  # insert MC override here
  print('max gift date: ', df['GiftDate'].max())
  print('most recent month: ', mrm)
  fym_map = {'cy': 1, 'covid': 4, 'r12': mrm, 'cytd': 1}
  if FH_PERIOD in fym_map.keys():
    FYM = fym_map[FH_PERIOD]
  else: 
    FYM = int(schema['firstMonthFiscalYear'])
  print('fh_period: ', FH_PERIOD)
  print('month: ', FYM)

  all_cols = list(dec.keys())
  cols = list(set(all_cols) ^ set(FH_COLS))
  df = preprocess(df[cols], FYM, f=None)
except Exception as e:
  raise(e)

print(df.dtypes)
print(df.shape)
df.head()

# COMMAND ----------

# DBTITLE 1,TLF Temporary Fix
#Update columns with mixed boolean types to strings
if client == 'TLF':
    df['TLF_DAF'] = df['TLF_DAF'].astype(str)
    df['TLF_IRA'] = df['TLF_IRA'].astype(str)
if client == 'FFB':
    df['FFB_MediaOutletCode'] = df['FFB_MediaOutletCode'].astype(str)
    df['FFB_SegmentCode'] = df['FFB_SegmentCode'].astype(str)


# COMMAND ----------

# MAGIC %md ## Process and Write to Curated

# COMMAND ----------

# DBTITLE 1,Process File Health
# Process File Health
try:
  # clear directories each run, added 7/28/2022    
  shutil.rmtree(os.path.join(filemap.CURATED, 'FileHealth', 'subsets3', FH_PERIOD)) 
  
  NUMBER_OF_SUBSETS = 10
  main(df, NUMBER_OF_SUBSETS, FH_PERIOD, filemap, dataset, FYM,n_years=6, check=True)
except Exception as e:
  raise(e)

# COMMAND ----------

# DBTITLE 1,Exit Notebook
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))