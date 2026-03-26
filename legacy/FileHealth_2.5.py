# Databricks notebook source
'''
File Health
'''

# COMMAND ----------

import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import os
import pandas as pd
import shutil
import time

import warnings
warnings.filterwarnings("ignore")


# COMMAND ----------

# MAGIC %run ./mount_datalake

# COMMAND ----------

# MAGIC %run ./logger

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %run ./features

# COMMAND ----------

# MAGIC %run ./FileHealth_functions_modular_2.4

# COMMAND ----------

# # establish context
try:
  client = dbutils.widgets.get('client')
  dataset = dbutils.widgets.get('dataset')
  process = dbutils.widgets.get('process')
  FH_PERIOD = dbutils.widgets.get('fh_period')
except:
  client = 'NJH'
  dataset = 'FileHealth_2.2'
  process = '/Shared/NJH/Curate/FileHealth_2.3'
  FH_PERIOD = 'fy'

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

# Constants
FH_COLS = [
  'ReportPeriod', 'EndofRptPeriod',
  'GiftFiscal', 'Period', 'calcDate', 
  'FHGroupDetail', 'FHGroup'
]

# COMMAND ----------

# read inputs
try:
  encoding = "ISO-8859-1"
  file = 'StagedForFH_wFilters.csv'
  df = pd.read_csv(filemap.STAGED + file, encoding=encoding)#.sample(frac=.01)
except Exception as e:
  desc = 'error reading file'
  log_error(runner, desc, e)

print(df.shape)
df.head()

# COMMAND ----------

# Format dataframe and pre-process

try:
  schema = get_schema_details(filemap.SCHEMA)
  dec = flatten_schema(schema[dataset])
  FYM = int(schema['firstMonthFiscalYear'])

  all_cols = list(dec.keys())
  cols = list(set(all_cols) ^ set(FH_COLS))
  df = preprocess(df[cols], FYM, f=None)
except Exception as e:
  desc = 'error formatting and pre-processing data'
  log_error(runner, desc, e)

print(df.shape)
df.head()

# COMMAND ----------

# Process File Health
try:
  # clear directories each run, added 7/28/2022    
  shutil.rmtree(os.path.join(filemap.CURATED, 'FileHealth', 'subsets3', FH_PERIOD)) 
  
  NUMBER_OF_SUBSETS = 10
#   dataset = 'FileHealth_2.2'
  # main_parallel_b(df, NUMBER_OF_SUBSETS, FH_PERIOD, filemap, runner, dataset, check=True)
  main(df, NUMBER_OF_SUBSETS, FH_PERIOD, filemap, runner, dataset, FYM, check=True)
except Exception as e:
  desc = 'error processing file health'
  log_error(runner, desc, e)

# COMMAND ----------

# # write to log file in ADLS2
# logger.critical("%s - exiting notebook: %s" % (str(datetime.now()), process))
# update_adls(logfile_loc, filemap.LOGS, logfile_name)

# COMMAND ----------

# dbutils.notebook.exit(json.dumps({
#   "status": "OK",
#   "timestamp (UTC)": str(datetime.now())
# }))

# COMMAND ----------

