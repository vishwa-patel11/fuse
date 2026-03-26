# Databricks notebook source
'''
initial ingestion
'''

# COMMAND ----------

import numpy as np

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# MAGIC %run ./mount_datalake

# COMMAND ----------

# MAGIC %run ./logger

# COMMAND ----------

# MAGIC %run ./archiver

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %run ./RFM_Lookup

# COMMAND ----------

# MAGIC %run ./parser

# COMMAND ----------

# # establish context
try:
  client = dbutils.widgets.get('client')
  process = dbutils.widgets.get('process')
except:
  client = 'NJH'
  process = '/Shared/AFHU/Stage/Client_2.4'

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

# MAGIC %run ./transfer_files

# COMMAND ----------

# MAGIC %md ## Transfer files

# COMMAND ----------

try:
  # Extract details from config file a.k.a "schema"
  schema = get_schema_details(filemap.SCHEMA)
  ancillaries, columns, data_mapper, directories, duplicates, encoding, exclusions, fh_keys, key_mapper, fy_month, parquet_cols, suppressions = unpack_schema(schema)
  
  # clean up directory
  local_files = cleanup_raw(filemap, runner, subs=['Data/', 'Matchbacks/', 'Reference/'])
  
  # transfer files from SharePoint to ADLS2
  try:
    cf = pd.read_csv(os.path.join(filemap.MASTER, 'filenames.csv'))
  except FileNotFoundError:
    cf = pd.DataFrame(columns=['FileName'])
    
  files = cf['FileName'].unique()
  client_files = apply_transfer(directories, files) 
      
  # handle ancillary files
  handle_ancillaries(filemap.RAW, ancillaries, check=True)
  
  # handle transaction files
  handle_transactions(filemap.RAW)
  
except Exception as e:
  desc = 'error transferring data'
  log_error(runner, desc, e)

# COMMAND ----------

client_files

# COMMAND ----------

if not client_files:
  # write to log file in ADLS2
  logger.critical("%s - exiting notebook: %s" % (str(datetime.now()), process))
  update_adls(logfile_loc, filemap.LOGS, logfile_name)
  
  dbutils.notebook.exit(json.dumps({
    "status": "OK",
    "timestamp (UTC)": str(datetime.now())
  }))

# COMMAND ----------

# MAGIC %md ## Perform client specific tasks

# COMMAND ----------


# Format data
  
try:
  
  # collect individual data files
  df = collect_transaction_files(filemap.RAW, columns, encoding, key_mapper, schema['DateFormat'])
  
  # open master historical data and combine datasets
  try:
    master = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
  except FileNotFoundError:
    master = pd.DataFrame()
  
  #align headers
  df_cols, master_cols = reorder_col_names(df.columns, master.columns)
  df = df[df_cols]
  master = master[master_cols]
  
  # merge dataframes
  df = master.append(df)
  
  # apply client specific preprocessing
  df = apply_client_specific(df, client)
  
  # de-duplicate dataset
  df = apply_dedupe(df, duplicates)   
  
  # cast columns and drop invalid dates
  df['GiftDate'] = pd.to_datetime(df['GiftDate'])
  df = drop_future_dates(df, 'GiftDate')
  df['GiftAmount'] = format_currency(df['GiftAmount'])
  
  for k in suppressions.keys():
    df = apply_func(df, suppressions, k) 
    
  # cast columns for conversion to parquet
  for c in parquet_cols:
    df[c] = df[c].fillna(value='None').astype(str)
  
except Exception as e:
  desc = 'error formatting data'
  log_error(runner, desc, e)

# Write to ADLS2
try:
  df.to_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
except Exception as e:
  desc = 'error writing to file'
  log_error(runner, desc, e)
  
  
print(df.shape)
df.head()

# (282669, 28)

# COMMAND ----------

# spot check FY values
df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', fy_month)
# df['GiftFiscal'] = df['GiftDate'].dt.year
mask = df['GiftFiscal'] > 2016
df[mask].groupby('GiftFiscal')['GiftAmount'].sum()


# COMMAND ----------

# Add file health fields
try:
  FH_COLS = ['DonorID', 'GiftDate', 'GiftAmount', 'FirstGiftDate']
  df = get_fh_FirstGiftDate(df)
  
  filters = get_fh_filters(fh_keys) 
  print('filters: ', filters)
  FH_COLS.extend(filters)
  
  for f in filters:
    df = apply_filters(df, f)
    
except Exception as e:
  desc = 'error adding First Gift Date'
  log_error(runner, desc, e)

print(df.shape)
print(FH_COLS)
df.head()

# COMMAND ----------

# Stage file for FH
try:
  dataset='FileHealth'
  dec = flatten_schema(schema[dataset])
  dec = shrink_dec(dec, FH_COLS)

  df_fh = df[FH_COLS].dropna(subset=['FirstGiftDate'])
  df_fh = assert_declarations(df_fh, dec, runner, view=True)
  df_fh.to_csv(os.path.join(filemap.STAGED, 'StagedForFH_wFilters.csv'), index=False)
  
except Exception as e:
  desc = 'error writing staged file to disk'
  log_error(runner, desc, e)
  

# COMMAND ----------

try:
  # record the names of the files that make up the transaction history
  record_file_list(client_files, cf)  
except Exception as e:
  desc = 'error transferring data'
  log_error(runner, desc, e)

# COMMAND ----------

# write to log file in ADLS2
logger.critical("%s - exiting notebook: %s" % (str(datetime.now()), process))
update_adls(logfile_loc, filemap.LOGS, logfile_name)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))