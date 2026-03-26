# Databricks notebook source
'''
initial ingestion
'''

# COMMAND ----------

from datetime import datetime
import numpy as np

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# MAGIC %run ./mount_datalake

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
  client = 'TCI'
  process = client

# establish file storage directories
filemap = Filemap(client)

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
  local_files = cleanup_raw(filemap, subs=['Data/', 'Matchbacks/', 'Reference/'])
  
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
  raise(e)

# COMMAND ----------

columns

# COMMAND ----------

print('client files (initial): ', client_files)
if client in ['MC']:
  client_files = [x for x in client_files if 'code' not in x.lower()]
client_files = [x for x in client_files if '.txt' not in x.lower()]
print('client files (final): ', client_files)

# COMMAND ----------

if not client_files:
  print("%s - exiting notebook: %s" % (str(datetime.now()), client))  
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
  min_date = df['GiftDate'].min()
  
  # open master historical data and combine datasets
  try:
    master = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
    if 'Special' in schema.keys():
      if schema['Special']['IgnoreDates']:
        pass
    else:
      mask = master['GiftDate'] < min_date
      master = master[mask]
  except FileNotFoundError:
    master = pd.DataFrame()

  #align headers
  df_cols, master_cols = reorder_col_names(df.columns, master.columns)
  df = df[df_cols]
  master = master[master_cols] 
  
  # remove pii
  pii_cols = schema['PII_Columns']
  df = _remove_pii(df, pii_cols)
  
  # merge dataframes
  if client == 'FWW':
    df.columns = [x.replace(' ','') for x in df.columns]
  print('master columns: ', master.columns)
  print('df columns: ', df.columns)
  df = pd.concat([master,df])
  
  # apply client specific preprocessing
  df = apply_client_specific(df, client)
  
  # de-duplicate dataset
  df = apply_dedupe(df, duplicates)
  
  # cast columns and drop invalid dates
  df['GiftDate'] = pd.to_datetime(df['GiftDate'])
  df = drop_future_dates(df, 'GiftDate')
  df['GiftAmount'] = format_currency(df['GiftAmount']) 
    
  # cast columns for conversion to parquet
  for c in parquet_cols:
    if c in df.columns:
      df[c] = df[c].fillna(value='None').astype(str)
  
except Exception as e:
  raise(e)

# Write to ADLS2
try:
  df.to_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
except Exception as e:
  raise(e)
  
try:
  mask = df.GiftDate.dt.year > datetime.now().year - 6
  df.loc[mask].to_csv(os.path.join(filemap.MASTER, 'Data_FiveYears.csv'), index=False)
except Exception as e:
  raise(e)
  
  
print(df.shape)
print(df.columns)
# df.head()

# (282669, 28)

# COMMAND ----------

# remove pii
client_files = [x.split('/')[-1] for x in client_files]
print(client_files)
remove_pii(client, client_files, etl=True)

# archive files
write_to_archive(client, client_files, etl=True)
  

# COMMAND ----------

# apply client specific suppressions
print(df.shape)
if client == 'TCI' and 'GiftSource' not in df.columns:
  pass
else:
  for k in suppressions.keys():
    df = apply_func(df, suppressions, k)
print(df.shape)

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
  raise(e)

print(df.shape)
print(FH_COLS)
df.head()

# COMMAND ----------

mask = df['GiftDate'].dt.year > 2016
df.groupby(df['GiftDate'].dt.year)['GiftAmount'].sum()

# COMMAND ----------

# Stage file for FH
try:
  dataset='FileHealth'
  dec = flatten_schema(schema[dataset])
  dec = shrink_dec(dec, FH_COLS)

  df_fh = df[FH_COLS].dropna(subset=['FirstGiftDate'])
  df_fh = assert_declarations(df_fh, dec, view=True)
  df_fh.to_csv(os.path.join(filemap.STAGED, 'StagedForFH_wFilters.csv'), index=False)
  
except Exception as e:
  raise(e)
  

# COMMAND ----------

mask = df_fh['GiftDate'].dt.year > 2016
df_fh.groupby(df_fh['GiftDate'].dt.year)['GiftAmount'].sum()

# COMMAND ----------

try:
  # record the names of the files that make up the transaction history
  record_file_list(client_files, cf)  
except Exception as e:
  raise(e)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))

# COMMAND ----------

