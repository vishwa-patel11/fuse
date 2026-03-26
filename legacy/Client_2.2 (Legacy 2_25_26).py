# Databricks notebook source
# MAGIC %md # Client_2.2 (ETL1 & ETL2 Production)

# COMMAND ----------

# MAGIC %md ## Runs and Imports
# MAGIC

# COMMAND ----------

'''
ingest transaction files and add to data history
stage data for File Health
'''

# COMMAND ----------

# DBTITLE 1,imports
from datetime import datetime
import numpy as np
import json
from datetime import datetime
from pytz import timezone

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# DBTITLE 1,mount datalake
# MAGIC %run ./mount_datalake

# COMMAND ----------

# DBTITLE 1,archiver
# MAGIC %run ./archiver

# COMMAND ----------

# DBTITLE 1,utilities
# MAGIC %run ./utilities

# COMMAND ----------

# DBTITLE 1,RFM Lookup
# MAGIC %run ./RFM_Lookup

# COMMAND ----------

# DBTITLE 1,parser
# MAGIC %run ./parser

# COMMAND ----------

# DBTITLE 1,transfer files
# MAGIC %run ./transfer_files

# COMMAND ----------

# MAGIC %md ## Choose Client

# COMMAND ----------

# DBTITLE 1,Choose the client we're running for
# # establish context - use the globally passed client variable (from ETL workflow) if it exists, otherwise fall back to the except statement to execute the script for the manually set variable 
try:
  client = dbutils.widgets.get('client')
except:
  client = 'FS' #["CARE", "FFB", "NJH", "HKI"]

# establish file storage directories
filemap = Filemap(client)

# COMMAND ----------

client

# COMMAND ----------

# MAGIC %md ## Identify files

# COMMAND ----------

# DBTITLE 1,Read the schema & processed file log
try:
  # read config file
  schema = get_schema_details(filemap.SCHEMA)

  # read log of processed files
  try:
    cf = pd.read_csv(os.path.join(filemap.MASTER, 'filenames.csv'))
  except FileNotFoundError:
    cf = pd.DataFrame(columns=['FileName'])
  
except Exception as e:
  raise(e)

schema

# COMMAND ----------

# DBTITLE 1,Find all the raw files present in RawData
try:
  client_files = [
    x for x in os.listdir(os.path.join(filemap.RAW, 'RawData')) \
    #if x not in cf.FileName.unique()
  ]
except FileNotFoundError:
  client_files = []
client_files

# COMMAND ----------

# DBTITLE 1,Filter Files 'code' (MC) and 'txt'
print('client files (initial): ', client_files)
if client in ['MC']:
  client_files = [x for x in client_files if 'code' not in x.lower()]
client_files = [x for x in client_files if '.txt' not in x.lower()]
print('client files (final): ', client_files)

# COMMAND ----------

# DBTITLE 1,use_current_data?
#Use data.parquet file if no new transaction files are in RawData
# use_current_data = False
# use_current_data
try:
  dbutils.widgets.text("use_current_data", False) # From manual run (default to False)
  use_current_data = dbutils.widgets.get("use_current_data")
except:
  use_current_data = False # Change condition within notebook here

# COMMAND ----------

# DBTITLE 1,Full ETL Manual Run : use_current_data option if run manually
# Takes "use_current_data" condition from Full ETL Manual Run Notebook as input, if ran
try:
    use_current_data = (dbutils.widgets.get("use_current_data") == "True")
except:
    pass

use_current_data

# COMMAND ----------

# DBTITLE 1,CURRENT DATA TEMP
# use_current_data = True


# COMMAND ----------

# DBTITLE 1,Exits if client_files is empty
# Exit only if client_files is empty AND use_current_data is False
if not client_files and not use_current_data:
    print(f"{datetime.now()} - exiting notebook: {client}")
    dbutils.notebook.exit(json.dumps({
        "status": "OK",
        "timestamp (UTC)": str(datetime.now())
    }))
else:
    print("Continuing notebook — either files were found or use_current_data=True")

# COMMAND ----------

# MAGIC %md ## Build data.parquet and perform client specific tasks

# COMMAND ----------

status = bool(client_files or not use_current_data)
print("Status:", status)

# COMMAND ----------

# DBTITLE 1,Build/Update Data.parquet and Data_Five_Years.csv Files
# Run full ETL if:
# - client_files exist  OR
# - use_current_data is False
if client_files or not use_current_data:

    # Format data
    try:
        df = collect_transaction_files(
            folder=filemap.RAW,
            columns=schema['Columns'],
            encoding=schema['Encoding'],
            key_mapper=schema['KeyMapper'],
            date_format=schema['DateFormat'],
            directory='RawData',
            schema=schema
        )

        from datetime import datetime
        from pytz import timezone
        df['Transaction File Name'] = ", ".join(client_files)
        df['Row Added At'] = datetime.now(tz=timezone('US/Eastern')).replace(microsecond=0)
        # ---- min/max date logic ----
        if client != 'NJH':
            min_date = df['GiftDate'].min()
        else:
            if 'Posted_Gift_Date' in df.columns:
                df["Posted_Gift_Date"] = pd.to_datetime(df["Posted_Gift_Date"], errors='coerce')
                min_date = df['Posted_Gift_Date'].min()
            else:
                min_date = df['GiftDate'].min()

        max_date = df['GiftDate'].max()
        # ---- load master historical ----
        try:
            master = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
            if schema.get('Special', {}).get('IgnoreDates') == True:
                pass
            else:
                mask = (master['GiftDate'] < min_date) | (master['GiftDate'] > max_date)
                master = master[mask]

        except FileNotFoundError:
            master = pd.DataFrame()

        # ---- align headers ----
        df_cols, master_cols = reorder_col_names(df.columns, master.columns)
        df = df[df_cols]
        master = master[master_cols]

        # ---- remove PII ----
        df = _remove_pii(df, schema['PII_Columns'])

        # ---- combine ----
        if client == 'FWW':
            df.columns = [x.replace(' ','') for x in df.columns]
        df = pd.concat([master, df])
        # ---- client-specific processing ----
        df = apply_client_specific(df, client)
        # ---- dedupe ----
        df = apply_dedupe(df, schema['DuplicateKeys'])
        # ---- casting and cleanup ----
        df['GiftDate'] = pd.to_datetime(df['GiftDate'], errors='coerce')
        df = drop_future_dates(df, 'GiftDate')
        df['GiftAmount'] = format_currency(df['GiftAmount'])

        for c in schema['ParquetCols']:
            if c in df.columns:
                df[c] = df[c].fillna('None').astype(str)

    except Exception as e:
        raise e

    # Always add processed timestamp
    df['data_processed_at'] = datetime.now(tz=timezone('US/Eastern')).replace(microsecond=0)

    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns}")
    print(f"Max Gift Date: {df.GiftDate.max()}")

# ───────────────────────────────────────────────
# ELSE → load existing parquet (skip ETL)
# ───────────────────────────────────────────────
else:
    print("No client files found & use_current_data=True → loading existing data")
    df = load_parquet(client).sort_values('GiftDate', ascending=False)
    # ---- client-specific processing ----
    df = apply_client_specific(df, client)
    # ---- dedupe ----
    df = apply_dedupe(df, schema['DuplicateKeys'])
    # ---- casting and cleanup ----
    df['GiftDate'] = pd.to_datetime(df['GiftDate'], errors='coerce')
    df = drop_future_dates(df, 'GiftDate')
    df['GiftAmount'] = format_currency(df['GiftAmount'])

    for c in schema['ParquetCols']:
        if c in df.columns:
            df[c] = df[c].fillna('None').astype(str)

    # Always add processed timestamp
    df['data_processed_at'] = datetime.now(tz=timezone('US/Eastern')).replace(microsecond=0)

    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns}")
    print(f"Max Gift Date: {df.GiftDate.max()}")

# COMMAND ----------

# DBTITLE 1,Arrange df Columns for Easier Preview
# Define the preferred order for the first four columns
preferred_order = ['GiftDate', 'DonorID', 'GiftID', 'GiftAmount', 'CampaignName', 'CampaignCode', 'SourceCode']

# Identify which columns from the preferred order actually exist in the dataframe
existing_columns = [col for col in preferred_order if col in df.columns]

# Get the remaining columns that are not in the preferred order
remaining_columns = [col for col in df.columns if col not in existing_columns]

# Reorder the dataframe with existing preferred columns first, followed by the rest
df = df[existing_columns + remaining_columns]

#Sort by descending GiftDate
df = df.sort_values('GiftDate', ascending = False)

df.head(2)

# COMMAND ----------

df

# COMMAND ----------

# DBTITLE 1,QC df duplicates by GiftID
#Review duplicates
duplicates = df[df.duplicated(subset='GiftID', keep=False)].sort_values('GiftID')
duplicates

# COMMAND ----------

# DBTITLE 1,QC Compare Data.parquet Gift Counts by Year
# Calculate new Data.parquet counts
new_count = df.groupby(df['GiftDate'].dt.year).size().reset_index(name='Updated Gift Counts')

try:
    # Calculate previous Data.parquet counts
    previous_parquet = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
    previous_count = previous_parquet.groupby(previous_parquet['GiftDate'].dt.year).size().reset_index(name='Previous Gift Counts')

    # Merge both dataframes on GiftDate year
    comparison_gift_count_df = new_count.merge(previous_count, on='GiftDate', how='outer').fillna(0)

    # Convert to integers to remove .0
    comparison_gift_count_df['Updated Gift Counts'] = comparison_gift_count_df['Updated Gift Counts'].astype(int)
    comparison_gift_count_df['Previous Gift Counts'] = comparison_gift_count_df['Previous Gift Counts'].astype(int)

    # Format counts with commas
    comparison_gift_count_df['Updated Gift Counts'] = comparison_gift_count_df['Updated Gift Counts'].map('{:,}'.format)
    comparison_gift_count_df['Previous Gift Counts'] = comparison_gift_count_df['Previous Gift Counts'].map('{:,}'.format)

    # Sort
    comparison_gift_count_df = comparison_gift_count_df.sort_values('GiftDate', ascending=False)

except FileNotFoundError:
    comparison_gift_count_df = ''
    print('Previous Data.parquet not found')

comparison_gift_count_df

# COMMAND ----------

# DBTITLE 1,QC Count Non-Null Values In Columns

# Count non-null values per column for the current dataframe
current_counts_df = df.count().sort_values(ascending=False).reset_index()
current_counts_df.columns = ['Column', 'Current Non-Null Count']

# Load previous data and count non-null values per column
try:
    previous_parquet = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
    previous_counts_df = previous_parquet.count().sort_values(ascending=False).reset_index()
    previous_counts_df.columns = ['Column', 'Previous Non-Null Count']

    # Merge both counts into a single dataframe for comparison
    comparison_column_counts_df = current_counts_df.merge(previous_counts_df, on='Column', how='outer').fillna(0)

    # Convert to integers
    comparison_column_counts_df['Current Non-Null Count'] = comparison_column_counts_df['Current Non-Null Count'].astype(int)
    comparison_column_counts_df['Previous Non-Null Count'] = comparison_column_counts_df['Previous Non-Null Count'].astype(int)

    # Format with commas
    comparison_column_counts_df['Current Non-Null Count'] = comparison_column_counts_df['Current Non-Null Count'].map('{:,}'.format)
    comparison_column_counts_df['Previous Non-Null Count'] = comparison_column_counts_df['Previous Non-Null Count'].map('{:,}'.format)

except FileNotFoundError:
    comparison_column_counts_df = ''
    print('Previous Data.parquet not found')

comparison_column_counts_df

# COMMAND ----------

# DBTITLE 1,Drop Null Columns
print(f"Column count before null drop: {len(df.columns)}")

#Drop columns with only null values
df = drop_null_columns(df)

print(f"Column count after null drop: {len(df.columns)}")


# COMMAND ----------

df.dtypes

# COMMAND ----------

# DBTITLE 1,Reduce For RFP Clients - TEMPORARY
# if client == 'SICL':
#   cutoff = pd.Timestamp("2025-05-01")
#   df = df[df['GiftDate'] >= cutoff]

# df

# COMMAND ----------

# DBTITLE 1,df preview
df

# COMMAND ----------

# DBTITLE 1,df dtypes
df.dtypes



# COMMAND ----------

# DBTITLE 1,Temp Gift Date Cut
# if client == 'MC':
#   df = df[df["GiftDate"] <= "2026-01-29"]

# df

# COMMAND ----------

# DBTITLE 1,Write Updated Data.parquet file
# Write to ADLS2

#Write previous Data.parquet file to Previous_Data.parquet
try:
  previous_data_parquet = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
  previous_data_parquet.to_parquet(os.path.join(filemap.MASTER, 'Previous_Data.parquet'))
  print('previous_data.parquet written to Master folder in datalake')

except FileNotFoundError:
  master = pd.DataFrame() #if no data.parquet file return empty DF
  print('Empty df written as previous_data.parquet to Master folder in datalake')


# Attempt to write the processed df to the Master data.parquet file
try:
  df.to_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
  print('Data.parquet written to Master folder in datalake')
except Exception as e:
  raise(e)

  # Write a CSV file containing records from the last five years  
try:
  import datetime
  if client == 'MC':
    mask = pd.to_datetime(df['GiftDate']).dt.year > datetime.datetime.now().year - 2
    df.loc[mask].to_csv(os.path.join(filemap.MASTER, 'Data_TwoYears.csv'), index=False)
    print('Data_TwoYears.csv written to Master folder in datalake')
  else:
    mask = pd.to_datetime(df['GiftDate']).dt.year > datetime.datetime.now().year - 5
    df.loc[mask].to_csv(os.path.join(filemap.MASTER, 'Data_FiveYears.csv'), index=False)
    print('Data_FiveYears.csv written to Master folder in datalake')
except Exception as e:
  raise(e)

# COMMAND ----------

# DBTITLE 1,Write Data.parquet to Delta Lake (TEST)
# pandas -> spark
def normalize_columns(cols):
    return [
        re.sub(r"[ ,;{}()\n\t=]+", "_", c).strip("_")
        for c in cols
    ]

sdf = df.copy()
sdf.columns = normalize_columns(sdf.columns)
sdf = spark.createDataFrame(sdf)

# write to Delta Lake in hive metastore
(
  sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")   # <-- key
    .saveAsTable(f"hive_metastore.default.{client}_data_parquet")
)

# COMMAND ----------

# DBTITLE 1,Preview data.parquet DF
df.sort_values('GiftDate', ascending = False).head()

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC
# MAGIC %md ## Archive, Apply Suppressions, and Build Initial New Columns

# COMMAND ----------

# DBTITLE 1,Remove PII
# remove pii
client_files = [x.split('/')[-1] for x in client_files]
print(client_files)
remove_pii(client, client_files, etl=True)



# COMMAND ----------

# DBTITLE 1,Archive Raw Files
# archive files
write_to_archive(client, client_files, etl=True) #moves Raw files to Archive

# COMMAND ----------

# MAGIC %md # ETL1 Specific Processes

# COMMAND ----------

# MAGIC %md ## Apply Suppressions and Build Initial New Columns for ETL1

# COMMAND ----------

# DBTITLE 1,Apply Client Specific Suppressions ETL1
# apply client specific suppressions
print(f"df shape before suppressions: {df.shape}")
suppressions = schema['Suppressions']
for k in suppressions.keys():
  df = apply_func(df, suppressions, k)
  
print(f"df shape after suppressions: {df.shape}")

# COMMAND ----------

# DBTITLE 1,Add GiftFiscal Column ETL1
# spot check FY values
fy_month = schema["firstMonthFiscalYear"] 
df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', fy_month)
# df['GiftFiscal'] = df['GiftDate'].dt.year
mask = df['GiftFiscal'] > 2016 #filters for Fiscal Years past 2016
df[mask].groupby('GiftFiscal')['GiftAmount'].sum()


# COMMAND ----------

# MAGIC %md ## Staging for CP ETL1

# COMMAND ----------

# DBTITLE 1,Write cp_stg to Staged ETL1
#Build cp_stg
cp_stg = df

# Write to ADLS2
  # Attempt to write the processed df to the Staged data_cp_staged.parquet file
try:
  cp_stg.to_parquet(os.path.join(filemap.STAGED, 'StagedForCP.parquet'))
except Exception as e:
  raise(e)

# COMMAND ----------

# MAGIC %md ## Staging for FH ETL1

# COMMAND ----------

# DBTITLE 1,Update Filters for FH ETL1
#  df = df.fillna(0)
 
try:
  FH_COLS = ['DonorID', 'GiftDate', 'GiftAmount', 'FirstGiftDate']
  df = get_fh_FirstGiftDate(df) #Add column FirstGiftDate to df
  
  filters = get_fh_filters(schema['FileHealth']) 
  print('filters: ', filters)
  FH_COLS.extend(filters) #adds filter names to the FH_COLS list
  
  for f in filters:
    df = apply_filters(df, f) #Update df with filters in FH_Columns and returned from get_fh_filters function
    
except Exception as e:
  raise(e)

print(df.shape)
print(FH_COLS)
df.head()

# COMMAND ----------

# DBTITLE 1,Convert Gift Date to datetime Format for FH ETL1
import pandas as pd

# Assuming df is your DataFrame and 'Gift Date' is the column with date strings
# Convert 'Gift Date' to datetime format

# Now you can use the .dt accessor
df['GiftYear'] = df['GiftDate'].dt.year
df['GiftMonth'] = df['GiftDate'].dt.month

# COMMAND ----------

# DBTITLE 1,Stage File for FH ETL1 - df_fh
# Stage file for FH
try:
  dataset='FileHealth'
  
  #create dictionary with FH filter names and datatypes ({'column1': 'int64','column2': 'object'})
  dec = flatten_schema(schema[dataset]) 

  #Retain only key value pairs where key is present in FH_COLS
  dec = shrink_dec(dec, FH_COLS) 

  #Create df_fh containing only columns specified in FH_COLS and drops rows where the FirstGiftDate column has NaN values.
  df_fh = df[FH_COLS].dropna(subset=['FirstGiftDate'])

  #Ensure DF conforms to the specified structure and datatypes
  df_fh = assert_declarations(df_fh, dec, view=True)
  
except Exception as e:
  raise(e)
  

# COMMAND ----------

# DBTITLE 1,Write df_fh to StagedForFH_wFilters ETL1
Write to Staged CSV StagedForFH_wFilters
df_fh.to_csv(os.path.join(filemap.STAGED, 'StagedForFH_wFilters.csv'), index=False)

# COMMAND ----------

# MAGIC %md # Conclude

# COMMAND ----------

# DBTITLE 1,Record file names for transaction history
try:
  # record the names of the files that make up the transaction history
  record_file_list(client_files, cf)  
except Exception as e:
  raise(e)

# COMMAND ----------

# DBTITLE 1,Final df Review
df

# COMMAND ----------

# DBTITLE 1,Deletes files in RawData directory
try:
  path = os.path.join(filemap.RAW, 'RawData')
  for f in os.listdir(path):
    os.remove(os.path.join(path, f))
except Exception as e:
  raise(e)

# COMMAND ----------

# DBTITLE 1,Exit
from datetime import datetime
import json

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))

# COMMAND ----------

