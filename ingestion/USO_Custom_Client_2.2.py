# Databricks notebook source
# MAGIC %md # Client_2.2 USO Custom (Custom Production)

# COMMAND ----------

# MAGIC %md ## Runs and Imports
# MAGIC

# COMMAND ----------

# DBTITLE 1,imports
from datetime import datetime
import numpy as np
from datetime import datetime
from pytz import timezone

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# DBTITLE 1,TEST DATETIME
from datetime import datetime
from pytz import timezone
print(datetime.now(tz=timezone('US/Eastern')))  


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

# MAGIC %run ./source_code_validation

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Choose Client

# COMMAND ----------

# DBTITLE 1,Choose the client we're running for
# # establish context - use the globally passed client variable (from ETL workflow) if it exists, otherwise fall back to the except statement to execute the script for the manually set variable 
try:
  client = dbutils.widgets.get('client')
except:
  client = 'USO'

# establish file storage directories
filemap = Filemap(client)

# COMMAND ----------

# DBTITLE 1,Choose the File we're running (USO specific)
# # establish context - use the globally passed client variable (from ETL workflow) if it exists, otherwise fall back to the except statement to execute the script for the manually set variable 
try:
  USO_file_name = dbutils.widgets.get('USO_filename')
  USO_file_name = USO_file_name.strip("[]").strip("'").strip('"')
except:
  USO_file_name = 'USO Renewals Export thru 6.29.25.xlsx'

print(USO_file_name)

# COMMAND ----------

# MAGIC
# MAGIC %md ## Transferring Cost data from Sharepoint to Datalake

# COMMAND ----------

# DBTITLE 1,Transfer campaign cost data
# Transferring campaign cost data from Sharepoint to datalake 
transfer_cost_data('Shared Documents/Reporting 2.0/SourceCodeDocs/USO/Campaign Costs') # input cost data directory

# COMMAND ----------

# DBTITLE 1,Run notebook to merge costs and trx data + export
# MAGIC %run ../USO_Create_Campaign_Cost_Report

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

# DBTITLE 1,Exits if client_files is empty
if not client_files:
    msg = {
        "status": "NO_FILES",
        "client": client,
        "timestamp_utc": str(datetime.utcnow()),
        "message": f"No files found for {client}. Exiting notebook."
    }
    print(msg["message"])
    dbutils.notebook.exit(json.dumps(msg))
else:
    print(f"{datetime.now()} - files detected for {client}: {client_files}")

# COMMAND ----------

# MAGIC %md ## Build data.parquet and perform client specific tasks

# COMMAND ----------

# DBTITLE 1,TEMP: Build DF from single file
print(f"Filename used: {USO_file_name}")
df = collect_transaction_file_USO(
    folder=filemap.RAW,
    filename= USO_file_name,
    columns=schema['Columns'],
    encoding=schema['Encoding'],
    key_mapper=schema['KeyMapper'],
    date_format=schema['DateFormat'],
    directory='RawData'
)

df

# COMMAND ----------

# DBTITLE 1,Build/Update Data.parquet and Data_Five_Years.csv Files
# Format data
try:  
  # collect individual data files and store in df. Rename transaction file columns based on KeyMapper in schema
  # df = collect_transaction_files_USO(
  #     folder = filemap.RAW
  #     , columns = schema['Columns']
  #     , encoding = schema['Encoding']
  #     , key_mapper = schema['KeyMapper']
  #     , date_format = schema['DateFormat']
  #     , directory='RawData'
  #   )

  print(f"Filename used: {USO_file_name}")
  df = collect_transaction_file_USO(
    folder=filemap.RAW,
    filename= USO_file_name,
    columns=schema['Columns'],
    encoding=schema['Encoding'],
    key_mapper=schema['KeyMapper'],
    date_format=schema['DateFormat'],
    directory='RawData')
  
  df_test = df
  
  
  #Ensure Mail Date is datetime
  df['Mail Date'] = pd.to_datetime(df['Mail Date'])
  #Set min_date for current data.parquet mask
  min_date = df['Mail Date'].min()
  #Set max_date for current data.parquet mask
  max_date = df['Mail Date'].max()
  
  #Determine if Appeal or Acquisition Campaign:
  
  if 'Recency Code' in df.columns and 'List Name' in df.columns:    
    raise ValueError("Data contains both 'Recency Code' and 'List Name' columns. Cannot determine Campaign Type.")
  elif 'Recency Code' in df.columns:
    df['Campaign Type'] = 'Appeal'
    current_campaign_type = 'Appeal'
    print('Appeal transaction file')
  elif 'List Name' in df.columns:
    df['Campaign Type'] = 'Acquisition'
    current_campaign_type = 'Acquisition'
    print('Acquisition transaction file')
  else:
    raise ValueError("Campaign Type not recognized: neither 'Recency Code' nor 'List Name' found in columns")

  # open master historical data and combine datasets
  try:
    master = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
    if 'Special' in schema.keys():
      if schema['Special']['IgnoreDates'] == True:
        pass
      else:
          # Keep rows that are either outside the date range or don't match campaign type
          mask = (
              (master['Mail Date'] < min_date) |
              (master['Mail Date'] > max_date) |
              (master['Campaign Type'] != current_campaign_type)
          )
          master = master[mask]

  except FileNotFoundError:
    master = pd.DataFrame() #if no data.parquet file return empty DF

  print(f"Acquisition df length: {len(df[df['Campaign Type'] == 'Acquisition'])}")
  print(f"Appeal df length: {len(df[df['Campaign Type'] == 'Appeal'])}")

  #align headers
  df_cols, master_cols = reorder_col_names(df.columns, master.columns)
  df = df[df_cols]
  master = master[master_cols] 
  
  # remove pii
  pii_cols = schema['PII_Columns'] 
  df = _remove_pii(df, pii_cols) #removes any PII columns listed in client schema
  
  # merge dataframes
  if client == 'FWW':
    df.columns = [x.replace(' ','') for x in df.columns] #Removes spaces from column names
    
  print('master columns: ', master.columns)
  print('df columns: ', df.columns)
  df = pd.concat([master,df]) #combines rows of 'master' (existing Data.parquet) and df (client files df) into single DataFrame

  
  print(f"Acquisition df length: {len(df[df['Campaign Type'] == 'Acquisition'])}")
  print(f"Appeal df length: {len(df[df['Campaign Type'] == 'Appeal'])}")

  # apply client specific preprocessing
  df = apply_client_specific(df, client)
  print('df client specific columns: ', df.columns)

  print(f"Acquisition df length: {len(df[df['Campaign Type'] == 'Acquisition'])}")
  print(f"Appeal df length: {len(df[df['Campaign Type'] == 'Appeal'])}")
  
  # de-duplicate dataset
  print(f"Acquisition df length: {len(df[df['Campaign Type'] == 'Acquisition'])}")
  print(f"Appeal df length: {len(df[df['Campaign Type'] == 'Appeal'])}")
  df = apply_dedupe(df, schema['DuplicateKeys']) 
  print(f"Acquisition df length: {len(df[df['Campaign Type'] == 'Acquisition'])}")
  print(f"Appeal df length: {len(df[df['Campaign Type'] == 'Appeal'])}")

  # cast columns and drop invalid dates
  df['Mail Date'] = pd.to_datetime(df['Mail Date'])
  df = drop_future_dates(df, 'Mail Date') # Removes rows with GiftDate in the future.
  df['Revenue'] = format_currency(df['Revenue']) #Update currency values ($ ,) to a numeric format
  df['Cost'] = format_currency(df['Cost']) #Update currency values ($ ,) to a numeric format
  
  print(f"Acquisition df length: {len(df[df['Campaign Type'] == 'Acquisition'])}")
  print(f"Appeal df length: {len(df[df['Campaign Type'] == 'Appeal'])}")
  
  df[['Gift Count', 'Mail Quantity']] = df[['Gift Count', 'Mail Quantity']].apply(
      lambda col: col.astype(str).str.strip().str.replace(',', '').str.replace('-', '0'))
  
  df['Gift Count'] = df['Gift Count'].astype(float)
  df[['Gift Count', 'Mail Quantity']] = df[['Gift Count', 'Mail Quantity']].fillna('0').astype(int)

    
  # # cast columns for conversion to parquet
  for c in schema['ParquetCols']:
    if c in df.columns:  # Check if column exists in df
        df[c] = df[c].fillna(value='None').astype(str)  # Convert to string and fill missing values with 'None'
   
except Exception as e:
  raise(e)

# Add processed at timestamp
# from datetime import datetime
# from pytz import timezone
print(f"Processed Time: {datetime.now(tz=timezone('US/Eastern'))}")
df['data_processed_at'] = datetime.now(tz=timezone('US/Eastern'))  
  
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns}")
print(f"Max Mail Date: {df['Mail Date'].max()}")
# df.head()

# COMMAND ----------

# DBTITLE 1,Arrange df Columns for Easier Preview
# Define the preferred order for the first four columns
preferred_order = ['Mail Date','Source Code', 'Recency Code', 'Frequency Code', 'Monetary Code', 'Mail Quantity', 'Gift Count', 'Revenue', 'Cost']

# Identify which columns from the preferred order actually exist in the dataframe
existing_columns = [col for col in preferred_order if col in df.columns]

# Get the remaining columns that are not in the preferred order
remaining_columns = [col for col in df.columns if col not in existing_columns]

# Reorder the dataframe with existing preferred columns first, followed by the rest
df = df[existing_columns + remaining_columns]

#Sort by descending GiftDate
df = df.sort_values('Mail Date', ascending = False)

df.head(2)

# COMMAND ----------

# DBTITLE 1,Preview DF
df

# COMMAND ----------

# DBTITLE 1,QC df duplicates by Source Code
#Review duplicates
duplicates = df[df.duplicated(subset='Source Code', keep=False)].sort_values('Source Code')
duplicates

# COMMAND ----------

# DBTITLE 1,QC Compare Data.parquet Gift Counts by Year
# Calculate new Data.parquet counts
new_count = df.groupby(df['Mail Date'].dt.year).size().reset_index(name='Updated Gift Counts')

try:
    # Calculate previous Data.parquet counts
    previous_parquet = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
    previous_count = previous_parquet.groupby(previous_parquet['Mail Date'].dt.year).size().reset_index(name='Previous Gift Counts')

    # Merge both dataframes on Mail Date year
    comparison_gift_count_df = new_count.merge(previous_count, on='Mail Date', how='outer').fillna(0)

    # Convert to integers to remove .0
    comparison_gift_count_df['Updated Gift Counts'] = comparison_gift_count_df['Updated Gift Counts'].astype(int)
    comparison_gift_count_df['Previous Gift Counts'] = comparison_gift_count_df['Previous Gift Counts'].astype(int)

    # Format counts with commas
    comparison_gift_count_df['Updated Gift Counts'] = comparison_gift_count_df['Updated Gift Counts'].map('{:,}'.format)
    comparison_gift_count_df['Previous Gift Counts'] = comparison_gift_count_df['Previous Gift Counts'].map('{:,}'.format)

    # Sort
    comparison_gift_count_df = comparison_gift_count_df.sort_values('Mail Date', ascending=False)

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

# DBTITLE 1,Drop Unwanted Columns
print("Column count before drop:", len(df.columns))

#List of columns to drop
cols_to_drop = ['Index', 'index', 'Sourcekey Position 5', 'Sourcekey Position 7','Src Contrib Srce Cd Pos567', 'Src Contrib List Segment (copy)', 'Day of Src Contrib Srce Mail Dt', 'Src Contrib List Segment (copy 2)', 'Forecast Gifts', 'Lifecycle Detail Prior Year (Pos 11)', 'Lifecycle Detail Current Year (Pos 12)', 'Channel Response Indicator (Group)', 'Forecast: Apply Gift Curve', 'Channel Response Indicator', 'Src Contrib Gift Cnt', 'Src Contrib Package Grp (copy)', 'Src Contrib Srce Cd Pos1'
     ] 

#Drop unwanted columns
df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

print("Column count after drop:", len(df.columns))


# COMMAND ----------

# DBTITLE 1,Column Updates
#Add Mail Month to Acq campaigns
# Ensure Mail Date is datetime
df['Mail Date'] = pd.to_datetime(df['Mail Date'], errors='coerce')
# Only fill where Mail Month is missing or blank
df['Mail Month'] = df['Mail Month'].mask(
    df['Mail Month'].isna() | (df['Mail Month'].str.strip() == ''),
    df['Mail Date'].dt.strftime('%B')
)

# Create Mail Month Number (1 = January, 12 = December)
df['Mail Month Number'] = df['Mail Date'].dt.month

# COMMAND ----------

# DBTITLE 1,Preview df before write
df

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
  mask = df['Mail Date'].dt.year > datetime.now().year - 6
  df.loc[mask].to_csv(os.path.join(filemap.MASTER, 'Data_FiveYears.csv'), index=False)
  print('Data_FiveYears.csv written to Master folder in datalake')
except Exception as e:
  raise(e)

# COMMAND ----------

# DBTITLE 1,Preview data.parquet DF after write
df.sort_values('Mail Date', ascending = False).head()

# COMMAND ----------

df.dtypes

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC
# MAGIC %md ## Archive, Apply Suppressions, and Build Initial New Columns

# COMMAND ----------

# DBTITLE 1,Archive Raw Files
# archive files
write_to_archive(client, USO_file_name, etl=True) #moves Raw files to Archive

# COMMAND ----------

# MAGIC %md ## Conclude

# COMMAND ----------

# DBTITLE 1,Record file names for transaction history
try:
  # record the names of the files that make up the transaction history
  record_file_list([USO_file_name], cf)  
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
        if f == USO_file_name:
            print(f"Deleting: {f}")
            os.remove(os.path.join(path, f))
except Exception as e:
    raise(e)

# COMMAND ----------

# DBTITLE 1,Acq Check
df[df['Campaign Type'] == 'Acquisition']

# COMMAND ----------

# DBTITLE 1,Appeal Check
df[df['Campaign Type'] == 'Appeal']

# COMMAND ----------

# DBTITLE 1,Exit
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))

# COMMAND ----------

