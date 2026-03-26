# Databricks notebook source
# MAGIC %md # FileHealth_Report (ETL1)

# COMMAND ----------

# MAGIC %md ## Import and Run

# COMMAND ----------

# DBTITLE 1,imports
from datetime import datetime
import os
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,run utilities
# MAGIC %run ./utilities

# COMMAND ----------

# DBTITLE 1,run mount_datalake
# MAGIC %run ./mount_datalake

# COMMAND ----------

# MAGIC %md ## Establish Client, Context, and Data

# COMMAND ----------

# DBTITLE 1,Establish Client
# # establish context
try:
  client = dbutils.widgets.get('client')
except:
  pass
  client = 'WFP'

# establish file storage directories
filemap = Filemap(client)

# COMMAND ----------

# DBTITLE 1,FH_PERIODS and Folder
# constants
FH_PERIODS = ['fy', 'fytd', 'r12', 'cy', 'cytd']  
FOLDER = 'FileHealth/subsets3'

# COMMAND ----------

# DBTITLE 1,Helper Functions for FH
# helpers
def union_all(*dfs):
  return reduce(DataFrame.union, dfs)
  
import os
from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql.functions import lit, col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example").getOrCreate()

def tlf_union_all(dfs):
    # Find the comprehensive schema
    all_columns = sorted(set(col for df in dfs for col in df.columns))
    # Collect column types across all DataFrames to determine a common schema
    column_types = {}
    for column in all_columns:
        types = set(df.schema[column].dataType for df in dfs if column in df.columns)
        if len(types) > 1:
            column_types[column] = "string"
        else:
            column_types[column] = types.pop().simpleString()
            
    # Align each DataFrame to the comprehensive schema and union
    aligned_dfs = [tlf_align_dataframe_schema(df, all_columns, column_types) for df in dfs]
    return reduce(lambda df1, df2: df1.unionByName(df2), aligned_dfs)

def tlf_align_dataframe_schema(df, all_columns, column_types):
    for column in all_columns:
        if column not in df.columns:
            df = df.withColumn(column, lit(None).cast(column_types[column]))
        else:
            df = df.withColumn(column, df[column].cast(column_types[column]))
    return df.select(*all_columns)

def align_dataframe_schema(df, target_schema):
    """
    Add missing columns as nulls to the DataFrame based on the target schema.
    """
    existing_columns = set(df.columns)
    missing_columns = set(target_schema) - existing_columns
    for col in missing_columns:
        df = df.withColumn(col, lit(None))
    return df.select(target_schema)

def union_all(*dfs):
    # Find the comprehensive schema
    all_columns = sorted(set(col for df in dfs for col in df.columns))
    # Align each DataFrame to the comprehensive schema and union
    aligned_dfs = [align_dataframe_schema(df, all_columns) for df in dfs]
    return reduce(DataFrame.unionByName, aligned_dfs)

# Use this revised function in your existing code



def write_to_adls2(df, client, file_type):
  try:
    filemap = Filemap(client)
    
    print('writing csv file')
    FILENAME = '%s_FH_dataset.%s' % (client, file_type)
   
    # Write to ADLS2 and coalesce as a single file
    print('writing to disk')
    _filename = FILENAME.split('.')[0]
    path = os.path.join(filemap.CURATED, _filename).split('/dbfs')[-1]

    if file_type == 'csv':
      df.coalesce(1).write.csv(path, header=True, mode='overwrite')
    elif file_type == 'parquet':
      df.coalesce(1).write.parquet(path, mode='overwrite')

    # Remove unneeded files from directory
    path = os.path.join(filemap.CURATED, _filename)
    for f in os.listdir(path):
      if file_type not in f:
        os.remove(os.path.join(path, f))
        
    # move file the to Curated root
    files = os.listdir(path)
    if FILENAME in os.listdir(filemap.CURATED):
      os.remove(os.path.join(filemap.CURATED, FILENAME))
    os.rename(os.path.join(path, files[0]), os.path.join(filemap.CURATED, FILENAME))   

    print('wrote file to adls2')
    return True 
  
  except:
    print('failed writing file to adls2')
    return False  


  

# COMMAND ----------

# DBTITLE 1,Helper Functions for FH part 2
import os
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql.functions import lit, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("example").getOrCreate()

def tlf_union_all(dfs):
    # Find the comprehensive schema
    all_columns = sorted(set(col for df in dfs for col in df.columns))
    
    # Align each DataFrame to the comprehensive schema and union
    aligned_dfs = [tlf_align_dataframe_schema(df, all_columns) for df in dfs]
    return reduce(lambda df1, df2: df1.unionByName(df2), aligned_dfs)

def tlf_align_dataframe_schema(df, all_columns):
    for column in all_columns:
        if column not in df.columns:
            df = df.withColumn(column, lit(None).cast(StringType()))
        else:
            df = df.withColumn(column, df[column].cast(StringType()))
    return df.select(*all_columns)

if client == 'TLF': #If client is TLF specific processing
    try:
        df_fhgroups = {}
        for fhp in FH_PERIODS:
            df_years = {}
            years = os.listdir(os.path.join(filemap.CURATED, FOLDER, fhp))
            for year in years:
                _df = []
                path = os.path.join(filemap.CURATED, FOLDER, fhp, year)
                for f in os.listdir(path):
                    file_path = os.path.join(path, f).split('/dbfs')[-1]
                    _df.append(spark.read.parquet(file_path))
                if _df:
                    # Debugging statement to check the contents of _df
                    print(f"Processing year {year} in period {fhp}:")
                    for idx, df in enumerate(_df):
                        print(f" - _df[{idx}] type: {type(df)}")
                    df_years[year] = tlf_union_all(_df)
            if df_years:
                yearly_dfs = list(df_years.values())
                df_fhgroups[fhp] = tlf_union_all(yearly_dfs)
        if df_fhgroups:
            all_dfs = list(df_fhgroups.values())
            df = tlf_union_all(all_dfs)
        else:
            raise ValueError("No data found")
        print(df.count())
    except Exception as e:
        print(f"Error: {e}")
        raise e
else:
    print(f"Client {client} is not 'TLF', skipping specific processing.")




if client != 'TLF': #If client not TLF specific Processing
  try:  
    df_fhgroups = {}
    for fhp in FH_PERIODS:

      df_years = {}
      years = os.listdir(os.path.join(filemap.CURATED, FOLDER, fhp))

      for year in years:
        _df = []
        path = os.path.join(filemap.CURATED, FOLDER, fhp, year)

        # collect all files from a given year
        for f in os.listdir(path):       
          file_path = os.path.join(path, f).split('/dbfs')[-1]
          _df.append(spark.read.parquet(file_path))

        df_years[year] = union_all(*_df)
        
      # collect all yearly files for a given fh group
      df_fhgroups[fhp] = union_all(*df_years.values()) 

    # collect all fh groups (which contain all years)
    df = union_all(*df_fhgroups.values())
    del _df
    del df_years
    del df_fhgroups

  except Exception as e:
    raise e
    
  print(df.count())

# COMMAND ----------

df.display()

# COMMAND ----------

# filtered_df = df.filter(~((col('Period') == 'fytd') & col('ReportPeriod').contains('4/30')))
# filtered_df.count()

# COMMAND ----------

# DBTITLE 1,List Unique Report Periods
unique_report_periods = df.select('ReportPeriod').distinct()

# To display the unique values
display(unique_report_periods)

# COMMAND ----------

# df = filtered_df

# COMMAND ----------

# MAGIC %md ## Write Data

# COMMAND ----------

# DBTITLE 1,prepare dataframe for writing to ADLS2
# prepare dataframe for writing to ADLS2 
try:
  # added for pyspark idiosyncracy
  df = df.withColumn("GiftDate", F.col("GiftDate").cast("date"))

  # Add processed at timestamp
  from datetime import datetime
  from pytz import timezone
  df = df.withColumn("data_processed_at", F.lit(datetime.now(tz=timezone('US/Eastern'))))
  
  if client != 'MC':
    write_to_adls2(df, client, 'csv')    
  else:
    write_to_adls2(df, client, 'parquet')
  
except Exception as e:
  raise e

# prepare dataframe for writing to ADLS2 
try:
  # added for pyspark idiosyncracy
  df = df.withColumn("GiftDate", F.col("GiftDate").cast("date"))

  # Add processed at timestamp
  from datetime import datetime
  from pytz import timezone
  df = df.withColumn("data_processed_at", F.lit(datetime.now(tz=timezone('US/Eastern'))))
  
  if client != 'MC':
    write_to_adls2(df, client, 'csv')    
  else:
    write_to_adls2(df, client, 'parquet')
  
except Exception as e:
  raise e

  

# COMMAND ----------

# DBTITLE 1,Exit Notebook
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))