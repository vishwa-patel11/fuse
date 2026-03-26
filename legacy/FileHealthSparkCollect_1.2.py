# Databricks notebook source
'''
Measures for FileHealth
'''

# COMMAND ----------

from functools import reduce
from itertools import chain
import os
import pandas as pd
import sqlalchemy


from pyspark.sql import DataFrame
from pyspark.sql.types import *
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

# main flattening function
def flatten_df(df, fhp):
  all_maps = {}
  cols = ['FHGroup', 'ReportPeriod']
  for col in cols:
    # identify unique values
    _ = df.select(F.collect_set(col).alias(col)).first()[col]
    all_maps[col] = {x: i for i,x in enumerate(sorted(_))}

    # create mapping expression from string to int
    mapping_expr = F.create_map([F.lit(x) for x in chain(*all_maps[col].items())])
    df = df.withColumn(col, mapping_expr[df[col]])

  # transpose the FHGroup values 
  cols = []
  for k,v in all_maps['ReportPeriod'].items():
    header = '_'.join([fhp, str(v)])
    cols.append(header)
    mask = (df['Period'] == fhp) & (df['ReportPeriod'] == v)
    df = df.withColumn(header, F.when(mask, F.col('FHGroup')).otherwise(None))

  # consolidate values
  ref_col = 'GiftID'
  exprs = {x: "max" for x in cols}
  _df = df.groupby(ref_col).agg(exprs)

  # rename the aggregated columns
  cols = [x.replace('max(', '').replace(')', '') for x in _df.columns]
  _df = _df.toDF(*cols)
  
  # cast columns to int
  for col in [x for x in cols if x != 'GiftID']:
    _df = _df.withColumn(col,F.col(col).cast(IntegerType()))  
  
  return _df, all_maps

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
FH_PERIODS = ['fy', 'fytd', 'r12']  
FOLDER = 'FileHealth/subsets4'

# helpers
def union_all(*dfs):
  return reduce(DataFrame.union, dfs)



# COMMAND ----------

# collect data

try:
  all_groups = []
  outer_dfs = {}  
  all_maps = {}
  for fhp in FH_PERIODS:
    inner_dfs = {}
    dir_path = os.path.join(filemap.CURATED, FOLDER, fhp)
    years = os.listdir(dir_path)
    
    for year in years:
      file_path = os.path.join(dir_path, year, 'fh').split('/dbfs')[-1]
      inner_dfs[year+fhp] = spark.read.parquet(file_path)
      
    all_years = union_all(*inner_dfs.values())#.dropDuplicates(['GiftID'])
    #df = union_all(*inner_dfs.values())#.dropDuplicates(['GiftID'])
      
    dim_df, all_maps[fhp] = flatten_df(all_years, fhp)
    all_groups.append(dim_df)
    
    cols = ['GiftDate', 'GiftID', 'DonorID', 'GiftAmount', 'GiftFiscal']
    outer_dfs[fhp] = all_years.select(cols).dropDuplicates(['GiftID'])
    
  df = union_all(*outer_dfs.values()).dropDuplicates(['GiftID'])
  
  # cast date
  df = df.withColumn('GiftDate', F.to_date(F.col('GiftDate')))
  
  # Combine dimension tables and join to dataframe
  dims = all_groups[0].join(all_groups[1], on='GiftID', how='left').join(all_groups[2], on='GiftID', how='left')
  df = df.join(dims, on='GiftID', how='left')
    
except Exception as e:
  desc = 'error collecting data'
  log_error(runner, desc, e)
  
# print(df.count(), len(df.columns))
# df.show(n=3)

# COMMAND ----------

def transform_map(d, col):
 
  mapping = pd.DataFrame(d).T
  mapping = mapping[col].to_frame()
  mapping = mapping.to_dict()[col]
  mapping = pd.DataFrame(mapping)
  mapping = mapping.stack().to_frame().reset_index()
  mapping.columns = [col, 'View', 'Key']
  mapping['Key'] = mapping['Key'].astype(int)
  return mapping

col = 'FHGroup'
mapping = transform_map(all_maps, col)
mapping = mapping.drop_duplicates(col).drop('View', axis=1)
mapping.to_csv(os.path.join(filemap.CURATED, 'FileHealth', 'mapping', "%s.csv" %col), index=False)
print(mapping)

col = 'ReportPeriod'
mapping = transform_map(all_maps, col)
mapping['Key'] = mapping['View'] + '_' + mapping['Key'].astype(str)
cols = ['Key', col]
mapping[cols].to_csv(os.path.join(filemap.CURATED, 'FileHealth', 'mapping', "%s.csv" %col), index=False)
print(mapping[cols])

# COMMAND ----------

# condition = (df['GiftFiscal'] >= 2017)
# cols = ['ReportPeriod', 'GiftFiscal']
# cols = ['GiftFiscal']
# df.filter(condition).groupby(cols).agg({"GiftAmount": "sum"}).collect()

# COMMAND ----------

d = dict(zip(mapping['Key'], mapping['ReportPeriod']))
cols = [d[x] if '_' in x else x for x in df.columns]
df = df.toDF(*cols)
df.columns

# COMMAND ----------

print(df.count())
df.show(n=3)

# COMMAND ----------

  FILENAME = '%s_FH_dataset' % client
#   df.to_csv(os.path.join(filemap.CURATED, FILENAME), index=False)  
  path = os.path.join(filemap.CURATED, FILENAME).split('/dbfs')[-1]
  df.coalesce(1).write.parquet(path, mode='overwrite')

# COMMAND ----------

df = df.toPandas()

print(df.shape)
df.head()

# COMMAND ----------

mask = df['GiftFiscal'] > 2016
df[mask].groupby('GiftFiscal')['GiftAmount'].sum()

# COMMAND ----------

cols = [x for x in df.columns if '_' in x]
for c in cols:
  df[c] = df[c].astype('Int8')
  
  
# mapping = transform_map(all_maps, 'ReportPeriod')
# mapping['Key'] = mapping['View'] + '_' + mapping['Key'].astype(str)

# rp_map = dict(zip(mapping['Key'], mapping['ReportPeriod']))

# cols = list(df.columns)
# for i, c in enumerate(cols):
#   for k in rp_map.keys():
#     if k in c:
#       pre = k.split('_')[0]
#       cols[i]=c.replace(k, '_'.join([pre, rp_map[k]]))
      
# df.columns = cols

df.to_parquet(os.path.join(filemap.CURATED, 'DAV_FH_dataset.parquet'), index=False)
  
print(df.shape)
df.head()

# COMMAND ----------

#### STOP HERE ####

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# prepare dataframe for writing to ADLS2 
try:
  
#   # identify columns from schema
#   schema = get_schema_details(filemap.SCHEMA)
#   dec = flatten_schema(schema[dataset])
  
#   # Assert the data shape and type are as expected for inserting to SQL db
#   df = assert_declarations(df, dec, runner, view=True)
  
  FILENAME = '%s_FH_dataset' % client
#   df.to_csv(os.path.join(filemap.CURATED, FILENAME), index=False)  
  path = os.path.join(filemap.CURATED, FILENAME).split('/dbfs')[-1]
  df.write.parquet(path, mode='overwrite')

 
  
except Exception as e:
  desc = 'error preparing dataframe for writing to disk'
  log_error(runner, desc, e)


# sql_schema = sql_schema[:-2]

# COMMAND ----------

# mapper = {  
# 'DateType': 'DATE',
# 'DoubleType': 'FLOAT',
# 'IntegerType': 'INTEGER',
# 'StringType': 'VARCHAR(100)',
# 'TimestampType': 'DATE',
# }
# df_schema = {dfs.name: mapper[str(dfs.dataType)] for dfs in df.schema}
# df_schema = [' '.join([k,v]) for k,v in df_schema.items()]

# sql_schema = ''
# for x in df_schema:
#   sql_schema += (x + ', ') 

# COMMAND ----------

# # create context
# DATALAKE_URL = os.environ["ADLS2_URL"]
# ADLS2_NAME = os.environ["ADLS2_NAME"]
# ADLS2_KEY = os.environ["ADLS2_KEY"]

# spark.conf.set("fs.azure.account.key." + ADLS2_NAME, ADLS2_KEY)
# spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
# spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# # identify database and table name
# DATABASE = 'msdbidb'
# TABLE = "FH_Test"
# print('TABLE: ', TABLE)


# # create sqlalchemy engine
# conn_str = os.environ["DB_CONN_STR"]
# engine = configure_connection(conn_str)
# metadata = sqlalchemy.MetaData()


# COMMAND ----------

# # create schemas:
# # spark_schema, sql_schema, sqlalchemy_schema = generate_schemas(filemap.SCHEMA, dataset, TABLE, metadata)

# success = False
# tries = 0
# while not success:
#   print('number of tries: ', tries)
#   try:
#     table_exists = create_table(engine, TABLE, runner, check=True)
#     success = True
#   except:
#     tries += 1





# COMMAND ----------

# 'AppealName VARCHAR(50), Audience VARCHAR(50), AudienceGroup VARCHAR(50), ContributionAmount FLOAT, ContributionDate DATE, CostPerPiece FLOAT, Frequency VARCHAR(50), InboundChannel VARCHAR(30), MailDate DATE, MailFY INTEGER, MarketingEffortName VARCHAR(100), Monetary VARCHAR(50), PaymentMethod VARCHAR(20), QtyMailed INTEGER, Recency VARCHAR(50), RevenueID VARCHAR(50), SegmentDescription VARCHAR(100), SourceCode VARCHAR(30), Unrestricted VARCHAR(5)'

# COMMAND ----------

# if table_exists:
#   # write to SQL DB
#   df.write.mode("overwrite") \
#       .format("com.microsoft.sqlserver.jdbc.spark") \
#       .option("url", f"jdbc:sqlserver://msdbi.database.windows.net:1433;databaseName={DATABASE};") \
#       .option("dbtable", TABLE) \
#       .option("user", os.environ["SS_UID"]) \
#       .option("password", os.environ["SS_PWD"] ) \
#       .option("createTableColumnTypes", df.schema) \
#       .save()

# COMMAND ----------

