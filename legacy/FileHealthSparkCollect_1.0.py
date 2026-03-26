# Databricks notebook source
'''
Measures for FileHealth
'''

# COMMAND ----------

from itertools import chain
import os
import pandas as pd
import sqlalchemy

from functools import reduce
from pyspark.sql import DataFrame

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
  dfs = {}  
  for fhp in FH_PERIODS:
    dir_path = os.path.join(filemap.CURATED, FOLDER, fhp)
    years = os.listdir(dir_path)
    for year in years:
      file_path = os.path.join(dir_path, year, 'fh').split('/dbfs')[-1]
      dfs[year+fhp] = spark.read.parquet(file_path)
  df = union_all(*dfs.values())
  
  # cast date
  cols = ['GiftDate', 'calcDate', 'EndofRptPeriod']
  for c in cols:
    df = df.withColumn(c, F.to_date(F.col(c)))
    
except Exception as e:
  desc = 'error collecting data'
  log_error(runner, desc, e)
  
print(df.count(), len(df.columns))
df.show(n=3)

# COMMAND ----------

# create fh group mapping
all_maps = {}
cols = ['FHGroup', 'ReportPeriod']
for col in cols:
  _ = df.select(F.collect_set(col).alias(col)).first()[col]
  all_maps[col] = {x: i for i,x in enumerate(sorted(_))}
  print(col, all_maps[col])

  mapping_expr = F.create_map([F.lit(x) for x in chain(*all_maps[col].items())])
  df = df.withColumn(col, mapping_expr[df[col]])
df.show(n=5)
# save mapping file
# cast column as int

# COMMAND ----------

condition = (df['DonorID'] == 10056301) & (df['GiftFiscal'] == 2016)
df = df.filter(condition)#.sort('calcDate')
df.show(n=20)

# COMMAND ----------

views = ['fy', 'fytd', 'r12']
cols = []
for view in views:
  print('view: ', view)
  for k,v in all_maps['ReportPeriod'].items():
    header = '_'.join([view, str(v)])
    cols.append(header)
    mask = (df['Period'] == view) & (df['ReportPeriod'] == v)
    df = df.withColumn(header, F.when(mask, F.col('FHGroup')).otherwise(None))
    

    


# COMMAND ----------

ref_col = 'GiftID'
for col in cols:
  _df = df.groupby(ref_col).agg(F.mean(col).alias(col))
  df = df.drop(col)
  df = df.join(_df, on=ref_col, how='left')
_df.show(n=5)

# COMMAND ----------

ref_col = 'GiftID'
df = df.dropDuplicates([ref_col])
df.show(n=30)

# COMMAND ----------

df.count()

# COMMAND ----------

condition = (df['Period'] == 'fy') & (df['GiftFiscal'] >= 2017)
cols = ['ReportPeriod', 'GiftFiscal']
df.filter(condition).groupby(cols).agg({"GiftAmount": "sum"}).collect()

# COMMAND ----------

# prepare dataframe for writing to ADLS2 
try:
  
#   # identify columns from schema
#   schema = get_schema_details(filemap.SCHEMA)
#   dec = flatten_schema(schema[dataset])
  
#   # Assert the data shape and type are as expected for inserting to SQL db
#   df = assert_declarations(df, dec, runner, view=True)
  df = df.drop('GiftID')
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

