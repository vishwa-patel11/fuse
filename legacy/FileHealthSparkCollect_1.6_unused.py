# Databricks notebook source
'''
Measures for FileHealth
'''

# COMMAND ----------

from functools import reduce
from itertools import chain
import os
import pandas as pd


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

# %run ./db_interface

# COMMAND ----------

# helpers

# main flattening function
def flatten_df(df, fhp):
  all_maps = {}
  cols = ['FHGroup', 'ReportPeriod']
  for col in cols:
    # identify unique values
    _ = df.select(F.collect_set(col).alias(col)).first()[col]
    all_maps[col] = {x: i for i,x in enumerate(sorted(_))}
    
  return df, all_maps
#   return _df, all_maps


def build_exp(first_year, n):
  t = [str(n)]
  for year in range(first_year, first_year+n):
    t.extend([str(year), 'FY'+str(year)])
  t = ', '.join(t)
  return 'stack('+t+')' 




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
FH_PERIODS = ['fy']#, 'fy', fytd', 'r12']  
FOLDER = 'FileHealth/subsets4'

# helpers
def union_all(*dfs):
  return reduce(DataFrame.union, dfs)

period_view = FH_PERIODS[0]

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
    cols = ['DonorID', 'FHGroup', 'GiftAmount', 'GiftDate', 'Period', 'ReportPeriod']
    outer_dfs[fhp] = all_years.select(cols)#.dropDuplicates(['GiftID'])
    
  df = union_all(*outer_dfs.values())#.dropDuplicates(['GiftID'])
  
  # cast date
  df = df.withColumn('GiftDate', F.to_date(F.col('GiftDate')))
    
except Exception as e:
  desc = 'error collecting data'
  log_error(runner, desc, e)
  
# print(df.count(), len(df.columns))
# df.show(n=3)

# COMMAND ----------

# # for development only
# df = df.sample(.0001)
# print(df.count())
# df.show(n=5)

# COMMAND ----------

# temporarily here for development with fiscal year
all_maps = all_maps[period_view]
all_maps

# COMMAND ----------

# create Report Period Year column
df = df.withColumn('ReportPeriodYear', F.split(df['ReportPeriod'], '-').getItem(0))

# add gift year column
df = df.withColumn('GiftYear', F.year('GiftDate'))

# add Last Gift Year columns
FIRST_YEAR = 2017
grouped = df.groupBy('DonorID').agg(F.max("GiftYear").alias("LastGiftYear"))
df = df.join(grouped, on='DonorID', how='left')

# print(df.count())

# COMMAND ----------

# Calculate the file health group detail delta values for Lapsed

# only one gift per any year is necessary for the file health group detail analysis
_df = df.dropDuplicates(['DonorID', 'GiftYear'])

# because file health processing is complete, we can filter out only those dates applicable to the report
# apply filter
mask = _df['LastGiftYear'] >= FIRST_YEAR - 1
_df = _df.filter(mask)


# get each donor's last gift before the reporting year
years = [x for x in range(FIRST_YEAR, FIRST_YEAR+5)]
for year in years:
  print('year: ', year)
  mask = _df['GiftYear'] < year
  col_name = 'FY'+str(year)
  grouped = _df.filter(mask).groupBy('DonorID').agg(F.max("GiftYear").alias(col_name)).drop_duplicates()
  _df = _df.join(grouped, on='DonorID', how='left')
  

# now that donor results are all aggregated, we can drop duplicate rows per donor  
_df = _df.dropDuplicates(['DonorID'])
# and select only the necessary aggregation columns
base_cols = ['DonorID']
year_cols = ['FY'+str(x) for x in range(FIRST_YEAR, FIRST_YEAR+5)]
cols = base_cols + year_cols
_df = _df.select(cols)

# conver the dataframe entries from years to year deltas
years = [x for x in range(FIRST_YEAR, FIRST_YEAR+5)]
for year in years:
  header = 'FY'+str(year)
  _df = _df.withColumn(header, year - F.col(header))

# stack the dataframe
_df = _df.select("DonorID", F.expr(build_exp(FIRST_YEAR, 5)))
cols = ['DonorID', 'ReportPeriodYear', 'LapsedValue']
_df = _df.toDF(*cols)

# cast 'LapsedValue' to string
c = 'LapsedValue'
_df = _df.withColumn(c, _df[c].cast(StringType()))

# join back to the larger dataframe
df = df.join(_df, on=['DonorID', 'ReportPeriodYear'], how='left')

# fill with empty string if FHGroup is not 'Lapsed'
c = 'LapsedValue'
df = df.withColumn(c, F.when(F.col('FHGroup').isin(['Lapsed']), F.col(c)).otherwise(''))

# COMMAND ----------

# Calculate the file health group detail delta values for Consecutive

# only one gift per any year is necessary for the file health group detail analysis
_df = df.dropDuplicates(['DonorID', 'GiftYear'])

# get list of gift years per donor
grouped = _df.groupby('DonorID').agg(F.collect_set(F.col('GiftYear')).alias('AllYears'))

# join back and drop duplicate DonorIDs
_df = _df.join(grouped, on='DonorID', how='left').dropDuplicates(['DonorID'])

# apply consecutive fuction to list
consec_udf = F.udf(lambda x,t: get_consecutive(x, t), IntegerType())
years = [x for x in range(FIRST_YEAR, FIRST_YEAR+5)]
for year in years:
  print('year: ', year)
  _df = _df.withColumn('FY'+str(year), consec_udf('AllYears', F.lit(year)))
  
  
# now that donor results are all aggregated, we can drop duplicate rows per donor  
_df = _df.dropDuplicates(['DonorID'])
# and select only the necessary aggregation columns
base_cols = ['DonorID']
year_cols = ['FY'+str(x) for x in range(FIRST_YEAR, FIRST_YEAR+5)]
cols = base_cols + year_cols
_df = _df.select(cols)

# stack the dataframe
_df = _df.select("DonorID", F.expr(build_exp(FIRST_YEAR, 5)))
cols = ['DonorID', 'ReportPeriodYear', 'ConsecutiveValue']
_df = _df.toDF(*cols)

# join back to the larger dataframe
df = df.join(_df, on=['DonorID', 'ReportPeriodYear'], how='left')

# fill with empty string if FHGroup is not 'Lapsed'
c = 'ConsecutiveValue'
df = df.withColumn(c, F.when(F.col('FHGroup').isin(['Consecutive Giver']), F.col(c)).otherwise(''))

# df.show(n=10)

# COMMAND ----------

# Calculate the file health group detail delta values for Reinstated

# only one gift per any year is necessary for the file health group detail analysis
_df = df.dropDuplicates(['DonorID', 'GiftYear'])

# get list of gift years per donor
grouped = _df.groupby('DonorID').agg(F.collect_set(F.col('GiftYear')).alias('AllYears'))

# join back and drop duplicate DonorIDs
_df = _df.join(grouped, on='DonorID', how='left').dropDuplicates(['DonorID'])

# apply consecutive fuction to list
reinstated_udf = F.udf(lambda x,t: get_reinstated(x,t), IntegerType())
years = [x for x in range(FIRST_YEAR, FIRST_YEAR+5)]
for year in years:
  print('year: ', year)
  _df = _df.withColumn('FY'+str(year), reinstated_udf(F.col('AllYears'), F.lit(year)))
  
  
# now that donor results are all aggregated, we can drop duplicate rows per donor  
_df = _df.dropDuplicates(['DonorID'])
# and select only the necessary aggregation columns
base_cols = ['DonorID']
year_cols = ['FY'+str(x) for x in range(FIRST_YEAR, FIRST_YEAR+5)]
cols = base_cols + year_cols
_df = _df.select(cols)

# stack the dataframe
_df = _df.select("DonorID", F.expr(build_exp(FIRST_YEAR, 5)))
cols = ['DonorID', 'ReportPeriodYear', 'ReinstatedValue']
_df = _df.toDF(*cols)

# join back to the larger dataframe
df = df.join(_df, on=['DonorID', 'ReportPeriodYear'], how='left')

# fill with empty string if FHGroup is not 'Lapsed'
c = 'ReinstatedValue'
df = df.withColumn(c, F.when(F.col('FHGroup').isin(['Reinstated Last Year']), F.col(c)).otherwise(''))

# df.show(n=10)

# COMMAND ----------

# and now that all processing of historical data is complete, 
# we can drop any dates that predate the report
mask = df['LastGiftYear'] >= FIRST_YEAR
df = df.filter(mask)

# print(df.count())

df.columns

# COMMAND ----------

# create FHGroupDetail column

# concatenate columns to get FHGD_value
feature = 'FHGroupDetail'
col_list = ['LapsedValue','ConsecutiveValue', 'ReinstatedValue']
df = df.withColumn(feature,F.concat(*col_list))
df = df.withColumn(feature, df[feature].cast(IntegerType()))


# COMMAND ----------

# mask = df['FHGroup'].isin(['New', 'New Last Year'])
# df.filter(~mask).show(n=10)

# COMMAND ----------

# create mapping expression from string to int
cols = ['FHGroup', 'ReportPeriod']
for col in cols:
  mapping_expr = F.create_map([F.lit(x) for x in chain(*all_maps[col].items())])
  df = df.withColumn(col, mapping_expr[df[col]])
# df.show(n=2)

# COMMAND ----------

# TODO Remove helper columns
# Remaining should be:
# DonorID, FHGroup, FHGroupDetail, GiftAmount, GiftDate, Period, ReportPeriod

cols = [
  'DonorID', 'FHGroup', 'FHGroupDetail', 'GiftAmount', 
  'GiftDate', 'Period', 'ReportPeriod'
]
df = df.select(cols)

# COMMAND ----------

def transform_map(d, col):
  mapping = pd.DataFrame(all_maps)[col].to_frame().dropna()
  mapping[col] = mapping[col].astype(int)
  return mapping.reset_index().rename(columns={'index': 'Key', col: 'Value'})

# TODO add code for FHGroupDetail
cols = ['FHGroup', 'ReportPeriod']
for col in cols:
  mapping = transform_map(all_maps, col)
  mapping.to_csv(os.path.join(filemap.CURATED, 'FileHealth', 'mapping', period_view, "%s.csv" %col), index=False)
  print(mapping)

# COMMAND ----------

# # Spot check values during development

# condition = (df['GiftFiscal'] >= 2017)
# cols = ['ReportPeriod', 'GiftFiscal']
# cols = ['GiftFiscal']
# df.filter(condition).groupby(cols).agg({"GiftAmount": "sum"}).collect()

# COMMAND ----------

# Write data to ADLS2 and then clean up spark metadata files
print('file health period: ', period_view)

FILENAME = '%s_FH_dataset_%s' % (client, period_view)
#   df.to_csv(os.path.join(filemap.CURATED, FILENAME), index=False)  
path = os.path.join(filemap.CURATED, FILENAME).split('/dbfs')[-1]
df.coalesce(1).write.parquet(path, mode='overwrite')

path = os.path.join(filemap.CURATED, FILENAME)
for f in os.listdir(path):
  if '.parquet' not in f:
    os.remove(os.path.join(path, f))
    
files = os.listdir(path)
os.rename(os.path.join(path, files[0]), os.path.join(path, 'dataset.snappy.parquet'))    

os.listdir(path)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

