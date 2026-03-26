# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor as Executor
from pyspark.sql.functions import isnan, when
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

def get_gift_fy(df, month, ref_col='GiftDate', report='fy', label='GiftFiscal'):
  if month == 1:
    return df.withColumn(label, F.year(F.col(ref_col)))
  else:
    condition = F.month(F.col(ref_col)) >= month
    if 'fy' in report:
      return df.withColumn(label, F.when(condition, F.year(F.col(ref_col))+1).otherwise(F.year(F.col(ref_col))))
    else:
      return df.withColumn(label, F.when(condition, F.year(F.col(ref_col))).otherwise(F.year(F.col(ref_col))-1))


def make_fh_consecutive(_df):
  # Consecutive Giver
  condition = (_df['PrevGiftFY'] == _df['ReportFY']-1) & (_df['PrevPrevGiftFY'] == _df['ReportFY']-2)
#   _df['Consecutive Giver'] = np.where(condition, True, False)
  _df = _df.withColumn('Consecutive Giver', F.when(condition, True).otherwise(False))

  return _df


def make_fh_group(_df): # ORIGINAL VERSION (MAY NEED TO UPDATE)
  cols = ['New', 'New Last Year', 'Lapsed', 'Reinstated Last Year', 'Consecutive Giver']
  for col in cols:
#     _df['fhgd' + col] = [col if x else '' for x in _df[col].values.tolist()]
    label = 'fhgd' + col
#     condition = (F.col(col).isNull()) | (F.isnan(F.col(col))) | (F.col(col) == False)
    _df = _df.withColumn(label, F.when(F.col(col) == False, '').otherwise(col))

  fhgd_cols = ['fhgd' + col for col in cols]
#   _df['FHGroup'] = _df[fhgd_cols].apply(lambda x: ''.join(x), axis=1)
  _df = _df.withColumn('FHGroup', F.concat(*fhgd_cols))
  
#   _df = _df.drop(fhgd_cols+cols, axis=1)
  cols_to_drop = fhgd_cols + cols
  _df = _df.drop(*cols_to_drop)

  return _df

# def make_fh_group(_df): #NEW VERSION ADDED 1/3/25- may need to call this version moving forward
#     cols = ['New', 'New Last Year', 'Lapsed', 'Reinstated Last Year', 'Consecutive Giver']
    
#     # Create fhgd columns using Spark's when and col functions
#     for col_name in cols:
#         _df = _df.withColumn(f'fhgd{col_name}', when(col(col_name) == True, col_name).otherwise(''))
    
#     # List of generated columns
#     fhgd_cols = [f'fhgd{col}' for col in cols]
    
#     # Concatenate the columns into a single column 'FHGroup' using concat_ws for proper spacing
#     _df = _df.withColumn('FHGroup', concat_ws('', *fhgd_cols))
    
#     # Drop the temporary and original columns
#     _df = _df.drop(*fhgd_cols, *cols)
    
#     return _df

def make_fh_group_detail(_df):
#   _df['FHGroupDetail'] = 'None'
  _df = _df.withColumn('FHGroupDetail', F.lit('None'))
  return _df


def make_fh_lapsed(_df):
  # Lapsed
  condition = ((_df['ReportFY'] - _df['PrevGiftFY']) > 1 ) & (~_df['New'])
#   _df['Lapsed'] = np.where(condition, True, False)
  _df = _df.withColumn('Lapsed', F.when(condition, True).otherwise(False))
  return _df


def make_fh_new_cols(_df):
  # New and New Last Year
#   _df['New'] = np.where(_df['FirstGiftFY'] == _df['ReportFY'], True, False)
  condition = _df['FirstGiftFY'] == _df['ReportFY']
  _df = _df.withColumn('New', F.when(condition, True).otherwise(False))
#   _df['New Last Year'] = np.where(_df['FirstGiftFY'] == _df['ReportFY']-1, True, False)
  condition = _df['FirstGiftFY'] == _df['ReportFY']-1
  _df = _df.withColumn('New Last Year', F.when(condition, True).otherwise(False))

  return _df


def make_fh_reinstated(_df):
  # Reinstated Last Year
  condition = (_df['PrevGiftFY'] == _df['ReportFY']-1) & (_df['PrevPrevGiftFY'] < _df['ReportFY']-2) & (~_df['New Last Year'])
#   _df['Reinstated Last Year'] = np.where(condition, True, False)
  _df = _df.withColumn('Reinstated Last Year', F.when(condition, True).otherwise(False))
  return _df


def make_fh_report(_df, period, fym, last_gift_date):
  if period == 'r12':
    fym = last_gift_date.month
    _df = get_gift_fy(_df, fym, report=period)
#     _df['ReportFY'] = pd.to_datetime(_df['calcDate']).dt.year
    _df = _df.withColumn('ReportFY', F.year(F.to_date(F.col('calcDate'), 'MM/dd/yyyy')))
  else:
#     _df['ReportFY'] = pd.to_datetime(_df['EndofRptPeriod']).dt.year      
    # _df = _df.withColumn('ReportFY', F.year(F.col('EndofRptPeriod')))
    _df = _df.withColumn('ReportFY', F.year(F.to_date(F.col('EndofRptPeriod'), 'MM/dd/yyyy')))
  return _df, fym
        


def make_report_dates(df, m, y, p, lgd):
  cd = 'calcDate'
  eorp = 'EndofRptPeriod'
  if p == 'fy':
    if m == 1:
#       df['calcDate'] = '01/01/%s' % str(y)
      df = df.withColumn(cd, F.lit('01/01/%s' % str(y)))
#       df['EndofRptPeriod'] = '12/31/%s' % str(y)
      df = df.withColumn(eorp, F.lit('12/31/%s' % str(y)))
    else:
      month = str(m).zfill(2)
      day = str(calendar.monthrange(y, m)[-1]).zfill(2)
#       df['calcDate'] = '%s/01/%s' % (month, str(y-1))
      df = df.withColumn(cd, F.lit('%s/01/%s' % (month, str(y-1))))

      month = str(m-1).zfill(2)
      day = str(calendar.monthrange(y, m-1)[-1]).zfill(2)        
#       df['EndofRptPeriod'] = '%s/%s/%s' % (month, day, str(y))
      df = df.withColumn(eorp, F.lit('%s/%s/%s' % (month, day, str(y))))
            
  elif p == 'fytd':
    prev = lgd.replace(day=1) - timedelta(days=1)
    last_month = str(prev.month).zfill(2)
    last_day = str(prev.day).zfill(2)
    if m == 1:
      y = y + 1
#       df['calcDate'] = '01/01/%s' % str(y)
      df = df.withColumn(cd, F.lit('01/01/%s' % str(y)))
#       df['EndofRptPeriod'] = '%s/%s/%s' % (last_month, last_day, str(y))
      df = df.withColumn(eorp, F.lit('%s/%s/%s' % (last_month, last_day, str(y))))
    else:
      month = str(m).zfill(2)
      day = str(calendar.monthrange(y, m)[-1]).zfill(2)
#       df['calcDate'] = '%s/01/%s' % (month, str(y))
      df = df.withColumn(cd, F.lit('%s/01/%s' % (month, str(y))))
#       df['EndofRptPeriod'] = '%s/%s/%s' % (last_month, last_day, str(y+1))
      df = df.withColumn(eorp, F.lit('%s/%s/%s' % (last_month, last_day, str(y+1))))
            
  elif p == 'r12':
    start = lgd.replace(day=1)
    start = start.replace(year=y)
#     df['calcDate'] = start.strftime('%m/%d/%Y')
    df = df.withColumn(cd, F.lit(start.strftime('%m/%d/%Y')))
    # df = df.withColumn(cd, F.lit(start.strftime('%Y-%m-%d')))

    prev = ((start - timedelta(days=1)) + relativedelta(years=1))#.date()
    last_month = str(prev.month).zfill(2)
    last_day = str(prev.day).zfill(2)
    last_year = str(prev.year).zfill(4)
#     df['EndofRptPeriod'] = '%s/%s/%s' % (last_month, last_day, last_year)
    df = df.withColumn(eorp, F.lit('%s/%s/%s' % (last_month, last_day, last_year)))
     
  df = df.withColumn('EndofRptPeriod', F.to_date(F.col('EndofRptPeriod'), 'MM/dd/yyyy'))
  # df = df.withColumn('calcDate', F.to_date(F.col('calcDate'), 'MM/dd/yyyy'))
  mask = df['GiftDate'] <= df['EndofRptPeriod']

  return df.filter(mask)


def prep_for_crl(_df, fym, period):
  # Prep for Consecutive, Reinstated, Lapsed
  inner_mask = _df['GiftFiscal'] < _df['ReportFY'] #first_year
#   grouped = _df[inner_mask].groupby('DonorID')['GiftDate'].max().to_frame(name='PrevGiftDate')
  grouped = _df.filter(inner_mask).groupby('DonorID').agg(F.max('GiftDate').alias('PrevGiftDate'))
  grouped = get_gift_fy(grouped, fym, ref_col='PrevGiftDate', report=period, label='PrevGiftFY')
#   _df = pd.merge(_df, grouped, on='DonorID', how='left')   
  _df = _df.join(grouped, on='DonorID', how='left')

  inner_mask = _df['GiftFiscal'] < (_df['ReportFY'] - 1) #first_year
#   grouped = _df[inner_mask].groupby('DonorID')['GiftDate'].max().to_frame(name='PrevPrevGiftDate')
  grouped = _df.filter(inner_mask).groupby('DonorID').agg(F.max('GiftDate').alias('PrevPrevGiftDate'))
  grouped = get_gift_fy(grouped, fym, ref_col='PrevPrevGiftDate', report=period, label='PrevPrevGiftFY')
#   _df = pd.merge(_df, grouped, on='DonorID', how='left')
  _df = _df.join(grouped, on='DonorID', how='left')

  cols = ['PrevGiftFY', 'PrevPrevGiftFY']
  for col in cols:    
#     _df['PrevGiftFY'] = _df['PrevGiftFY'].astype('Int64')
#     _df['PrevGiftFY'] = _df['PrevGiftFY'].fillna(value=0) 
#     _df['PrevPrevGiftFY'] = _df['PrevPrevGiftFY'].astype('Int64')
#     _df['PrevPrevGiftFY'] = _df['PrevPrevGiftFY'].fillna(value=0)
    _df = _df.withColumn(col,F.col(col).cast(IntegerType()))  
    _df = _df.withColumn(col, F.when(F.col(col).isNull(), 0).otherwise(F.col(col)))  

  return _df



def prep_and_last_gift(_df, fym, period):
  # First Gift
#   grouped = _df.groupby('DonorID')['GiftDate'].min().to_frame(name='FirstGiftDate')
  grouped = df.groupby('DonorID').agg(F.min('GiftDate').alias('FirstGiftDate'))
  grouped = get_gift_fy(grouped, fym, ref_col='FirstGiftDate', report=period, label='FirstGiftFY')
#   _df = pd.merge(_df, grouped, on='DonorID', how='left')
  _df = _df.join(grouped, on='DonorID', how='left')

  # Last Gift
#   grouped = _df.groupby('DonorID')['GiftDate'].max().to_frame(name='LastGiftDate')
  grouped = df.groupby('DonorID').agg(F.max('GiftDate').alias('LastGiftDate'))
  grouped = get_gift_fy(grouped, fym, ref_col='LastGiftDate', report=period, label='LastGiftFY')
#   _df = pd.merge(_df, grouped, on='DonorID', how='left')
  _df = _df.join(grouped, on='DonorID', how='left')

  return _df


def prep_report_subset(df, period, year):
  if period == 'fy':
    mask = df['GiftFiscal'] <= year
  else:
    mask = df['GiftFiscal'] <= year + 1            
  return df.filter(mask)


def preprocess(df, fym):    
  df = get_gift_fy(df, fym)
  col = "DonorID"
  df = df.withColumn(col,F.col(col).cast(StringType()))
  return df


def process_df(df, period, dec, FYM, lgd, n_years=5):
  
  this_year = datetime.now().year
  years = [i for i in range(this_year-n_years, this_year)]

  fh_cols = list(dec.keys())

#   years = [max(years)]
  for year in years:
    
    first_year = year-1

    _df = calc_fh(df, period, year, FYM, lgd)

    # Drop unwanted columns
#     _df = _df[fh_cols]
    _df = _df.select(fh_cols)
  
    prefix = "subsets4/%s/%s" %(period, str(year))
    
    path = os.path.join(filemap.CURATED, 'FileHealth', prefix)
    os.makedirs(path, exist_ok=True)      
    
    path = os.path.join(path, 'fh').split('/dbfs')[-1]
    _df.write.parquet(path, mode='overwrite')    

  return True


def calc_fh(df, period, year, FYM, last_gift_date):
    
  # Filter report period and time
  _df = prep_report_subset(df, period, year)

  # calcDate and EndofRptPeriod
  _df = make_report_dates(_df, FYM, year, period, last_gift_date)

  # ReportFY
  _df, FYM = make_fh_report(_df, period, FYM, last_gift_date)

  # First Gift and # Last Gift
  _df = prep_and_last_gift(_df, FYM, period)

  # New and New Last Year
  _df = make_fh_new_cols(_df)

  # Prep for Consecutive, Reinstated, Lapsed
  _df = prep_for_crl(_df, FYM, period)  

  # Consecutive Giver
  _df = make_fh_consecutive(_df)

  # Lapsed
  _df = make_fh_lapsed(_df)

  # Reinstated Last Year
  _df = make_fh_reinstated(_df)

  # Report Period
#   _df['ReportPeriod'] = _df['calcDate'] + ' - ' + _df['EndofRptPeriod']
  sep = ' - '
  cols = ['calcDate', 'EndofRptPeriod']
  _df = _df.withColumn('calcDate', F.to_date(F.col('calcDate'), 'MM/dd/yyyy'))
  _df = _df.withColumn('ReportPeriod', F.concat_ws(sep, *cols))

  # FH Group
  _df = make_fh_group(_df)

  # FH Group Detail
  _df = make_fh_group_detail(_df)

  # Period
#   _df['Period'] = period
  _df = _df.withColumn('Period', F.lit(period))

  return _df





# COMMAND ----------

def main(df, n, fh_period, filemap, dataset, FYM, n_years=5, check=True):
  try:
    
    lgd = df['GiftDate'].max()
    split_donor_sets = generate_sets_to_split(df, n, check_len=check)

    df_list = split_dataframe_by_sets(df, split_donor_sets, n, check=check)

    # release memory
    if check_set_integrity(df_list, df):    
      del df

    # read schema from ADLS2
    schema = get_schema_details(filemap.SCHEMA)
    print('dataset in python module: ', dataset)
    # get dtypes
    dec = flatten_schema(schema[dataset])
    print('dec in python module: ', dec)

    for i in range(n):
      print('iteration: ', i)
      start = time.time()
      process_df(df_list[i], i, fh_period, dec, FYM, lgd)
      print('elapsed time: ', time.time() - start)

  except Exception as e:
    raise(e)
      
      
def main_spark(df, fh_period, filemap, dataset, FYM, n_years=5, check=True):
  try:
    
#     lgd = df['GiftDate'].max()
    lgd = df.agg({"GiftDate": "max"}).collect()[0][0]

    # read schema from ADLS2
    schema = get_schema_details(filemap.SCHEMA)
    print('dataset in python module: ', dataset)
    
    # get dtypes
    dec = flatten_schema(schema[dataset])
    print('dec in python module: ', dec)

    process_df(df, fh_period, dec, FYM, lgd)

  except Exception as e:
    raise(e)


def main_parallel_a(df, n, fh_period, filemap, dataset, FYM, n_years=5, check=True):
  try:
    split_donor_sets = generate_sets_to_split(df, n, check_len=check)

    df_list = split_dataframe_by_sets(df, split_donor_sets, n, check=check)

    # release memory
    if check_set_integrity(df_list, df):    
      del df

    # read schema from ADLS2
    schema = get_schema_details(filemap.SCHEMA)
    # get dtypes
    dec = flatten_schema(schema[dataset])

    df_list = [[y,x] for x,y in enumerate(df_list)]
    run_in_parallel = lambda x: process_df(x[0], x[1], fh_period, dec, FYM)
    with ThreadPoolExecutor(n) as pool:
      pool.map(run_in_parallel, df_list)

  except Exception as e:
    raise(e)

      
def main_parallel_b(df, n, fh_period, filemap, dataset, FYM, n_years=5, check=True):
  try:
    split_donor_sets = generate_sets_to_split(df, n, check_len=check)

    df_list = split_dataframe_by_sets(df, split_donor_sets, n, check=check)

    # release memory
    if check_set_integrity(df_list, df):    
      del df

    # read schema from ADLS2
    schema = get_schema_details(filemap.SCHEMA)
    # get dtypes
    dec = flatten_schema(schema[dataset])

    with Executor() as executor:
      for i in range(n):
#         p = os.path.join(path,f)
        _ = executor.submit(process_df, df_list[i], i, fh_period, dec, FYM)
    
  except Exception as e:
    raise(e)

# COMMAND ----------

# Report helpers
def add_rejoin_by_period(df, periods, pmap):
  # loop over report periods, masking data where gift date >= first date of report period
  # take the channel of the minimum date in the masked data, set to RejoinChannel %d %report period, 
  # where report period is the number in the pmap dictionary
  for p in periods:
    # prepare/filter dataframe
    mask = (F.col('ReportPeriod')==p) & (F.col('GiftDate') >= F.col('StartDate'))
    _df = df.filter(mask).orderBy('GiftDate')
    _df = _df.dropDuplicates(['DonorID'])

    # generate rejoin channel
    feature = 'ReJoinChannel%d' %pmap[p]
    _df = _df.withColumnRenamed('Channel', feature)
    
    # generate rejoin month
    month_col = 'ReJoinMonth%d' %pmap[p]
    _df = _df.withColumn(month_col, F.month(_df['GiftDate']))

    # join back to the original dataframe
    cols = ['DonorID', feature, month_col]
    df = df.join(_df.select(cols), on=cols[0], how='left')
    del _df

  return df

def add_report_period(df_all):
  if 'ReportPeriod' not in df_all.columns:
    df_all = df_all.withColumn("ReportPeriod", F.concat(F.col("StartDate"), F.lit(" - "), F.col("EndDate")))
  return df_all

def aggregate_rejoin_fields(df, pmap):
  df['ReJoinChannel'] = None
  df['ReJoinMonth'] = None
  for rp_str in df.ReportPeriod.unique():
    rp = pmap[rp_str]
    print(rp_str, rp)
    mask = df.ReportPeriod == rp_str
    df.loc[mask, 'ReJoinChannel'] = df['ReJoinChannel%d'%(rp)]
    df.loc[mask, 'ReJoinMonth'] = df['ReJoinMonth%d'%(rp)]
  return df

def combine_dataframes(df, df_all):

  cols = ['DonorID', 'ReportPeriod', 'StartDate', 'rejoin_hash'] + [x for x in df.columns if 'ReJoin' in x] 
  df = df.select(cols).dropDuplicates()
  # print(df.count())
  # df.show(n=3)

  cols = ['DonorID'] + [x for x in df.columns if 'ReJoin' in x]
  df_all = df_all.select(['DonorID', 'StartDate', 'ReportPeriod', 'rejoin_hash']).dropDuplicates()
  df = df_all.join(df.select(cols), on='DonorID', how='inner')

  df = df.toPandas().sort_values(['DonorID', 'StartDate'])
  print(df.shape)
  # df.head()

  cols = ['DonorID', 'ReportPeriod', 'rejoin_hash'] + sorted([x for x in df.columns if 'ReJoin' in x])
  df = df[cols].drop_duplicates(['DonorID', 'ReportPeriod'])
  print(df.shape)
  # df.head(10)

  return df

def ffill_channel_nulls_by_column(df, pmap):
  cols = ['ReJoinChannel', 'ReJoinMonth']
  for i in list(pmap.values())[1:]:
    for col in cols:
      df[col+str(i)] = df[col+str(i)].fillna(value=df[col+str(i-1)])
  return df


def filter_by_fhgroup(df_all, fhg='Lapsed'):
  mask = F.col('FHGroup') == fhg
  df = df_all.filter(mask)
  mask = (df['GiftDate'] >= df['StartDate']) & (df['GiftDate'] <= df['EndDate'])
  return df.filter(mask)


def get_data_for_rejoin(filemap, period_view):
  PERIOD = period_view
  print('period view: ', PERIOD)
  path = os.path.join(filemap.CURATED, 'DAV_FH_dataset_%s_EDA' %PERIOD)
  f = os.listdir(path)[0]
  path = os.path.join(path, f).split('/dbfs')[-1]
  print(path)
  df_saved = spark.read.parquet(path)
  cols = [x for x in df_saved.columns if 'ReJoin' not in x]
  df_saved = df_saved.select(cols)
  cols = ['DonorID', 'FHGroup', 'Channel', 'GiftDate', 'StartDate', 'EndDate', 'rejoin_hash']
  df_all = df_saved.select(cols)
  return df_saved, df_all

def get_rejoin_function_inputs(df):
  periods = get_unique_from_spark_col(df, 'ReportPeriod')
  pmap = {v:i for i,v in enumerate(sorted(periods))}
  return periods, pmap

def pandas_to_spark(df, df_saved):
  # Create a Spark session
  spark = SparkSession.builder.appName("JoinChannel").getOrCreate()

  # Convert Pandas DataFrame to PySpark DataFrame
  df = spark.createDataFrame(df)

  # Perform the join operation
  df = df_saved.join(df, on="rejoin_hash", how="left")

  return df

def save_spark_coalesced(df, period_view, fn='dataset'):
  # Write data to ADLS2 and then clean up spark metadata files
  print('file health period: ', period_view)

  FILENAME = '%s_FH_%s_%s_EDA' % ('DAV', fn, period_view) 
  path = os.path.join(filemap.CURATED, FILENAME).split('/dbfs')[-1]
  df.coalesce(1).write.parquet(path, mode='overwrite')

  path = os.path.join(filemap.CURATED, FILENAME)
  for f in os.listdir(path):
    if '.parquet' not in f:
      os.remove(os.path.join(path, f))
      
  files = os.listdir(path)
  handle = '%s.snappy.parquet' %fn
  os.rename(os.path.join(path, files[0]), os.path.join(path, handle))    

  return handle in os.listdir(path)