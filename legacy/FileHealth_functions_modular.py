# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor as Executor

# COMMAND ----------

def get_gift_fy(df, month, ref_col='GiftDate', report='fy'):
  condition = df[ref_col].dt.month >= month

  if month == 1:
    return df[ref_col].dt.year

  else:
    if 'fy' in report:
      return np.where(condition, df[ref_col].dt.year+1, df[ref_col].dt.year)        
    else:
      return np.where(condition, df[ref_col].dt.year, df[ref_col].dt.year-1)


def make_fh_consecutive(_df):
  # Consecutive Giver
  condition = (_df['PrevGiftFY'] == _df['ReportFY']-1) & (_df['PrevPrevGiftFY'] == _df['ReportFY']-2)
  _df['Consecutive Giver'] = np.where(condition, True, False)

  return _df


def make_fh_group(_df):
  cols = ['New', 'New Last Year', 'Lapsed', 'Reinstated Last Year', 'Consecutive Giver']
  for col in cols:
    _df['fhgd' + col] = [col if x else '' for x in _df[col].values.tolist()]

  fhgd_cols = ['fhgd' + col for col in cols]
  _df['FHGroup'] = _df[fhgd_cols].apply(lambda x: ''.join(x), axis=1)
  _df = _df.drop(fhgd_cols+cols, axis=1)

  return _df

def make_fh_group_detail(_df):
  _df['FHGroupDetail'] = 'None'
  return _df


def make_fh_lapsed(_df):
  # Lapsed
  condition = ((_df['ReportFY'] - _df['PrevGiftFY']) > 1 ) & (~_df['New'])
  _df['Lapsed'] = np.where(condition, True, False)

  return _df


def make_fh_new_cols(_df):
  # New and New Last Year
  _df['New'] = np.where(_df['FirstGiftFY'] == _df['ReportFY'], True, False)
  _df['New Last Year'] = np.where(_df['FirstGiftFY'] == _df['ReportFY']-1, True, False)

  return _df


def make_fh_reinstated(_df):
  # Reinstated Last Year
  condition = (_df['PrevGiftFY'] == _df['ReportFY']-1) & (_df['PrevPrevGiftFY'] < _df['ReportFY']-2) & (~_df['New Last Year'])
  _df['Reinstated Last Year'] = np.where(condition, True, False)

  return _df


def make_fh_report(_df, period, fym, last_gift_date):
  if period == 'r12':
    fym = last_gift_date.month
    _df['GiftFiscal'] = get_gift_fy(_df, fym, report=period)
    _df['ReportFY'] = pd.to_datetime(_df['calcDate']).dt.year
  else:
    _df['ReportFY'] = pd.to_datetime(_df['EndofRptPeriod']).dt.year        
  return _df, fym
        


def make_report_dates(df, m, y, p, lgd):
  if p == 'fy':
    if m == 1:
      df['calcDate'] = '01/01/%s' % str(y)
      df['EndofRptPeriod'] = '12/31/%s' % str(y)
    else:
      month = str(m).zfill(2)
      day = str(calendar.monthrange(y, m)[-1]).zfill(2)
      df['calcDate'] = '%s/01/%s' % (month, str(y-1))

      month = str(m-1).zfill(2)
      day = str(calendar.monthrange(y, m-1)[-1]).zfill(2)        
      df['EndofRptPeriod'] = '%s/%s/%s' % (month, day, str(y))
            
  elif p == 'fytd':
    prev = lgd.replace(day=1) - timedelta(days=1)
    last_month = str(prev.month).zfill(2)
    last_day = str(prev.day).zfill(2)
    if m == 1:
      df['calcDate'] = '01/01/%s' % str(y)
      df['EndofRptPeriod'] = '%s/%s/%s' % (last_month, last_day, str(y))
    else:
      month = str(m).zfill(2)
      day = str(calendar.monthrange(y, m)[-1]).zfill(2)
      df['calcDate'] = '%s/01/%s' % (month, str(y))
      df['EndofRptPeriod'] = '%s/%s/%s' % (last_month, last_day, str(y+1))
            
  elif p == 'r12':
#     start = (lgd.replace(day=1) - relativedelta(years=1)).date()
    start = lgd.replace(day=1)
    start = start.replace(year=y)
    df['calcDate'] = start.strftime('%m/%d/%Y')

    prev = ((start - timedelta(days=1)) + relativedelta(years=1)).date()
    last_month = str(prev.month).zfill(2)
    last_day = str(prev.day).zfill(2)
    last_year = str(prev.year).zfill(4)
    df['EndofRptPeriod'] = '%s/%s/%s' % (last_month, last_day, last_year)
        
  mask = df['GiftDate'] <= pd.to_datetime(df['EndofRptPeriod'])

  return df[mask]


def prep_for_crl(_df, fym, period):
  # Prep for Consecutive, Reinstated, Lapsed
  inner_mask = _df['GiftFiscal'] < _df['ReportFY'] #first_year
  grouped = _df[inner_mask].groupby('DonorID')['GiftDate'].max().to_frame(name='PrevGiftDate')
  grouped['PrevGiftFY'] = get_gift_fy(grouped, fym, ref_col='PrevGiftDate', report=period)
  _df = pd.merge(_df, grouped, on='DonorID', how='left')   

  inner_mask = _df['GiftFiscal'] < (_df['ReportFY'] - 1) #first_year
  grouped = _df[inner_mask].groupby('DonorID')['GiftDate'].max().to_frame(name='PrevPrevGiftDate')
  grouped['PrevPrevGiftFY'] = get_gift_fy(grouped, fym, ref_col='PrevPrevGiftDate', report=period)
  _df = pd.merge(_df, grouped, on='DonorID', how='left')

  _df['PrevGiftFY'] = _df['PrevGiftFY'].astype('Int64')
  _df['PrevGiftFY'] = _df['PrevGiftFY'].fillna(value=0)     

  _df['PrevPrevGiftFY'] = _df['PrevPrevGiftFY'].astype('Int64')
  _df['PrevPrevGiftFY'] = _df['PrevPrevGiftFY'].fillna(value=0)

  return _df



def prep_and_last_gift(_df, fym, period):
  # First Gift
  grouped = _df.groupby('DonorID')['GiftDate'].min().to_frame(name='FirstGiftDate')
  grouped['FirstGiftFY'] = get_gift_fy(grouped, fym, ref_col='FirstGiftDate', report=period)
  _df = pd.merge(_df, grouped, on='DonorID', how='left')

  # Last Gift
  grouped = _df.groupby('DonorID')['GiftDate'].max().to_frame(name='LastGiftDate')
  grouped['LastGiftFY'] = get_gift_fy(grouped, fym, ref_col='LastGiftDate', report=period)
  _df = pd.merge(_df, grouped, on='DonorID', how='left')

  return _df


def prep_report_subset(df, period, year):
  if period == 'fy':
    mask = df['GiftFiscal'] <= year
  else:
    mask = df['GiftFiscal'] <= year + 1            
  return df[mask]


def preprocess(df, fym, f='%Y%m%d'):    
  if f:
    df['GiftDate'] = pd.to_datetime(df['GiftDate'], format=f)
  else:
    df['GiftDate'] = pd.to_datetime(df['GiftDate'])
  df['GiftFiscal'] = get_gift_fy(df, fym)
  df['GiftAmount'] = format_currency(df['GiftAmount'])
  df['DonorID'] = df['DonorID'].astype(str)    
  return df


def process_df(df, i, period, dec, runner, FYM, lgd, n_years=5):
  
  this_year = datetime.now().year
  years = [i for i in range(this_year-n_years, this_year)]

  FH_COLS = [
    'DonorID', 'EndofRptPeriod', 'FHGroup', 'FHGroupDetail', 'GiftAmount',
    'GiftDate', 'GiftFiscal', 'Period', 'ReportPeriod', 'calcDate'
  ] 

  for year in years:
    
    first_year = year-1

    _df = calc_fh(df, period, year, FYM, lgd)

    # Drop unwanted columns
    _df = _df[FH_COLS]
  
    prefix = "subsets2/%s/%s/" % (period, str(year))
    
    path = os.path.join(filemap.CURATED, 'FileHealth', prefix)
    os.makedirs(path, exist_ok=True)      
#     [os.remove(path + fi) for fi in os.listdir(path) if os.path.isfile(path + fi)]
    
    fi = prefix + 'fh_' + str(i) + ".parquet"
    _df.to_parquet(os.path.join(filemap.CURATED, 'FileHealth', fi), index=False) 

  return True


# def process_df(df, i, period, dec, runner, FYM, n_years=5):

#   this_year = datetime.now().year
#   years = [i for i in range(this_year-n_years, this_year)]

#   FH_COLS = [
#     'DonorID', 'EndofRptPeriod', 'FHGroup', 'FHGroupDetail', 'GiftAmount',
#     'GiftDate', 'GiftFiscal', 'Period', 'ReportPeriod', 'calcDate'
#   ] 

#   last_gift_date = df['GiftDate'].max()
# #   print('last gift date: ', last_gift_date)

#   dfs =[]

#   for year in years:
    
#     first_year = year-1

#     _df = calc_fh(df, period, year, FYM, last_gift_date)

#     # Drop unwanted columns
#     _df = _df[FH_COLS]
  
#     _df = assert_declarations(_df, dec, runner, view=True)  
#     # prefix = "/subsets/%s/fh_" % fh_period
#     prefix = "subsets2/%s/%s/fh_" % (period, str(year))
    
#     path = os.path.join(CURATED, prefix)
#     os.makedirs(path, exist_ok=True)      
#     [os.remove(path + fi) for fi in os.listdir(path) if os.path.isfile(path + fi)]
    
#     fi = prefix + str(i) + ".parquet"
#     _df.to_parquet(CURATED + fi, index=False) 

#   return True


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
  _df['ReportPeriod'] = _df['calcDate'] + ' - ' + _df['EndofRptPeriod']

  # FH Group
  _df = make_fh_group(_df)

  # FH Group Detail
  _df = make_fh_group_detail(_df)

  # Period
  _df['Period'] = period    

  return _df






# COMMAND ----------

def main(df, n, fh_period, filemap, runner, dataset, FYM, n_years=5, check=True):
  try:
    
    lgd = df['GiftDate'].max()
    split_donor_sets = generate_sets_to_split(df, n, check_len=check)

    df_list = split_dataframe_by_sets(df, split_donor_sets, n, check=check)

    # release memory
    if check_set_integrity(df_list, df):    
      del df

    # read schema from ADLS2
    schema = get_schema_details(filemap.SCHEMA)
    # get dtypes
    dec = flatten_schema(schema[dataset])

    for i in range(n):
      print('iteration: ', i)
      start = time.time()
      process_df(df_list[i], i, fh_period, dec, runner, FYM, lgd)
      print('elapsed time: ', time.time() - start)

  except Exception as e:
      desc = 'error splitting and writing csv files'
      log_error(runner, desc, e)


# def main(df, n, fh_period, filemap, runner, dataset, FYM, n_years=5, check=True):
#   try:
#     split_donor_sets = generate_sets_to_split(df, n, check_len=check)

#     df_list = split_dataframe_by_sets(df, split_donor_sets, n, check=check)

#     # release memory
#     if check_set_integrity(df_list, df):    
#       del df

#     # read schema from ADLS2
#     schema = get_schema_details(filemap.SCHEMA)
#     # get dtypes
#     dec = flatten_schema(schema[dataset])

#     for i in range(n):
#       print('iteration: ', i)
#       start = time.time()
#       process_df(df_list[i], i, fh_period, dec, runner, FYM)
#       print('elapsed time: ', time.time() - start)

#   except Exception as e:
#       desc = 'error splitting and writing csv files'
#       log_error(runner, desc, e)
      
      
def main_parallel_a(df, n, fh_period, filemap, runner, dataset, FYM, n_years=5, check=True):
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
    run_in_parallel = lambda x: process_df(x[0], x[1], fh_period, dec, runner, FYM)
    with ThreadPoolExecutor(n) as pool:
      pool.map(run_in_parallel, df_list)

  except Exception as e:
      desc = 'error splitting and writing csv files'
      log_error(runner, desc, e)

      
def main_parallel_b(df, n, fh_period, filemap, runner, dataset, FYM, n_years=5, check=True):
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
        _ = executor.submit(process_df, df_list[i], i, fh_period, dec, runner, FYM)
    
  except Exception as e:
      desc = 'error splitting and writing csv files'
      log_error(runner, desc, e)

# COMMAND ----------

# Functions for FileHealth_Measures report

def group_agg(df, cols, ref, agg, feature):
  _grouped = df.groupby(cols).agg({ref: agg})
  _grouped.columns = [feature]
  return _grouped

def group_agg_merge(df, grouped, mask, cols, ref, feature, agg):
  _grouped = df[mask].groupby(cols).agg({ref: agg})
  _grouped.columns = [feature]
  grouped = pd.merge(grouped, _grouped, on=cols)
  return grouped

def fh_Universe(df):
  cols = ['FHGroup', 'ReportPeriod']
  ref = 'DonorID'
  agg = 'nunique'
  feature = 'Universe'
  return group_agg(df, cols, ref, agg, feature)

def fh_Responses(df, grouped, mask, cols):
  
  ref = 'DonorID'
  feature = 'Responses'
  agg = 'nunique'
  return group_agg_merge(df, grouped, mask, cols, ref, feature, agg)


def fh_Revenue(df, grouped, mask, cols):
  ref = 'GiftAmount'
  agg = 'sum'
  feature = 'Revenue'
  return group_agg_merge(df, grouped, mask, cols, ref, feature, agg)

def fh_Gifts(df, grouped, mask, cols):
  ref = 'DonorID'
  agg = 'count'
  feature = 'Gifts'
  return group_agg_merge(df, grouped, mask, cols, ref, feature, agg)


def fh_Ratios(df):
  grouped['Retention'] = round(grouped['Responses'] / grouped['Universe'], 4)
  grouped['GiftsPerDnr'] = round(grouped['Gifts'] / grouped['Responses'], 2)
  grouped['VPD'] = round(grouped['Revenue'] / grouped['Responses'], 2)
  grouped['AvgGift'] = round(grouped['Revenue'] / grouped['Gifts'], 2)
  return grouped


def fh_Format(totals):
  totals['FHGroup'] = 'Total'
  totals = totals.reset_index()
  totals = totals.rename(columns={'index': 'ReportPeriod'})
  totals = totals.set_index(['FHGroup', 'ReportPeriod'])
  return totals
  

def apply_measures(fhp):
  
  df = pd.DataFrame()
  path = '/'.join([CURATED, "subsets", fhp])
  for f in os.listdir(path):
    print('file: ', f)
    df = df.append(pd.read_parquet(os.path.join(path, f)))
  
  print('df shape: ', df.shape)

  # Universe
  grouped = fh_Universe(df)
  
  mask = (df['GiftDate'] >= df['calcDate']) & (df['GiftDate'] <= df['EndofRptPeriod'])
  cols = ['FHGroup', 'ReportPeriod']

  # Responses
  grouped = fh_Responses(df, grouped, mask, cols)

  # Revenue
  grouped = fh_Revenue(df, grouped, mask, cols)

  # Gifts
  grouped = fh_Gifts(df, grouped, mask, cols)

  # Ratios
  grouped = fh_Ratios(grouped)

  rpt = grouped.stack().unstack(level=1)
  totals = rpt.sum(level=1, axis=0)
  cols = ['Universe', 'Responses', 'Revenue', 'Gifts']
  totals = totals.loc[cols]

  # Ratios
  totals = fh_Ratios(totals)

  # Format
  totals = fh_Format(totals)
  
  return rpt.append(totals)
