# Databricks notebook source
def get_gift_fy(df, month, ref_col='GiftDate', report='fy'):
  condition = df[ref_col].dt.month >= month

  if month == 1:
    return df[ref_col].dt.year

  else:
    if 'fy' in report:
      return np.where(condition, df[ref_col].dt.year+1, df[ref_col].dt.year)        
    else:
      return np.where(condition, df[ref_col].dt.year, df[ref_col].dt.year-1)


def make_fh_report(_df, period, fym, last_gift_date):
  if period == 'r12':
    fym = last_gift_date.month
    _df['GiftFiscal'] = get_gift_fy(_df, fym, report=period)
    _df['ReportFY'] = pd.to_datetime(_df['calcDate']).dt.year
  elif (period == 'fytd') & (fym > 1):# & (last_gift_date.month >= fym):
    _df['ReportFY'] = pd.to_datetime(_df['calcDate']).dt.year + 1
  elif period == 'fytd':
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
      y = y + 1
      df['calcDate'] = '01/01/%s' % str(y)
      df['EndofRptPeriod'] = '%s/%s/%s' % (last_month, last_day, str(y))
    else:
      month = str(m).zfill(2)
      day = str(calendar.monthrange(y, m)[-1]).zfill(2)
      if lgd.month == m:
        df['calcDate'] = '%s/01/%s' % (month, str(y-1))
      else:
        df['calcDate'] = '%s/01/%s' % (month, str(y))
        
      if (lgd.month < m) & (lgd.month!=1): # added second condition 1/25/2023
        df['EndofRptPeriod'] = '%s/%s/%s' % (last_month, last_day, str(y+1))
      else:
        df['EndofRptPeriod'] = '%s/%s/%s' % (last_month, last_day, str(y))
            
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
  return df.loc[mask]


def preprocess(df, fym, f='%Y%m%d'):    
  if f:
    df['GiftDate'] = pd.to_datetime(df['GiftDate'], format=f)
  else:
    df['GiftDate'] = pd.to_datetime(df['GiftDate'])
  df['GiftFiscal'] = get_gift_fy(df, fym)
  df['GiftAmount'] = format_currency(df['GiftAmount'])
  df['DonorID'] = df['DonorID'].astype(str)    
  return df


def process_df(df, i, period, dec, FYM, lgd, n_years=6):
  
  display_period = period
  
  if period in ['cy', 'covid', 'r12']:
    period = 'fy'    
  if period in ['cytd']:
    period = 'fytd'
  
  this_year = fiscal_from_date(datetime.now(), FYM)
  # added 8/24 - to undo also change n_years back to 6
  if (period == 'fy') & (lgd.month >= FYM):
    this_year = this_year - 1
  if (period == 'r12') & (lgd.month >= FYM) & (FYM > 1):
    this_year = this_year - 1
#   if (period == 'fytd') & (datetime.now().month < FYM):
#     this_year = this_year + 1
  years = [i for i in range(this_year-n_years, this_year)]

  fh_cols = list(dec.keys())

  for year in years:
    
    # added 7/11/2022 to fix FY reset
    if period == 'fy':
      if lgd.month >= FYM:
        year = year + 1
    
    first_year = year-1
    
    _df = calc_fh(df, period, year, FYM, lgd)
    _df['Period'] = display_period

    # Drop unwanted columns
    _df = _df[fh_cols]   
    
    
    subset_dir = 'subsets3'   
#     os.makedirs(os.path.join(filemap.CURATED, subset_dir, period), exist_ok=True)
    
    prefix = "%s/%s/%s/" % (subset_dir, display_period, str(year))
    path = os.path.join(filemap.CURATED, 'FileHealth', prefix)
    os.makedirs(path, exist_ok=True)      
    
    fi = prefix + 'fh_' + str(i) + ".parquet"
    _df.to_parquet(os.path.join(filemap.CURATED, 'FileHealth', fi), index=False) 

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
  _df['ReportPeriod'] = _df['calcDate'] + ' - ' + _df['EndofRptPeriod']

  # FH Group
  _df = make_fh_group(_df)

  # FH Group Detail
  fytd_reset = (FYM != 1) & (last_gift_date.month >= FYM)
  _df = make_fh_group_detail(_df, period, fytd_reset)

  # Period
#   _df['Period'] = period    

  return _df






# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor as Executor

# COMMAND ----------



# COMMAND ----------

# fh groups

def make_fh_group(_df):
  cols = ['New', 'New Last Year', 'Lapsed', 'Reinstated Last Year', 'Consecutive Giver']
  for col in cols:
    _df['fhgd' + col] = [col if x else '' for x in _df[col].values.tolist()]

  fhgd_cols = ['fhgd' + col for col in cols]
  _df['FHGroup'] = _df[fhgd_cols].apply(lambda x: ''.join(x), axis=1)
  _df = _df.drop(fhgd_cols+cols, axis=1)

  return _df


def make_fh_consecutive(_df):
  # Consecutive Giver
  condition = (_df['PrevGiftFY'] == _df['ReportFY']-1) & (_df['PrevPrevGiftFY'] == _df['ReportFY']-2)
  _df['Consecutive Giver'] = np.where(condition, True, False)

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

# COMMAND ----------

# fh group details

def make_fh_group_detail(df, period, fytd_reset):
  
  # prep dataframe for fh group detail calcs
  df, first_year = prep_fhgroup_detail(df)   
  
  # calculated file health group detail results
  funcs = [make_fhgd_lapsed, make_fhgd_consecutive, make_fhgd_reinstated]
  for func in funcs:
    df = func(df, first_year, period, fytd_reset)
  
  # concatenate columns to get FHGD_value
  feature = 'FHGroupDetail'
  cols = ['FHGroup', 'LapsedValue','ConsecutiveValue', 'ReinstatedValue']
  df[cols] = df[cols].fillna(value='')
  df[feature] = df[cols].apply(lambda x: ''.join(x), axis=1)
  
  return df


def make_fhgd_consecutive(df, first_year, period, fytd_reset):
  FHG = 'Consecutive'   
  _df = df.drop_duplicates(['DonorID', 'GiftYear']) 

  # get list of gift years per donor
  _df = make_all_years(_df)

  # apply consecutive fuction to list
  years = [x for x in range(first_year, first_year+5)]
  for year in years:
    _df['FY'+str(year)] = [get_consecutive(x, year, period, fytd_reset) for x in _df['AllYears'].values.tolist()]

  # format results 
  return wrap_up_fhgd(df, _df, FHG, years)


def make_fhgd_lapsed(df, first_year, period, fytd_reset):
  FHG = 'Lapsed'  
  _df = df.drop_duplicates(['DonorID', 'GiftYear']) 

  # get list of gift years per donor
  _df = make_all_years(_df)

  # apply consecutive fuction to list
  years = [x for x in range(first_year, first_year+5)]
  for year in years:
    _df['FY'+str(year)] = [get_lapsed(x, year) for x in _df['AllYears'].values.tolist()] #returns the number of years between the report year and the last gift (prior to that report year)

  # format results 
  return wrap_up_fhgd(df, _df, FHG, years)
  

def make_fhgd_reinstated(df, first_year, period, fytd_reset):
  FHG = 'Reinstated'   
  _df = df.drop_duplicates(['DonorID', 'GiftYear']) 

  # get list of gift years per donor
  _df = make_all_years(_df)

  # apply consecutive fuction to list
  years = [x for x in range(first_year, first_year+5)]
  for year in years:
    _df['FY'+str(year)] = [get_reinstated(x, year, period, fytd_reset) for x in _df['AllYears'].values.tolist()]

  # format results 
  return wrap_up_fhgd(df, _df, FHG, years)


def merge_and_overwrite(df, _df, fhg):
  # cast fh group detail value to string
  col = '%sValue' %fhg
  _df[col] = _df[col].astype(str) 
  
  # join back to the larger dataframe
  df = pd.merge(df, _df, on=['DonorID', 'ReportPeriodYear'], how='left')

  # fill with empty string if FHGroup does not contain 'Consecutive'
  condition = df['FHGroup'].str.contains(fhg).fillna(value=False)
  df[col] = np.where(condition, df[col], '')
  
  return df


def make_all_years(_df):
#   grouped = _df.groupby('DonorID')['GiftYear'].unique().to_frame(name='AllYears')
  grouped = _df.groupby('DonorID')['GiftFiscal'].unique().to_frame(name='AllYears')
  _df = pd.merge(_df, grouped, on='DonorID', how='left').drop_duplicates('DonorID')
  return _df


def prep_fhgroup_detail(df):
  # create Report Period Year column
  # dates like '01/01/2022'

# df['ReportPeriodYear'] = df['calcDate'].str.split('/').str[-1]
  df['ReportPeriodYear'] = df['EndofRptPeriod'].str.split('/').str[-1]
  
  # find first report year
  first_year = min([int(x) for x in df['ReportPeriodYear'].unique()])

  # add gift year column
#   df['GiftYear'] = df['GiftDate'].dt.year
  fy_month = ((df['ReportPeriod'].str.split('/').str[0]).astype(int)).max()
  df['GiftYear'] = fiscal_from_column(df, 'GiftDate', fy_month)

#   # add Last Gift Year columns
#   _df = df.groupby('DonorID')['GiftYear'].max().to_frame(name='LastGiftYear')
#   df = pd.merge(df, _df, on='DonorID', how='left')  
  
  return df, first_year


def reshape_and_rename(_df, years):
  base_col = ['DonorID']
  _df = _df.drop_duplicates(base_col)
  # and select only the necessary aggregation columns  
  year_cols = ['FY'+str(year) for year in years]
  _df = _df[base_col + year_cols]
  _df.columns = [x.replace('FY', '') for x in _df.columns]
  return _df


def stack(_df, fhg):
  # stack the dataframe
  _df = _df.set_index('DonorID').stack().reset_index()
  _df.columns = ['DonorID', 'ReportPeriodYear', '%sValue' %fhg]
  _df['ReportPeriodYear'] = _df['ReportPeriodYear'].astype(str) 
  return _df


def wrap_up_fhgd(df, _df, fhg, years):
  # now that donor results are all aggregated, we can drop duplicate rows per donor  
  _df = reshape_and_rename(_df, years)  
    
  # stack the dataframe
  _df = stack(_df, fhg)  

  # join back to the larger dataframe
  df = merge_and_overwrite(df, _df, fhg)
  
  return df

# COMMAND ----------

def main(df, n, fh_period, filemap, dataset, FYM, n_years=5, check=True):
  try:
    
    lgd = df['GiftDate'].max()
    print('last gift date: ', lgd)
    split_donor_sets = generate_sets_to_split(df, n, check_len=check)

    df_list = split_dataframe_by_sets(df, split_donor_sets, n, check=check)

    # release memory
    if check_set_integrity(df_list, df):    
      del df

    # read schema from ADLS2
    schema = get_schema_details(filemap.SCHEMA)
    
    # get dtypes
    dec = flatten_schema(schema[dataset])
    
    if check:
      print('dataset in python module: ', dataset)
      print('dec in python module: ', dec)

    for i in range(n):
      print('iteration: ', i)
      start = time.time()
      process_df(df_list[i], i, fh_period, dec, FYM, lgd)
      print('elapsed time: ', time.time() - start)

  except Exception as e:
    raise(e)


