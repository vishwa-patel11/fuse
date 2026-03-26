# Databricks notebook source
'''
file health methods used by Mercy Corp ETL process
'''

# COMMAND ----------

# for clarity and to facilitate easy looping, the code uses abbreviations for 
# each of the FHGroupDetail labels.  These labels are mapped back into the dataframe
# with a call to df["column-name"].map(ABBREVIATIONS)

FHGDetail_ABBRS = {
  'c2' : "Consecutive Giver 2 Yrs",
  'c3' : "Consecutive Giver 3 Yrs",
  'c4' : "Consecutive Giver 4+ Yrs",
  'c5' : "Consecutive Giver 4+ Yrs",
  'c6' : "Consecutive Giver 4+ Yrs",
  'c7' : "Consecutive Giver 4+ Yrs",
  'r1' : "Reinstated Last Year 13-24",
  'r2' : "Reinstated Last Year 25-36",
  'r3' : "Reinstated Last Year 37-48",
  'r4' : "Reinstated Last Year 49-60",
  'r5' : "Reinstated Last Year 61-72",
  'r6' : "Reinstated Last Year 73-84",
  'r7' : "Reinstated Last Year 85+",
  'l0' : "No Giving History",
  'l1' : "Lapsed 13-24",
  'l2' : "Lapsed 25-36",
  'l3' : "Lapsed 37-48",
  'l4' : "Lapsed 49-60",
  'l5' : "Lapsed 61-72",
  'l6' : "Lapsed 73-84",
  'l7' : "Lapsed 85+",
  'n' : 'New',
  'nly' : 'New Last Year',
  'no_label' : "No Giving History"
}

FHG_ABBRS = {
  "Consecutive Giver 2 Yrs" : "Consecutive Giver",
  "Consecutive Giver 3 Yrs" : "Consecutive Giver",
  "Consecutive Giver 4+ Yrs" : "Consecutive Giver",
  "Reinstated Last Year 13-24" : "Reinstated Last Year",
  "Reinstated Last Year 25-36" : "Reinstated Last Year",
  "Reinstated Last Year 37-48" : "Reinstated Last Year",
  "Reinstated Last Year 49-60" : "Reinstated Last Year",
  "Reinstated Last Year 61-72" : "Reinstated Last Year",
  "Reinstated Last Year 73-84" : "Reinstated Last Year",
  "Reinstated Last Year 85+" : "Reinstated Last Year",
  "Lapsed 13-24" : "Lapsed",
  "Lapsed 25-36" : "Lapsed",
  "Lapsed 37-48" : "Lapsed",
  "Lapsed 49-60" : "Lapsed",
  "Lapsed 61-72" : "Lapsed",
  "Lapsed 73-84" : "Lapsed",
  "Lapsed 85+" : "Lapsed",
  'New' : 'New',
  'New Last Year' : 'New Last Year',
  "No Giving History" : "No Giving History",
  "No Mail Date" : "No Mail Date"
}


# COMMAND ----------

# File Health Group feature columns

def make_feature_fhgroup(df, reference_col="FHGroupDetail", abbr=FHG_ABBRS):
  '''
  Creates a column in the dataframe called Fhgroup and applies labels
    given the contents of column Fhgroupdetail.
  Assumes that the reference column (typically Fhgroupdetail) has already been generated 
    and that the abbreviations in the abbr dictionary apply.
  Arguments:
  - df:  a Pandas dataframe
  - reference column (default = FHGroupDetail): a string representing the column to inspect
      for finding the keys to lookup in the abbr dict
  - abbr (default = FHG_ABBRS): a dictionary containing the file health detail to group mapping
  Returns:
  - a vector containing the approprate file health group labels.
  '''
  return df[reference_col].map(abbr)

def make_feature_fhgdetail(df, feature, group_col, feature_col, agg_col, required_col="MailDate", drop=True): 
  '''
  Creates a column in the dataframe containing the File Health Group Detail labels.
  Arguments:
  - df: a Pandas dataframe
  - feature:  a string representing the name to give the new column, e.g. "FHGroupDetail"
  - group_col: a string representing the variable to group by when analyzing historical 
      behavior, e.g. "DonorID"
  - feature_col: a string representing the column to be aggregated for assessment
      e.g. "GiftDateFiscalYear"
  - agg_col: a string representing the label to be given to the new column containing
      the aggregated feature vectors
  - required_col (default="MailDate"): an optional column to be used to overwrite values
      when insufficient data is available.  For instance, if no mail date is present in
      the data (but gift date FY and campaign FY are), the File Health Group Detail label 
      should be overwritten with "No Giving History".
  Returns:
  - the Pandas dataframe updated with the FHGroupDetail column
  '''
  cols_to_drop = []
  # See below for helper function documentation
  
  # Group by group_col and aggregated the feature_col into a vector labelled agg_col
  df = group_unique_and_merge(df, group_col, feature_col, agg_col)
  
  # Assign critical values for efficient bit wise calculations
  df, cols_to_drop = assign_critical_columns(df, reference_col, cols_to_drop) 
  
  # Assign boolean file health group identifiers for efficient bit wise calculations
  df, cols_to_drop = assign_fhg_bools(df, reference_col, agg_col, cols_to_drop)
  
  # Identify activity on the year prior to the year of interest
  df, cols_to_drop = identify_preceeding(df, agg_col, cols_to_drop)
  
  # Calculate the number of years associated with each file health group
  df, cols_to_drop = calculate_fhgd_values(df, agg_col, cols_to_drop)
  
  # Develop the abbreviated labels containing a character and a number
  df, cols_to_drop = assign_fhgd_labels(df, cols_to_drop)
  
  # Extract the file health group detail label from the various columns
  fhgd_cols = ['fhgd_l', 'fhgd_r', 'fhgd_c','fhgd_n', 'fhgd_nly']
  df[feature] = df[fhgd_cols].apply(lambda x: ''.join(x), axis=1)
  
  # drop temporary columns
  cols_to_drop.extend(fhgd_cols)
  if drop:
    df = df.drop(cols_to_drop, axis=1)
  
  # map the abbreviations to the full file health group detail labels
  df = map_in_abbreviations(df, feature)
  
  # update null or None values as appropriate
  if required_col:
    df.loc[df[required_col].isnull(), feature] = "No Giving History"
  df[feature].fillna(value='')
  
  return df






# COMMAND ----------

# File Health Group helper methods

def assign_critical_columns(df, reference_col, cols_to_drop):
  '''
  Uses the reference column to identify the years critical in determining the 
    associated file health group detail label.
  Arguments:
  - df: a Pandas dataframe
  - reference_col: a string representing the reference column to be used for 
    assessing file health (i.e. fiscal year for campaign performance)
  - cols_to_drop: a list containing the running list of temporary columns to
    be dropped at the conclusion of the file health function.
  Returns:
  - df: the updated Pandas dataframe
  - cols_to_drop: the original list, updated with the temporary column names 
    oringinating with this method.
  '''
  # identify the critical years
  df['critical'] = df[reference_col] - 1
  df['preceding_critical'] = df[reference_col] - 2   
  
  # identify gifts (true or false) on critical years
  df['gift_on_critical'] = [bool(x[0] in x[1]) for x in df[['critical', agg_col]].values.tolist()]
  df['gift_preceding_critical'] = [bool(x[0] in x[1]) for x in df[['preceding_critical', agg_col]].values.tolist()]
  
  # collect temporary columns
  cols_to_drop.extend(['critical', 'preceding_critical', 'gift_on_critical', 'gift_preceding_critical'])
  
  # return df with additional columns, as well as the updated columns-to-drop list
  return df, cols_to_drop


def assign_fhg_bools(df, reference_col, agg_col, cols_to_drop):
  '''
  Creates boolean vectors to be used to facilitate efficient labeling of the file health group details.
  Arguments:
  - df: a Pandas dataframe
  - reference_col: a string representing the reference column to be used for 
    assessing file health (i.e. fiscal year for campaign performance) 
  - agg_col: a string representing the name of the column storing the aggregated column to be inspected
    to determine the file health group label.  These values are compared against the reference_col - 
    i.e. for Campaign Performance, the agg_col column contains a list of gift fiscal years for each donor,
      while the reference_col is the fiscal year of the campaign associated with that row of data.
  - cols_to_drop: a list containing the running list of temporary columns to
    be dropped at the conclusion of the file health function.
  Returns:
  - df: the updated Pandas dataframe
  - cols_to_drop: the original list, updated with the temporary column names 
    oringinating with this method.
  '''
  # 'n' (New): True if the reference_col is <= the oldest year in the aggregated column
  #   i.e. if the fiscal year of the campaign is <= the Donor's first gift fiscal year
  df['bool_n'] = [bool(x[0] <= min(x[1])) for x in df[[reference_col, agg_col]].values.tolist()]
  
  # 'nly' (New Last Year): The same as above, but for the previous year
  df['bool_nly'] = [bool(x[0] == min(x[1])) for x in df[['critical', agg_col]].values.tolist()]
  
  # 'l' (Lapsed): True if no gift on the critical year in question, and also not New
  df['bool_l'] = [bool((not x[0]) & (not x[1])) for x in df[['gift_on_critical', 'bool_n']].values.tolist()]
  
  # 'r' (Reinstated Last Year): True if no gift given the year prior to the critical year, but a gift is
  #     given the year prior to that (and also not simply New Last Year)
  df['bool_r'] = [bool((not x[0]) & (x[1]) & (not x[2])) for x in df[['gift_preceding_critical', 'gift_on_critical', 'bool_nly']].values.tolist()]
  
  # 'c' (Consecutive Giver): True if a gift is given on both the critical year and the previous year
  df['bool_c'] = [bool((x[0]) & (x[1])) for x in df[['gift_preceding_critical', 'gift_on_critical']].values.tolist()]
  
  # record temporary columns
  cols_to_drop.extend(['bool_n', 'bool_nly', 'bool_l', 'bool_r', 'bool_c'])
  
  # return df with additional columns, as well as the updated columns-to-drop list
  return df, cols_to_drop


def assign_fhgd_labels(df, cols_to_drop):
  '''
  Assigns the abbreviated file health group label to the dataframe using the number of years
    and a boolean identifier associated with that particular category.
  Arguments:
  - df: a Pandas dataframe
  - cols_to_drop: a list containing the running list of temporary columns to
    be dropped at the conclusion of the file health function.
  Returns:
  - df: the updated Pandas dataframe
  - cols_to_drop: the original list, updated with the temporary column names 
    oringinating with this method. 
  '''
  # loop over the group prefixes, and assign a letter and number combination to the 
  # associated column, i.e. 'c3' for a consecutive giver of 3 years.
  # Also, record the temporary column names for eventual removal.
  group_prefixes = ['c', 'l', 'r']
  for gp in group_prefixes:
    df['fhgd_' + gp] = [gp + str(x[0]) if x[1] else '' for x in df[['years_' + gp, 'bool_' + gp]].values.tolist()]
    cols_to_drop.append('fhgd_' + gp)
    
  # loop over the new prefixes and assign the prefix when the associated boolean
  # identifier is True.  Also, record the temporary column names for eventual removal.
  new_prefixes = ['n', 'nly']
  for np in new_prefixes:
    df['fhgd_' + np] = [np if x else '' for x in df['bool_' + np].values.tolist()]
    cols_to_drop.append('fhgd_' + np)
    
  # return df with additional columns, as well as the updated columns-to-drop list
  return df, cols_to_drop


def calculate_fhgd_values(df, agg_col, cols_to_drop):
  '''
  Calculates the number of years associated with each file health category.
  Arguments:
  - df: a Pandas dataframe
  - agg_col: a string representing the name of the column storing the aggregated column to be inspected
    to determine the file health group label.
  - cols_to_drop: a list containing the running list of temporary columns to
    be dropped at the conclusion of the file health function.
  Returns:
  - df: the updated Pandas dataframe
  - cols_to_drop: the original list, updated with the temporary column names 
    oringinating with this method.  
  '''
  # Cast and fill the columns to be used in the calculations
  df['critical'] = df['critical'].astype("Int64")
  df['critical_filled'] = df['critical'].fillna(value=0)
  df['most_recent'] = df['most_recent'].astype("Int64")
  
  # Group the list of years in the agg_col into subsets of 
  #   continuous / consecutive years
  df['grouped_feature'] = [group_consecs(x) for x in df[agg_col]]
  
  # identify the subset of years to which the critical year in question belongs
  df['consec_group'] = [[i for i in x[0] if x[1] in i] for x in df[['grouped_feature', 'critical_filled']].values.tolist()]
  
  # update NAs with an empty list to facilitate functionality below
  df['consec_group'] = [x[0] if x else [] for x in df['consec_group'].values.tolist()]

  # Lapsed years: the critical year - the most recent gift = number of years lapsed
  df['years_l'] = [(x[0] - x[1]) for x in df[['critical', 'most_recent']].values.tolist()]
  
  # Reinstated years: lapsed years - 1 
  # only ultimately included when the Reinstated boolean is True, see assign_fhgd_labels()
  df['years_r'] = np.where(df['years_l'] == 0, 0, df['years_l'] - 1)
  
  # Consectutive years: the critical years - the oldest year in the consecutive grouping (+1)
  df['years_c'] = [x[0] - min(x[1]) + 1 if x[1] else 0 for x in df[['critical', 'consec_group']].values.tolist()]
  
  # fill NAs with zero
  year_cols = ['years_l', 'years_r', 'years_c']
  df[year_cols] = df[year_cols].fillna(value=0)
  
  # record the temporary columns
  cols_to_drop.extend(['critical_filled', 'most_recent', 'grouped_feature', 'consec_group'])
  cols_to_drop.extend(year_cols)
  
  # return df with additional columns, as well as the updated columns-to-drop list
  return df, cols_to_drop


# https://docs.python.org/2.6/library/itertools.html#examples
def group_consecs(data):
  '''
  Groups a list of numbers into continuous / consecutive subsets:
    i.e. [1,2,3,7,8,9] -> [[1,2,3],[7,8,9]]
  Arguments:
  - data: a collection (list-like) of numbers
  Returns:
  - a list of lists of consecutive groupings
  '''
  ranges =[]
  for k,g in groupby(enumerate(data),lambda x:x[0]-x[1]):
    group = (map(itemgetter(1),g))
    group = list(map(int,group))
    ranges.append(group)
  return ranges


def group_unique_and_merge(df, group_col, feature_col, agg_col):
  '''
  Finds the set of unique values in the feature_col vector grouped dy group_col,
    and creates a new column called agg_col containing these sets in list form.
  Arguments:
  - df: a Pandas dataframe
  - group_col: a string representing the name of the column to group by, e.g. DonorID
  - feature_col: a string representing the name of the column to group, e.g. GiftDateFiscalYear
  - agg_col: a string representing the desired name of the new aggregated column
  Returns:
  - the updated Pandas dataframe containing the aggregated column
  '''
  # Get the set of unique feature_col associated with the grouped group_col
  grouped = df.groupby(group_col)[feature_col].unique().to_frame()
  
  # Rename the new aggregated column
  grouped.columns = [agg_col]
  
  # Merge the grouped dataframe back to the original, on the group_col key
  df = df.merge(grouped, on=group_col, how='left')
  
  # Ensure the aggregated column is sorted (ascending)
  df[agg_col] = [sorted(x) for x in df[agg_col]]
  
  # Return the updated dataframe
  return df


def identify_preceeding(df, agg_col, cols_to_drop):
  '''
  Identify all the years in the agg_col column before the critical date.
  Arguments:
  - df: a Pandas dataframe
  - agg_col: a string representing the column storing the aggregated column to be inspected
    to determine the file health group label.
  - cols_to_drop: a list containing the running list of temporary columns to
    be dropped at the conclusion of the file health function.
  Returns:
  - df: the updated Pandas dataframe
  - cols_to_drop: the original list, updated with the temporary column names 
    oringinating with this method.
  '''
  # Identify the dates in the agg_col column less than the date in the "critical" column
  df['preceding'] = [[i for i in x[1] if i < x[0]] for x in df[['critical', agg_col]].values.tolist()]
  
  # If there are no agg_col column dates older than the "critical" date, 
  # simply store the critical date
  df['preceding'] = np.where(df['preceding'], df['preceding'], df['critical'])
  
  # Make sure the elements of the "preceding" column are list-like 
  # to facilitate the call to max() below
  df['preceding'] = [x if isinstance(x,list) else [x] for x in df['preceding'].values.tolist()]
  
  # Find the newest (i.e. most_recent) year in the preceding years before critical.
  df['most_recent'] = [max(x) for x in df['preceding'].values.tolist()]
  
  # Record temporary columns
  cols_to_drop.append('preceding')
  
  # Return the updated dataframe
  return df, cols_to_drop


# COMMAND ----------

# File Health report class

class FileHealth():
  
  def __init__(self, fy_first_month, fy_first_day, fy_last_month, fy_last_day, rolling_interval, report_interval, report_range):
    self.fy_first_month = fy_first_month
    self.fy_first_day = fy_first_day
    self.fy_last_month = fy_last_month
    self.fy_last_day = fy_last_day
    self.rolling_interval = rolling_interval
    self.report_interval = report_interval
    self.report_range = report_range
    
    self.fy_label = "Fiscal Year"
    self.fytd_label = "Fiscal YTD"
    self.r12_label = "Rolling 12"
    
    self.todays_date = pd.to_datetime('today').date()
    self.days_delta = timedelta(days=1)
    
    self.col_mapper = {
      "StartDate": "calcDate",
      "EndofPrevMonth": "EndofRptPeriod",
      "OrderState":"State"
    }
    
    
  def expand_start_date_fy(self, df, df_list, feature='StartDate', 
                           end_date='EndofPrevMonth', 
                           ref_col='GiftDate'):
    '''
    Takes a dataframe with a seed <end_date> and expands the start and end dates
      for the number of years in self.report_range according to the rules of FY reporting:
      end_date = "if max gift date >= 9/30/year (today's date) then 9/30/year (today's date) else 9/30/year (today's date) -1",
      start_date = "10/1/ year(End Date) - 1"
    Arguments:
    - df: a Pandas dataframe
    - df_list: a list of dataframes in which to collect each dataframe produced by the methond
    - feature (default: "StartDate"): a string representing the name to be assigned to 
      the column representing the start of the report interval for that row
    - end_Date (default: "EndofPrevMonth"): a string representing the name of the column
      ending the report period
    - ref_col (default: "GiftDate"): a string representing the name of the column to be used as
      the boolen mask.
    Returns:
    A Pandas dataframe expanded to contain the appropriate rows for all years in self.report_range.
    '''
    ri=self.report_interval
    fm=self.fy_first_month
    fd=self.fy_first_day
    
    for year in range(self.report_range + 1):    
      # copy base df
      _df = df.copy()
      # re-assign end_date
      _df[end_date] = _df[end_date] - pd.DateOffset(years=year)
      # create start_date
#       _df[feature] = (_df[end_date] - pd.DateOffset(years=ri)).apply(lambda dt: dt.replace(month=fm, day=fd))
      if fm > 1:
        _df[feature] = _df[end_date] - pd.DateOffset(years=ri)
      else:
        _df[feature] = _df[end_date]
        
      _df[feature] = pd.to_datetime({
        'year': _df[feature].dt.year,
        'month': fm,
        'day': fd
      })
      
      # filter and keep only rows that meet condition
      mask = _df[ref_col] <= _df[end_date]    
      _df = _df[mask]
      
      # append the temp df to the list of df's
      df_list.append(_df)
    # re-assign df_fy with all year groups
    return pd.concat(df_list, axis=0, ignore_index=True)
  
  
  def expand_start_date_fytd(self, df, df_list, feature='StartDate', 
                             end_date='EndofPrevMonth', 
                             ref_col='GiftDate'):
    '''
    Takes a dataframe with a seed <end_date> and expands the start and end dates
      for the number of years in self.report_range according to the rules of FYTD reporting:
      end_date = "if max gift date = end of gift date month then max gift date else end of month gift date - 1",
      start_date = "If month End Date <= 9 then 10/1/year (End Date) -1 else 10/1/year (End Date)"
    Arguments:
    - df: a Pandas dataframe
    - df_list: a list of dataframes in which to collect each dataframe produced by the methond
    - feature (default: "StartDate"): a string representing the name to be assigned to 
      the column representing the start of the report interval for that row
    - end_Date (default: "EndofPrevMonth"): a string representing the name of the column
      ending the report period
    - ref_col (default: "GiftDate"): a string representing the name of the column to be used as
      the boolen mask.
    Returns:
    A Pandas dataframe expanded to contain the appropriate rows for all years in self.report_range.
    '''

    ri=self.report_interval
    fm=self.fy_last_month
    fd=self.fy_last_day    
    
    for year in range(1, self.report_range + 1):    
      # copy base df
      _df = df.copy()
      # re-assign end_date
      _df[end_date] = _df[end_date] - pd.DateOffset(years=year)
      # re-assign start_date
      _df[feature] = _df[feature] - pd.DateOffset(years=year)
      # filter and keep only rows that meet condition
#       mask = _df[ref_col] <= (_df[feature] + pd.DateOffset(years=ri)).apply(lambda dt: dt.replace(month=fm, day=fd)) 
      mask = _df[ref_col] <= _df[end_date]
      _df = _df[mask]
      # append the temp df to the list of df's
      df_list.append(_df)
    # re-assign df_fy with all year groups
    return pd.concat(df_list, axis=0, ignore_index=True)
  
  
  def expand_periods(self, s, e, ri=12):
    '''
    Given a start and end date, geneates a dictionary with the year(s) before the given example as te key,
      and the offset datetime as the value
    Arguments:  
    - s (start): a datetime like object representing the start of a report period.
    - d (end): a datetime like object representing the end of a reort period.
    - ri (report interval, default =12) an int representing the desired report periods in months.
    Returns:
    rd: a dictionary with the year(s) before the given example as te key,
      and the offset datetime as the value
    '''
    rd = {0: (s,e)}
    for i in range(1, self.report_range + 1):
      rd[i] = (s - relativedelta(months=ri*i), e - relativedelta(months=ri*i))
    return rd
  
  
  def expand_subgroup(self, df, label):
    '''
    Perfomrs each of a number of preprocessing steps to each report "subgroup", 
      i.e. "fiscal", "fiscal ytd" and "rolling 12".
    Arguments:
    - df: a Pandas dataframe
    - label: a string representing the name of the report "subgroup", 
        i.e. "fiscal", "fiscaly ytd" and "rolling 12".
    Returns:
    a dataframe with the appropriate columns added
    '''
    
    # prepare dataframe
    date_cols = ['EndofPrevMonth', 'StartDate']  
    df = cast_given_dates(df, date_cols)
    
    # generate report periods
#     t = time.time()
#     df['ReportPeriod'] = fh.make_report_period(df, end='EndofPrevMonth')
#     print('report period: ', time.time() - t)

    # identifiy the period type used in the report
    df['Period'] = label

    # add feature columns for FH Group labels
    df['FileHealthReference'] = df['EndofPrevMonth'].max().year

    # add feature to generalize report period as integer year    
    # generate dictionary of start dates to years
    start_dates = self.make_date_dict(df)
    
    # add the feature vector
    df['ReportPeriod_YR'] = fh.make_ReportPeriod_YR(df, start_dates)

    # upgrade/downgrade reference
    df["ReportPeriodReference"] = df['StartDate'].map(start_dates)
    
    return df.rename(columns=self.col_mapper)
  
  
  def get_date_range(self, df, col):
    '''
    Calculates the range in years of a given dataframe column.
    Arguments:
    - df: a Pandas dataframe
    - col: a string representing a datetime column
    Returns:
    - an int representing the range of the dataframe column in years
    '''
    df[col] = pd.to_datetime(df[col])
    return int(df[col].max().year - df[col].min().year)
  
  
  def get_fy_end(self, date):
    '''
    Given any date, determines the last day of the fiscal year in which that date belongs.
    Arguments:
    - date: a datetime like object
    Returns:
    - a datetime like object representing the last day of the fiscal year
    '''
    # if max gift date >= 9/30/year (today's date) then 9/30/year (today's date) 
    if date > datetime(date.year, self.fy_last_month, self.fy_last_day).date():
      return datetime(date.year + 1, self.fy_last_month, self.fy_last_day).date()  
    # else 9/30/year (today's date) -1
    else:
      return datetime(date.year, self.fy_last_month, self.fy_last_day).date()
    
    
  def get_fy_month(self, n, month=None):
    '''
    Determines the month number of the fiscal year, i.e. where October = 1.
    Arguments:
    - n: an int representing the conventional month number (i.e. January = 1)
    - month (default = self.fy_last_month): an int representing the conventional 
        number of the last month of the fiscal year (i.e. Septempber = 9).
    Returns:
    an int representing the month number of the fiscal year.
    '''
    
    if month is None:
      month = self.fy_last_month
      
    if n > month:
      return n-month
    else:
      return n+(12-month)
    
    
  def get_fy_start(self, end_date):
    if self.fy_first_month > 1:
      return datetime(end_date.year-1, self.fy_first_month, self.fy_first_day).date()
    else:
      return datetime(end_date.year, self.fy_first_month, self.fy_first_day).date()
    
    
  def get_last_date(self, date):
    '''
    Given any date, returns the last day of the last full month (i.e. the given day if it
      is the last day of the month, otherwise the last day of the previous month).
    Arguments: 
    - date: a datetime-like object
    Returns:
    - a datetime representing the most recent last day of month
    '''
    if calendar.monthrange(date.year, date.month)[-1] == date.day:
        return date.date()
    else:
        last_month = date - relativedelta(months=1)
        last_day_last_month = calendar.monthrange(last_month.year, last_month.month)[-1] 
        return datetime(last_month.year, last_month.month, last_day_last_month).date()
  
  
  def get_latest_fy(self, today):
    if today.month >= self.fy_first_month:
      return today.year
    else:
      return today.year - 1
    
    
  def make_date_dict(self, df, 
                     sd="StartDate", 
                     ed="EndofPrevMonth", 
                     gd="GiftDate"):
    '''
    Helper method for file health calculations to facilitate looping over
      multiple years.  Determines the range of years that a donor has given
      and writes the corresponding start_date to a dictionary for each year.
    Arguments:
    - df: a Pandas dataframe
    - sd (default='StartDate'): a string representing the column in the dataframe
        used to identify the period start date
    - ed (default='EndofPrevMonth'): a string representing the column in the dataframe
        used to identify the period end date
    - gd (default='GiftDate'): a string representing the column in the dataframe
        used to identify the date of a given gift.
    Returns:
    - a dictionary of the form:
        <number of years preceeding the latest start date> : <period year>
    '''
    current_year = df[ed].max().year
    latest_start = df[sd].max()
    date_range = self.get_date_range(df, gd)
    start_dates = {}
    for i in range(date_range + 1):
      start_dates[latest_start - relativedelta(months=12*i)] = current_year - i
    return start_dates
    
    
  def make_EndofPrevMonth(self, df, feature, 
                          ref_col='GiftDate'):
  
    most_recent_gift = df[ref_col].max()

    if most_recent_gift.month >= self.fy_last_month + 1:
      year = most_recent_gift.year
    else:
      year = most_recent_gift.year - 1
      
    print('Year: ', year)

    df[feature] = [datetime(year, self.fy_last_month, self.fy_last_day).date() for x in range(len(df))]

    return pd.to_datetime(df[feature])
    
    
  def make_fy_dates(self, df, date, f, g, label):
    e = f(date)
    s = g(e)
    df = df.append(
      {
        "Period": label,
        "Date": date,
        "EndDate": e,
        "StartDate": s 
      },
      ignore_index=True
    )
    return df
  
  
  def make_fytd_window(self, date):
    '''
    Given any date, returns the fiscal year to date endpoints.
    Arguments:
    - date: a datetime-like object
    - fy_first_month: an int representing the first month of the fiscal year
    - fy_first_day: an int representing the first day of the fiscal year (always 1)
    - fy_last_month: an int representing the last month of the fiscal year
    Returns:
    - start_date: a datetime corresponding to the start of an fytd window
    - end_date: a datetime corresponding to the end of an fytd window
    '''
    end_date = self.get_last_date(date)
    if (end_date.month <= self.fy_last_month) & (self.fy_first_month != 1):
        start_date = datetime(end_date.year-1, self.fy_first_month, self.fy_first_day).date()    
    else:
        start_date = datetime(end_date.year, self.fy_first_month, self.fy_first_day).date() 
    return start_date, end_date


  def make_r12_window(self, date):
    '''
    Given any date, returns the rolling 12 month endpoints.
    Arguments:
    - date: a datetime-like object
    - rolling_interval: an int corresponding to the rolling window size
    Returns:
    - start_date: a datetime corresponding to the start of a rolling 12 window
    - end_date: a datetime corresponding to the end of a rolling 12 window
    '''
    end_date = self.get_last_date(date)
    start_date = (end_date - relativedelta(months=self.rolling_interval)) + relativedelta(days=1)
    return start_date, end_date


  def make_report_period(self, df, start = 'StartDate', end = 'EndDate', frmt = "%m/%d/%y"):
    '''
    Concatenates together two dates to form a string representation of a date range.
    Arguments:
    - df: a Pandas dataframe
    - start:  a string representing the name of the column to make up the first part of the date range
        defualt: 'StartDate'
    - end: a string representing the name of the column to make up the second part of the date range
        default: 'EndDate'
    - frmt:  a formatting string to pass to strftime() to properly parse the supplied dates
        default: '%m/%d/%y'
    Returns:
    - a string of the form "mm/dd/yy - mm/dd/yy"
    '''
    # return df[start].astype(str) + ' - ' + df[end].astype(str)
    return df[start].dt.strftime(frmt) + ' - ' + df[end].dt.strftime(frmt)
  
  
  def make_ReportPeriod_YR(self, df, date_dict, 
                                   feature='ReportPeriod_YR', 
                                   gd="GiftDate", 
                                   dl='date_list'):
    '''
    TODO
    '''
    date_list = sorted(date_dict.keys())[::-1]
    df[dl] = [date_list for x in range(len(df))]  
    # keep only dl dates when the gift date is >= dl date
    df[feature] = [[i for i in x[1] if x[0]>= i] for x in df[[gd, dl]].values.tolist()]
    # take the first (highest) dl date, i.e. the max dl date <= the gift date
    df[feature] = [x[0] if x else None for x in df[feature].values.tolist()]
    df[feature] = df[feature].map(date_dict)  
    
    return df[feature]
  
  
  