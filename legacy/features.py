# Databricks notebook source
# This notebook provides functions specifically applicable to the creation of those features which are used across multiple clients.


# COMMAND ----------

# MAGIC %run ./utilities

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

# Shared feature generating functions
def make_feature_Acq(df, col, length, firstchars, true_label, false_label):
  '''
  Generates the "Acq" feature vector which is used in multiple tables. 
  -  Checks that the length of the column element is equal to the length specified
  -  Checks that the first characters of the column element are in the specified list
  -  Combines these checks and labels the intersection with the specified value
  Arguments:
  -  df: a Pandas dataframe
  -  col: a string representing the column to check
  -  length: an int representing the length to be used for comparison
  -  firstchars: a list containing the first characters to be used for comparison
  -  true_label: a string representing the label to be given to rows meeting the rule
  -  false_label: a string representing the label to be given to rows that do not match the rule
  Returns:
  -  a vector containing the labels specified, i.e. true_label when the rule is met, 
      false_label otherwise
  '''
  be_1 = check_string_length(df, col, length)
  be_2 = check_string_firstchars(df, col, firstchars)
  boolean_mask = (be_1) & (be_2)
  return np.where(boolean_mask, true_label, false_label)

def make_feature_Day1_Fiscal(df, year_col, cond_col, month=7, day=1):
  '''
  Generates the first day of the fiscal year for a given date.
  Arguments:
  - df: a Pandas dataframe
  - year_col: a string representing the column in the dataframe containing
      the fiscal year of a particular gift
  - cond_col: a string representing the column of the dataframe used to force nulls
  - month (default = 10): an int representing the month number, i.e. 10: October
  - day (default = 1): an int representing the first day of the month
  Returns:
  - a datetime representing the first day of the fiscal year
  '''
  feature_vector = pd.to_datetime({'year':df[year_col]-1, 'month':month, 'day':day}, errors='coerce')
  boolean_mask = df[cond_col].isnull()
  feature_vector[boolean_mask] = None
  return feature_vector

def make_feature_End_Fiscal(df, year_col, cond_col, month=6, day=30):
  '''
  Generates the last day of the fiscal year for a given date.
  Arguments:
  - df: a Pandas dataframe
  - year_col: a string representing the column in the dataframe containing
      the fiscal year of a particular gift
  - cond_col: a string representing the column of the dataframe used to force nulls
  - month (default = 9): an int representing the month number, i.e. 9: September
  - day (default = 30): an int representing the last day of the month
  Returns:
  - a datetime representing the last day of the fiscal year
  '''
  feature_vector = pd.to_datetime({'year':df[year_col], 'month':month, 'day':day}, errors='coerce')
  boolean_mask = df[cond_col].isnull()
  feature_vector[boolean_mask] = None
  return feature_vector

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


def make_feature_channel_behavior(df, feature, file_health=False, ref_col='Program'):
  '''
  Determines the presence of online or DM gifts and assigns the 
    corresponding ChannelBehavior label to a given dataframe 
  Arguments:
  - df: a Pandas dataframe
  - feature: a string representing the name of the column to be used to record
      the ChannelBehavior label
  - file_health (default=False): a boolean used to differentiate between reporting
      schemes (FileHealth or CampaignPerformance).  
        FileHealth looks at gift dates within a calendar period
        CampaignPerformance looks at gift dates preceeding a given mail date.
  Returns:
  - a Pandas dataframe including the additional column containing the ChannelBehavior labels.  
  '''
  helper = "CountOnline" 
  if not file_health:
    boolean_mask = df["GiftDate"] < df["MailDate"]
  else:
    boolean_mask = df["GiftDate"] <= df["EndofRptPeriod"]
  grouped = prep_channel_group(df[boolean_mask], 
                               helper, 
                               groupby_cols=['DonorID', ref_col])
  df = grouped_channel_to_bool(df, grouped, helper)

  helper = "CountDM"
  grouped = prep_channel_group(df.dropna(subset=[ref_col]), 
                               helper, 
                               groupby_cols=['DonorID', ref_col])
  
  df = grouped_channel_to_bool(df, grouped, helper)
  
  return label_channel_from_conditions(df, feature, 'CountOnline', 'CountDM')


def make_feature_cume12id(df, gift_ref_col, dim_table):
  '''
  Creates the RFM column Cume12ID containing the label for the cumulative sum (in dollars) of the 
  gifts given before the campaign mail date.
  Arguments:
  - df: a Pandas dataframe
  - gift_ref_col: a string representing the name of the reference column containing the
    gift date / gift amount pairs to be included in the RFM calculation.
  - feature: a string representing the name of the dimension table to reference in the RFM calculation.
  Returns:
  - a vector containing the RFM label strings associated with each row of the original dataframe.
  '''
  helper = 'Cume12Gifts'
  df[helper] = [sum(i[1] for i in x) for x in df[gift_ref_col].values.tolist()]
  return get_RFM_values(df, dim_table, helper)

def make_feature_freqid(df, gift_ref_col, helper, feature="FreqID"):
  '''
  Creates the RFM column FreqID containing the label for the number of gifts given before 
  the campaign mail date.
  Arguments:
  - df: a Pandas dataframe
  - gift_ref_col: a string representing the name of the reference column containing the
    gift date / gift amount pairs to be included in the RFM calculation.
  - helper: a string representing the (temporary/private) name assigned to the column used for 
    intermediate calculations.
  - feature: a string representing the name of the dimension table to reference in the RFM calculation.
  Returns:
  - a vector containing the RFM label strings associated with each row of the original dataframe.
  '''
  df[helper] = [sum(1 for i in x) for x in df[gift_ref_col].values.tolist()]
  return get_RFM_values(df, feature, helper, closed="both")


def make_feature_FuseDmFilter(df, kw_list, 
                     feature="FuseDmFilter",
                     ref_col="CampaignID"):
  '''
  Creates a boolean vector for use in filtering data.
  Arguments:
  - df: a Pandas dataframe
  - kw_list: a list containing keywords, such that if the ref_col value
      is in the keyword, the feature value should be True.
  - feature (default="FuseDmFilter"): a string representing the column name
      to be assigned to the boolean vector
  - ref_col (default="CampaignID"): a string representing the name of the 
      column to be checked for the presence of keywords
  Returns:
  - the original dataframe:
      with the feature column added
      filtered to remove rows where the feature value is null
  '''
  df[feature] = [str(x) in kw_list for x in df[ref_col].values.tolist()]
  return df.dropna(subset=[feature])


def make_feature_gift_level_id(df, dim_table, dim_label, cols_to_drop, ref_col):
  '''TODO'''
  df = df.reset_index(drop=True)
  dim_table, _mapper = prepare_label_map(df=dim_table, 
                                       feature=dim_label, 
                                       cols_to_drop=cols_to_drop)  
  return map_in_labels(df, dim_table, ref_col, _mapper)


def make_feature_hpcid(df, gift_ref_col, helpers, feature="HpcID"):
  '''
  Creates the RFM column HpcID containing the label for the highest dollar amount of the 
  set of gifts given before the campaign mail date.
  Arguments:
  - df: a Pandas dataframe
  - gift_ref_col: a string representing the name of the reference column containing the
    gift date / gift amount pairs to be included in the RFM calculation.
  - helpers: a list of strings representing the (temporary/private) names assigned to the columns used for 
    intermediate calculations.
  - feature: a string representing the name of the dimension table to reference in the RFM calculation.
  Returns:
  - a vector containing the RFM label strings associated with each row of the original dataframe.
  '''
  # helper values  
  df[helpers[0]] = [[i[1] if i==i else 0 for i in x] for x in df[gift_ref_col].values.tolist()]
  df[helpers[0]] = np.where(df[helpers[0]], df[helpers[0]],  [0.0])
  df[helpers[0]] = [x if isinstance(x,list) else [x] for x in df[helpers[0]].values.tolist()]

  df[helpers[1]] = [max(i for i in x) for x in df[helpers[0]].values.tolist()]

  return get_RFM_values(df, feature, helpers[1])


def make_feature_AmountIDLookup(df, gift_ref_col, feature="AmountIDLookup"):
  '''
  Creates the RFM column AmountIDLookup containing the label for the dollar amount of the most 
  recent gift before the campaign mail date.
  Arguments:
  - df: a Pandas dataframe
  - gift_ref_col: a string representing the name of the reference column containing the
    gift date / gift amount pairs to be included in the RFM calculation.
  - feature: a string representing the name of the dimension table to reference in the RFM calculation.
  Returns:
  - a vector containing the RFM label strings associated with each row of the original dataframe.
  '''
  # Get the most recent GiftDate, GiftAmount pair
  helper = 'MostRecentGiftPair'
  df[helper] = [x[-1] for x in df[gift_ref_col].values.tolist()]

  # Get the amount from the most recent pair
  last_helper = helper
  helper = 'MostRecentGiftAmount'
  df[helper] = [x[-1] if isinstance(x,list) else 0 for x in df[last_helper].values.tolist()]

  return get_RFM_values(df, feature, helper)


def make_feature_mrcid(df, gift_ref_col, mail_ref_col, helpers, feature="MrcID"):
  '''
  Creates the RFM column MrcID which represents the number of months between the mail date of the campaign
  and the most recent gift date.
  Arguments:
  - df: a Pandas dataframe
  - gift_ref_col: a string representing the name of the gift date column to be used for comparison in the 
    RFM calculation, as only gift dates before the campaign mail date are needed.
  - mail_reference_col: a string representing the name of the campaign mail date column used as a cut off
    for determining which gift dates to include.
  - helpers: a list of strings representing the (temporary/private) names assigned to the columns used for 
    intermediate calculations.
  - feature: a string representing the name of the dimension table to reference in the RFM calculation.
  Returns:
  - a vector containing the RFM label strings associated with each row of the original dataframe.
  '''
  # Get just the dates of the gifts before the current MailDate
  df[helpers[0]] = [[i[0] if i==i else pd.NaT for i in x] for x in df[gift_ref_col].values.tolist()]
  df[helpers[0]] = np.where(df[helpers[0]], df[helpers[0]], pd.NaT)
  df[helpers[0]] = [x if isinstance(x,list) else [x] for x in df[helpers[0]].values.tolist()]

  # Get the most recent gift before the current MailDate
  df[helpers[1]] = [max(i for i in x) for x in df[helpers[0]].values.tolist()]
  df[helpers[1]] = pd.to_datetime(df[helpers[1]])

  # Get the number of months between the MailDate and most recent GiftDate
  df[helpers[2]] = ((df[mail_ref_col] - df[helpers[1]]) / np.timedelta64(1, 'M'))
  df[helpers[2]] = np.where(df[helpers[2]] == df[helpers[2]], 
                                round(df[helpers[2]], 0), 
                                df[helpers[2]])
  
  # map in the RFM id label and return
  return get_RFM_values(df, feature, helpers[2], closed='both')


def make_feature_new_existing(df, ref_col="FHGroup"):
  '''
  TODO
  '''
  feature = 'New/Existing'
  df[feature] = np.where(df[ref_col] == 'New', 'New', 'Existing')
  df[feature] = np.where(df[ref_col] == "No Giving History", "(blank)", df[feature])
  return df[feature]


def make_feature_sust_mrcid(df,
                            feature='Sust_MrcID',
                            helpers=['SustGiftDates', 
                                     'MostRecentSustGiftDate', 
                                     'SustMrcGapMonths'],
                            flag='SustainerFlag',
                            label='Sustainer',
                            rfm_col='SustGiftPairs',
                            agg_col='AllGifts',
                            gift_col='GiftDate',
                            nan_value='Z: None'):
  '''
  Creates the RFM column Sust_MrcID which is essentially the MrcID calculation when the SustainerFlag is True.
  Arguments:
  - df: a Pandas dataframe
  - helpers: a list of strings representing the (temporary/private) names assigned to the columns used for 
    intermediate calculations.
  - flag: a string representing the column name of the SustainerFlag field.
  - label: a string representing the label expected in the flag column when Sustainer == True.
  - rfm_col: a string representing the name of the column to be assigned to the subset of aggregated columns
    necessary for this particular RFM calculation.
  - agg_col: a string representing the name of the column containing the gift date / gift amount pairs
    to be included in the RFM calculation. 
  - gift_col: a string representing the name of the column containing the date used for comparison in the 
    RFM calculation.
  - nan_value: a string representing the label to be assigned when the RFM calculation returns None/null/NA/etc.
  Returns:
  - a vector containing the RFM label strings associated with each row of the original dataframe.
  '''
  sust_df = df[df[flag] == label].dropna()
  sust_df[rfm_col] = get_RFM_pairs(sust_df, agg_col, gift_col, rfm_col)

  sust_df[feature] = make_feature_mrcid(sust_df, rfm_col, gift_col, helpers)
  sust_df[feature] = null_out_rfm(sust_df, feature)
  df = df.merge(sust_df[feature], how='left', left_index=True, right_index=True)
  return np.where(df[feature] != df[feature], nan_value, df[feature])


# def make_feature_TSE_program(df):
#   '''
#   Identifies the appropriate Program string based on the Appeal ID.
#   Arguments:
#   - df: a row of a Pandas dataframe
#   Returns:
#   - the the correctly labeled element of the Program column
#   '''  
#   null_cols = ["CORPORATE", "CAR", "CHAR", "CFC", "P4P", "WM"]
#   additional_nulls = ["CHI", "LF08","PALM","Peer","SCFr","STO","WAS"]
#   all_null_cols = null_cols + additional_nulls

#   char_to_label = {
#     "C": "Canada Appeal",
#     "H": "US Appeal 0-12 months",
#     "L": "US Appeal 13-36 months" ,
#     "W": "Welcome Kit",
#     "P": "US Prospect",
#     "Q": "US Deep Lapsed 37+",
#     "X": "Canada Prospect",
#     "S": "Sustainer"
#   }
#   if df["AppealID"] in all_null_cols:
#     return None
#   elif df["AppealID"] == "P19ERROR":
#     return "US Prospect"
#   elif ((len(df["AppealID"]) == 3) | (len(df["AppealID"]) == 4)):
#     first_char = df["AppealID"][0]
#     if first_char in char_to_label.keys():
#       return char_to_label[first_char]
#   elif df["AppealID"].upper() == "REC":
#     return "Sustainer"
#   elif df["AppealID"].upper() == "ONLINE":
#     return "Online"
#   else:
#     return None

  
def make_feature_TSE_Program(df, feature, null_cols, char_map):  
  '''
  Uses TSE specific AppealID rules to determine the Program.
  Arguments:
  - df: a Pandas dataframe
  - feature: a string representing the name of the Program column
  - null_cols: a list of strings representing AppealIDs where Program should be None
  - char_map:  a dict mapping from single letter code to strings, where the
      single letter code is the first character of the AppealID, and the string
      is the desired program.
  Returns:
  - a Pandas dataframe with the added feature column
  '''
  # record original column set
  cols = set(df.columns)
  
  # check specific AppealIDs 
  appeal_ids = ["P19ERROR", "REC", "ONLINE"]
  for id in appeal_ids:
    df[id] = df['AppealID'].str.upper() == id
  
  # check lengths
  df['lenAppealID'] = df['AppealID'].str.len()
  df['checkLength'] = (df['lenAppealID'] == 3) | (df['lenAppealID'] == 4)
  
  # check first character
  df['firstChar'] = df['AppealID'].str[0]
  df['inCharMap'] = df['firstChar'].isin(char_map.keys())
  
  # check if in given null columns
  df['inAllNulls'] = df['AppealID'].isin(null_cols)
  
  df[feature] = None
  df[feature] = np.where(df['P19ERROR'], "US Prospect", df[feature])
  df[feature] = np.where(df['REC'], "Sustainer", df[feature])
  df[feature] = np.where(df['ONLINE'], "Online", df[feature])
  df[feature] = np.where(df['checkLength'] & df['inCharMap'],
                        df['firstChar'].map(char_map),
                        df[feature])
  df[feature] = np.where(df['inAllNulls'], None, df[feature])
  
  cols = list(cols)
  cols.append(feature)  
  
  return df[cols]


def make_feature_TSE_ProgramFilter(df, feature, char_list, kw_list):
  
  cols = set(df.columns)

  # check lengths
  df['lenAppealID'] = df['AppealID'].str.len()
  df['lenAppealIDeq3or4'] = (df['lenAppealID'] == 3) | (df['lenAppealID'] == 4)
  df['lenAppealIDgr4'] = df['lenAppealID'] >= 4

  # check first character
  df['AppealID_0'] = df['AppealID'].str[0]
  df['inFirstCharList'] = df['AppealID_0'].isin(char_list)

  # check for dashes
  df['AppealID_1'] = df['AppealID'].str[1]
  df['AppealID_2'] = df['AppealID'].str[2]
  df['AppealIDwithDash'] = (df['AppealID_1'] == '-') | (df['AppealID_2'] == '-')

  # check for keywords
  df['kwInAppealID'] = df['AppealID'].str.contains('|'.join(kw_list))

  df[feature] = None
  df[feature] = np.where(df['lenAppealIDeq3or4'] & df['inFirstCharList'], "FuseDM", df[feature])
  df[feature] = np.where(df['AppealIDwithDash'] & df['lenAppealIDgr4'], "FuseDM", df[feature])
  df[feature] = np.where(df['kwInAppealID'], "FuseDM", df[feature])
  df[feature] = df[feature].fillna("Other")

  cols = list(cols)
  cols.append(feature)
  return df[cols]


def make_feature_years_on_file(df, sd='calcDate', gd='FirstGiftDate'):
  '''
  TODO
  '''
  year_dict = {
    0 : "No Giving",
    1 : "1st Year",
    2 : "2nd Year",
    3 : "3rd Year",
    4 : "4th Year",
    5 : "5+ Years"
  }
  
  feature = "YearsOnFile"
  DAYS_IN_YEAR = 365
  df[feature] = df[sd] - df[gd]
  df[feature] = df[feature].dt.days
  df[feature] = df[feature] // DAYS_IN_YEAR
  df[feature] = np.where(df[feature] >= 5, 5, df[feature])
  df[feature] = np.where(df[feature] != df[feature], 0, df[feature])
  df[feature] = df[feature].map(year_dict)
  df[feature] = np.where(df[feature] != df[feature], "No Giving", df[feature])
  return df[feature]

# COMMAND ----------

# Helper functions for commonly created feature vectors
def add_bool_cols(df, col):
  '''
  Creates boolean columns given from based on data found in the dataframe
  Arguments:
  - df: a Pandas Dataframe
  - col: a string representing a column of lists containing data to become column names:
  Returns:
  - a Pandas dataframe with the original columns and boolean columns added
      e.g. col = years, if years = [[2020, 2021], [2018, 2020], ..., [2018, 2019, 2020]]
      will lead to
               2018      2019      2020      2021
               False     False     True      True
               True      False     True      False
               ...
               True      True      True      False
  '''
  values = list(set([a for b in df[col].tolist() for a in b]))
  for val in values:
    try:
      df[int(val)] = [val in vals for vals in df[col]]
    except:
      df[val] = [val in vals for vals in df[col]]
  return df


def assign_channel_behavior_labels(df, col):
  '''
  Takes a sparse dataframe (with boolean columns) and assigns row labels
  according to a given rule.
  Arguments:
  - df: a Pandas dataframe with boolean columns
  - col: a string representing the column name of the summarized count
      of the boolean columns
  Returns:
  - a vector containing the labels "Other Giving", "Online Only", "Multi Channel"
      or "DM Only"
  '''
  other_giving = df[col] == 0
  if "Online" in df.columns:
    online_only = (df[col] == 1) & df["Online"]
    mixed_channel=(df[col] > 1) & df["Online"]
    dm_only = (df[col] >= 1) & ~df["Online"]
  else:
    online_only = False
    mixed_channel = False
    dm_only = (df[col] >= 1)

  df['cb'] = None
  df['cb'] = np.where(other_giving, "Other Giving", df['cb'])
  df['cb'] = np.where(online_only, "Online Only", df['cb'])
  df['cb'] = np.where(mixed_channel, "Multi Channel", df['cb'])
  df['cb'] = np.where(dm_only, "DM Only", df['cb'])

  return df['cb']


def collect_rolling(df, years, agg_col='AllGifts', mail_col='MailDate'):
  '''
  Creates a vector containing the (gift_date, gift_amount) pairs preceding the date identified
  in the "mail_col" column by the number of years passed to "years".
  Arguments:
  - df: a Pandas dataframe
  - years: an int representing the number of years to include.
  - agg_col: a string representing the name of the column containing all gifts before the date 
    identified in the "mail_col" column.
  - mail_col: a string representing the name of the column containing the mail date of the campaign.
  Returns:
  - A vector containing the (gift_date, gift_amount) pairs which occurred in the time specified.
  '''
  return [[i for i in x[0] if x[1]>i[0]>= x[1]-pd.DateOffset(years=years)] for x in df[[agg_col, mail_col]].values.tolist()]


def expand_and_flatten(df, boolean_mask, group_col, feature_col, col_name):
  '''
  Creates a table with group_col values as rows, and a boolean value for each of 
    values in feature_col (True if present in the original dataframe, False otherwise)
  Arguments:
  - df: a Pandas dataframe
  - boolean_mask: a boolean vector used to filter the original dataframe
  - group_col: a string representing the column name to be grouped
  - feature_col: a string representing the column name to be evaluated
  - col_name: a string representing the name to be applied to the grouped column
  Returns:
  - flattened: a Pandas dataframe with one row per group_col value, and one 
      column for each element in the set of values in df[feature_col].
  - cols: a list representing the set of values in df[feature_col]
  '''
  for_calc = df[boolean_mask]
  for_calc['mono'] = [i for i in range(len(for_calc.index))]

  # create a temporary sparse dataframe for efficiency
  expanded = expand_list_to_bools(for_calc, group_col, feature_col, col_name=col_name)

  # take subset of distinct keys
  cols = expanded.columns.difference(for_calc.columns).tolist()
  cols.extend([group_col])
  expanded = expanded[cols]

  # flatten to a single row per donorid, True for each mode of giving, false otherwise
  flattened = expanded.groupby(group_col).max()

  # clean up resources
  del for_calc
  del expanded

  # select only channel keys and not DonorID
  cols.pop(cols.index(group_col))
  
  return flattened, cols


def expand_list_to_bools(df, group_col, feature_col, col_name='years', monotonic='mono'):
  '''
  Groups data by the group_col, and then creates a boolean column (called 'years' by default)
    for each unique value found in the feature_col.
    E.g. if the feature_col vector is comprised of the set {2018, 2019. 2020}, this function
      will create columns on the groupby dataframe for 2018, 2019, 2020.
  Arguments:
  - df: a Pandas dataframe
  - group_col: a string representing the name of the column to be used to group the data
  - feature_co: a string representing the name of the column to be used to find the
      unique set of values to be made into columns.
  - col_name (optional): a string ('years' by default) representing the name to be applied
      to the column created to hold the set of unique values of the feature_col
  - monotonic (optional): a string ('mono' by default) representing the name of the 
      monotonically increasing vector created specifically in the original dataframe to
      be used when merging temporarily re-shaped data back into itself, and so needs to 
      be preserved here.
  Returns:
  - The original Pandas dataframe with additional boolean columns, specifically one each for each of
      the member of the set of values in df[feature_col].
  '''
  grouped = df.groupby(group_col)[feature_col].unique().to_frame()
  grouped.columns = [col_name]
  grouped = add_bool_cols(grouped, col=col_name)
  df = df.merge(grouped, on=group_col)
  del grouped
  df = df.drop([col_name], axis=1)
  df = df.drop_duplicates(df.columns.difference([monotonic]))  
  return df


def _get_prefix(abbr):
  '''
  Helper to be used to facilitate the use of abbreviations internally as FHGDetail shorthand.
  Checks the passed abbreviation abbr for a first character in the set {c,l,r} and returns
    one of those three letters when present.
  Arguments:
  - abbr: a string representing the FHGDetail abbreviation,
    e.g. "r2" for "Reinstated Last Year 25-36"
  Returns:
  - a single character from the set {c,l,r} as appropriate.
  '''
  try:
    res = re.search('^[c,l,r]', abbr)
    if res:
      return res.group()
    else:
      return None
  except:
    return None
  

def _get_suffix(abbr):
  '''
  Helper to be used to facilitate the use of abbreviations internally as FHGDetail shorthand.
  Checks the passed abbreviation abbr for a digit and returns that digit when present.
  Arguments:
  - abbr: a string representing the FHGDetail abbreviation,
    e.g. "r2" for "Reinstated Last Year 25-36"
  Returns:
  - an int (i.e. 2 in the example above) as appropriate.
  '''
  try:
    res = re.search('\d+', abbr)
    if res:
      return int(res.group())
    else:
      return np.nan
  except:
    return np.nan

  
def get_df_and_mapper(dimension):
  '''
  Helper function for the often repeated task of reading a dimension table 
    into memory and preparing it for use.  
  Arguments:
  - dimension: a string representing the dimension to be used
  Returns:
  - the output of a call to prepare_label_map():
      df: a Pandas dataframe representing the dimension lookup table
          but with the labels replaced with integers
      mapper: a dictionary preserving the integer to label mappping.
  '''
  # dimension like "AmountIDLookup"
  try:
    df = pd.read_csv(filemap.SHARED + dimension + ".csv")
    cols_to_drop = ["Alpha"]
    
    # FreqID breakes the convention established with other dimensions
    # where the name of the file is also the name of the column:
    dimension = "FreqID" if dimension == "Freqid" else dimension
    
    return prepare_label_map(df, dimension, cols_to_drop)
  except Exception as e:
    desc = 'error preparing label map'
    log_error(runner, desc, e)
    
    
def get_raw_gift_pairs(df, group_col, gift_date_col, gift_amount_col, new_col, how='left'):
  '''
  Creates a vector containing the (gift_date, gift_amount) pairs associated with each member 
  of the group_col (i.e. DonorID) and adds this to the given dataframe.
  Arguments:
  - df: a Pandas dataframe
  - group_col: a string representing the name of the column to be grouped (i.e. DonorID).
  - gift_date_col: a string representing the name of the column containing the date of each gift.
  - gift_amount_col: a string representing the name of the column containing the dollar amount of each gift.
  - new_col: a string representing the name to be assigned to the aggregated column.
  - how: a string (default='left') representing the argument of the same name to be passed to pandas.merge, 
    e.g. 'left' indicates a left-join, etc.
  Returns:
  - a Pandas dataframe containing the aggregated column.
  '''
  # Get all gifts per donor and add to each row
  grouped = df.groupby(group_col)[[gift_date_col, gift_amount_col]].apply(lambda x: x.values.tolist()).to_frame()
  grouped.columns = [new_col]
  df = df.merge(grouped, on=group_col, how=how)
  del grouped
  return df


def get_RFM_pairs(df, gift_col, mail_col, rfm_col):
  '''
  Creates a vector containing the subset of (gift_date, gift_amount) pairs to be included in the particular
  RFM calculation.
  Arguments:
  - df: a Pandas dataframe
  - gift_col: a string representing the name of the column containing the date of the gift given.
  - mail_col: a string representing the name of the column containing the date of the campaign mailing.
  - rfm_col: a string representing the name to be assigned to the new column.
  Returns:
  - a vector of (gift_date, gift_amount) pairs.
  '''
  # Get gifts before MailDate
  df[rfm_col] = [[i for i in x[0] if i[0] < x[1]] for x in df[[gift_col, mail_col]].values.tolist()]
  df[rfm_col] = np.where(df[rfm_col], df[rfm_col], [pd.NaT])
  return [x if isinstance(x,list) else [x] for x in df[rfm_col].values.tolist()]

  
def get_RFM_values(df, feature, helper, closed='left'):
  '''
  Maps the RFM values to the appropriate labels as specified in the dimension tables.
  Arguments:
  - df: a Pandas dataframe
  - feature: a string representing the RFM feature.
  - helper: a string representing the helper column containint the necessary value to be mapped.
  - closed: a string (default = 'left') used to distinguish between open and closed intervals.
    E.g. 'left'  -> [a,b)
         'right' -> (a,b]
         'both'  -> [a,b]
  Returns:
  - a vector containing the appropriate RFM labels.
  '''
  dim_df, mapper = get_df_and_mapper(feature)
  return map_in_labels(df, dim_df, helper, mapper, closed)


def grouped_channel_to_bool(df, group_df, helper, join_key='DonorID', how='left', fill_value=False):
  '''
  Checks for the presence of the helper string in a grouped df, and applies a corresponding
    boolean to the parent df.
  Arguments:
  - df: a Pandas dataframe
  - group_df: a Pandas dataframe that has been grouped by DonorID and Program by default
  - helper: a string representing a label identified in the calling function and which is 
      present based on sum boolean condition
  - join_key (default="DonorID"): a string representing the column name used to join the 
      group_df and parent df when merging.
  - how (default='Left'): a string representing the <how> argument passed to the Pandas 
      merge method
  - fill_value (default=False): a boolean value to be used to fill empty elements in the 
      helper series
  Returns:
  - a Pandas dataframe with the additional boolean column labeled corresponding to the 
      helper argument.  
  '''
  # set column to boolean indicator
  group_df[helper] = np.where(group_df[helper] >= 1, True, False)
  # merge back to the transaction dataframe
  df = df.merge(group_df[helper], on=join_key, how=how)
  # convert Nan's to booleans and return
  df[helper] = df[helper].fillna(value=fill_value)
  return df


def label_channel_from_conditions(df, feature, c1, c2, 
                                  ordered_labels=[
                                    "Multi-Channel",
                                    "DM Only",
                                    "Online Only",
                                    "Other Giving"]):
  '''
  Given the four combinations of two boolean conditions, assigns a label 
    from a list of ordered labels.
  Arguments:
  - df: a Pandas dataframe
  - feature: a string representing the name of the column to store the returned label
  - c1: a string representing a boolean column in a Pandas dataframe
  - c2: a string representing a boolean column in a Pandas dataframe
  - ordered_labels (default given): a list of labels in specific order
  Returns:
  - a Pandas df with the additional feature column holding assigned labels
  '''
  df[feature] = None
  # yes c1 and yes c2
  df[feature] = np.where(df[c1] & df[c2], ordered_labels[0], df[feature])
  # no c1 and yes c2
  df[feature] = np.where(~df[c1] & df[c2], ordered_labels[1], df[feature])
  # yes c1 and no c2
  df[feature] = np.where(df[c1] & ~df[c2], ordered_labels[2], df[feature])
  # no c1 and no c2
  df[feature] = np.where(~df[c1] & ~df[c2], ordered_labels[3], df[feature])
  return df
    
# def labels_from_bool_cols(df, cols, new_col_name):
#   '''
#   Creates a column of labels given a dataframe with a number of boolean
#     columns, and where the labels to be applied is the name of the boolean
#     column == True for that row.  If no column is True for a given row, 
#     the string 'no_label' is applied.
#   Arguments: 
#   - df: a Pandas dataframe
#   - cols: a list of strings representing the set of columns to be used
#       for evaluating the correct label to be assigned.
#   - new_col_name: a string representing the name of the new column to be 
#       appended to the original dataframe to hold the labels.
#   Returns:
#   - the original Pandas dataframe with an additional column (named the 
#       value of new_col_name) holding the label for each row.
#   '''
#   temp = df[cols]
#   temp['no_label'] = temp.sum(axis=1)
#   temp['no_label'] = temp['no_label'] == 0
#   for c in temp.columns:
#     temp[c] = temp[c].astype(int)
#   temp[new_col_name] = temp.idxmax(axis=1)
#   df[new_col_name] = temp[new_col_name]
#   del temp
#   return df[new_col_name]


# def make_date_dict(df, sd="StartDate", ed="EndofPrevMonth", gd="GiftDate"):
#   '''
#   Helper method for file health calculations to facilitate looping over
#     multiple years.  Determines the range of years that a donor has given
#     and writes the corresponding start_date to a dictionary for each year.
#   Arguments:
#   - df: a Pandas dataframe
#   - sd (default='StartDate'): a string representing the column in the dataframe
#       used to identify the period start date
#   - ed (default='EndofPrevMonth'): a string representing the column in the dataframe
#       used to identify the period end date
#   - gd (default='GiftDate'): a string representing the column in the dataframe
#       used to identify the date of a given gift.
#   Returns:
#   - a dictionary of the form:
#       <number of years preceeding the latest start date> : <period year>
#   '''
#   current_year = df[ed].max().year
#   latest_start = df[sd].max()
#   date_range = int(get_date_range(df, gd))
#   start_dates = {}
#   for i in range(date_range + 1):
#     start_dates[latest_start - relativedelta(months=12*i)] = current_year - i
#   return start_dates


def map_in_abbreviations(df, feature, history=7):
  '''
  Replaces convenient file health group detail abbreviations with expected strings.
  Arguments:
  - df: a Pandas dataframe
  - feature: the column to tranform
  - history (default = 7): an int representing the largest subset label in the 
      FHGDetail_ABBRS dictionary
  Returns:
  - the transformed feature column  
  '''
  df['suffix'] = df[feature].apply(_get_suffix)
  df['prefix'] = df[feature].apply(_get_prefix)
  df['rule'] = np.where(df['suffix'] <= history, df[feature], df['prefix'] + str(history))
  df['rule'] = np.where(df['rule'] != df['rule'], df[feature], df['rule'])  
  df['rule'] = df['rule'].map(FHGDetail_ABBRS)
  df[feature] = df['rule']
  df = df.drop(['rule', 'suffix', 'prefix'], axis=1)
  return df


def map_in_labels(data, dimension, feature, mapper, closed = 'left'):
  '''
  Maps the dimension labels into the specified dataset.
  Arguments:
  - data: a Pandas dataframe representing a dataset to be labelled
  - dimension: a Pandas dataframe representing a label lookup table
  - feature: a string representing the name of the dimension
  - mapper: a dictionary with integers as keys and the dimension labels as values 
  - closed (default: 'left'): a string identifying the type of intervals passed to 
      pd.IntervalIndex.from_tuples(), with closed = inclusive.
  Returns:
  - a list of strings representing the original dimension labels.  
  '''
  # generate list of min/max tuples
  ranges = dimension.loc[:, 'Min':'Max'].apply(tuple, 1).tolist()

  # generate an interval index object
  idx = pd.IntervalIndex.from_tuples(ranges, closed=closed)

  # get the lookup table index associated with each "feature"
  indexer = idx.get_indexer(data[feature].values).tolist()

  # map the index back to the original string labels
  return [mapper[x] if x != -1 else 'Z: NoAmt' for x in indexer]


def null_out_rfm(df, feature, mail_col='MailDate'):
  '''
  Replaces values in the feature column with "Z: None" when the value in the 
    mail column is missing.
  Arguments:
  - df: a Pandas dataframe
  - feature: a string representing the column containing values to be overwritten.
  - mail_col: a string representing the column containing the control value,
    i.e. when the mail_col has missing values, the feature column value is overwritten.
  Returns:
  - an updated vector overwritten with "Z: None" when the mail_col value is missing.
  '''
  rfm_label = "Z: None"# + feature
  return np.where(pd.isnull(df[mail_col]), rfm_label, df[feature])


def prep_channel_group(df, helper,
                       groupby_cols=['DonorID', 'Program'],
                       online_ref="Online"):
  '''
  TODO
  '''
  grouped = df.groupby(groupby_cols).size().unstack()
  if helper == "CountOnline":    
    if online_ref in grouped.columns:
      grouped = grouped.rename(columns={online_ref : helper})
    else:
      grouped[helper] = 0
    return grouped  
  elif helper == "CountDM":
    if online_ref in grouped.columns:
      grouped = grouped.drop(online_ref, axis=1)
    grouped[helper] = grouped.sum(axis=1)
    return grouped

  
def prepare_label_map(df, feature, cols_to_drop = []):
  '''
  Modifies the dimension table as read from file to facilitate using programmatically
    by first preserving the index to feature label mapping, and then overwriting the feature
    label with an integer.
  Arguments:
  - df: a Pandas dataframe representing the dimension lookup table
  - feature: a string representing the feature label column of the dataframe
  - cols_to_drop (default: None): a list of column to drop from the dimension look up table
  Returns:
  - df: a Pandas dataframe representing the dimension lookup table
  - mapper: a dictionary with the lookup index as keys and the dimension labels as values
  '''
  # save the index to label mapping
  mapper=dict(zip(df.index.to_list(), df[feature]))
  # overwrite the label column with an integer for compatibility with Pandas methods below
  df[feature] = df.index
  # drop unnecessary columns
  df = df.drop(cols_to_drop, axis=1)
  
  return df, mapper
    


# COMMAND ----------

# for File Health

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

