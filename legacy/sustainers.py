# Databricks notebook source
'''
Sustainer fields
'''

# COMMAND ----------


# SustainerGiftFlag
def make_sustainer_gift_flag(df, gift_col, kw="Recur", sust_label="Sustainer Gift", no_sust_label="1X Gift"):
  '''
  Creates a binary categorical column labelled with either sust_label or no_sust_label.
  Arguments:
  - df: a Pandas dataframe
  - gift_col: a string representing the name of the column to check for the presence of the kw (keyword) 
  - kw: a string ("Recur" by deault) representing the client specific keyword used to identify recurring gifts
  - sust_label: a string ("Sustainer Gift by default") representing the client specific sustainer label
  - no_sust_label: a string ("1X Gift by default") representing the client specific non-sustainer label
  Returns:
  - the categorical vector
  '''
  feature="SustainerGiftFlag"
  boolean_mask = df[gift_col].str.contains(kw)
  df[feature] = np.where(boolean_mask, sust_label, no_sust_label)
  return df[feature]

# FirstSustainerGiftDate
def make_first_sustainer_gift_date(df, gift_col, group_col, date_col, kw="Recurring"):
  '''
  Identifies the first date that a gift flagged with the kw arg was given.
  Arguments:
  - df: a Pandas dataframe
  - gift_col: a string representing the name of the column to check for the presence of the kw (keyword) 
  - date_col: a string representing the name of the column containing the gift dates
  - kw: a string ("Recurring" by deault) representing the client specific keyword used to identify recurring gifts
  Returns:
  - a vector containing the first gift date (as a datetime) where the kw was included in the gift col.
  
  '''
  feature = "FirstSustainerGiftDate"
  boolean_mask = df[gift_col].str.contains(kw)
  df[feature] = groupby_and_transform(df, group_col, date_col, 'min', filter=boolean_mask)
  df = df.fillna({feature: df.groupby(group_col)[feature].transform('first')})
  return pd.to_datetime(df[feature])

# SustainerFlag
def make_sustainer_flag(df, date_col, first_date, 
                        fiscal, group_col, month, day, 
                        sust_label='Sustainer', 
                        no_sust_label='1X Giver'):
  '''
  Generates the sustainer labels from the given transaction data.
  Arguments:
  - df: a Pandas dataframe
  - date_col: a string representing the name of the gift date column
  - first_date: a string representing the name of the column where the donor's 
      first gift date is stored
  - fiscal: a string representing the name of the column where the fiscal year 
      of a gift is referenced
  - month: an int representing the last month of the client's fiscal year (e.g. September -> 9)
  - day: the last day of the the last month of the client's fiscal year (e.g. 30)
  - sust_label: a string ("Sustainer Gift by default") representing the client specific sustainer label
  - no_sust_label: a string ("1X Gift by default") representing the client specific non-sustainer label
  Returns:
  - the vector containing the sust_label and no_sust_label categorical labels
  '''  
  feature = "SustainerFlag"
  sf_cond1 = df[date_col] >= df[first_date]
  sf_cond2 = df[first_date] <= pd.to_datetime({"year": df[fiscal].astype(int),
                                        "month": month, 
                                        "day": day})
  sf_cond3 = ~df[first_date].isna()

  boolean_mask = sf_cond1 & sf_cond2 & sf_cond3
  df["_SustCount"] = groupby_and_transform(df, group_col, group_col, 'count', filter=boolean_mask)

  boolean_mask = (df["_SustCount"] >= 1) & sf_cond2
  df[feature] = np.where(boolean_mask, sust_label, no_sust_label)
  df[feature] = np.where(sf_cond1, sust_label, df[feature])
  df = df.drop("_SustCount", axis = 1)
  return df[feature]

# SustainerJoinSrc
def make_sustainer_join_src(df, date_col, first_date, group_col, agg_col):
  '''
  Applies the value of the agg_col vector to all members of the group after
    filtering for appropriate gift dates.
  Arguments:
  - df: a Pandas dataframe
  - date_col: a string representing the name of the gift date column
  - first_date: a string representing the name of the column where the donor's 
      first gift date is stored
  - group_col: a string representing the name of the column to group by
      e.g. "DonorID"
  - agg_col: a string representing the label to be applied to all members of the group
      e.g. "AppealID"
  Returns:
  - the transformed dataframe where all group members share the same agg_col value
  '''
  boolean_mask = df[date_col] >= df[first_date]
  return groupby_and_transform(df, group_col, agg_col, 'first', filter=boolean_mask)
  

# SustainerJoinFiscal
def make_sustainer_join_fiscal(df, first_date, month, gift_date):
  '''
  Identifies the fiscal year that the donor became a sustainer, or explicitly specifies None
    if the gift date preceedes the first date at which the donor became a sustainer
  Arguments:
  - df: a Pandas dataframe
  - first_date: a string representing the column containing the date at which the donor
      became a sustainer
  - month: an int representing the last month of the client's fiscal year (e.g. September -> 9)
  - gift_date: a string representing the column containing the gift date
  Returns:
  - a vector containing the fiscal year that the donor became a sustainer, or None
  '''
  feature = "SustainerJoinFiscal"
  df[feature] = fiscal_from_column(df, first_date, month) 
  return np.where(df[gift_date] >= df[first_date], df[feature], None)

# SustainerJoinMonth
def make_sustainer_join_month(df, first_date, gift_date):
  '''
  Identifies the month at which the donor became a sustainer, or explicitly specifies None
    if the gift date preceedes the first date at which the donor became a sustainer
  Arguments:
  - df: a Pandas dataframe
  - first_date: a string representing the name of the column containing the date
      at which a donor became a sustainer
  - gift_date: a string representing the gift date column
  Returns:
  - a vector containing the month that a donor became a sustainer, or None
  '''
  feature = "SustainerJoinMonth"
  df[feature] = df[first_date].dt.month
  return np.where(df[gift_date] >= df[first_date], df[feature], None)  

# SustainerJoinProgram
def make_sustainer_join_program(df, gift_date, first_date, group_col, agg_col):
  '''
  Applies the value of the agg_col vector to all members of the group after
    filtering for appropriate gift dates.
  Arguments:
  - df: a Pandas dataframe
  - gift_date: a string representing the name of the gift date column
  - first_date: a string representing the name of the column where the donor's 
      first gift date is stored
  - group_col: a string representing the name of the column to group by
      e.g. "DonorID"
  - agg_col: a string representing the label to be applied to all members of the group
      e.g. "Program"
  Returns:
  - the transformed dataframe where all group members share the same agg_col value
  '''
  feature = "SustainerJoinProgram"
  boolean_mask = df[gift_date] == df[first_date]
  df[feature] = groupby_and_transform(df, group_col, agg_col, 'first', filter = boolean_mask)
  df = df.fillna({feature: df.groupby(group_col)[feature].transform('first')})
  return df[feature]
  
# SustainerJoinChannel
def make_sustainer_join_channel(df, ref_col, ctrl_col):
  '''
  Wrapper around the check_online_or_dm() function, with additional functionality
    added specifically for sustainers
  Arguments:
  - df: a Pandas dataframe
  - ref_col: a string representing the reference column to check for the keyword "ONLINE"
  - ctrl_col: a string representing a column to check for nulls
  Returns:
  - a vector containing the result of the call to check_online_or_dm() if the 
      SustainerFlag col == "Sustainer", otherwise None
  '''
  feature = "SustainerJoinChannel"
  df = check_online_or_dm(df, feature=feature, 
                          ref_col=ref_col,
                          ctrl_col=ctrl_col) 
  df[feature] = np.where(df["SustainerFlag"] != "Sustainer", None, df[feature])
  return df[feature]


# COMMAND ----------

def make_fh_sustainer_fields(df):
  '''
  Wrapper around the Sustainer component fuctions.  
  See individual function docstrings for details.
  Arguments:
  - df: a Pandas dataframe
  Returns:
  - the original dataframe, transformed to include the specified Sustainer columns
  '''
  df["SustainerGiftFlag"] = make_sustainer_gift_flag(df, gift_col="GiftType")
  df["FirstSustainerGiftDate"] = make_first_sustainer_gift_date(df, gift_col="GiftType", 
                                                                group_col="DonorID", 
                                                                date_col="GiftDate")
  df["SustainerFlag"] = make_sustainer_flag(df, date_col="GiftDate", 
                           first_date="FirstSustainerGiftDate", 
                           fiscal="GiftFiscal", 
                           group_col="DonorID", 
                           month=fh.fy_last_month, 
                           day=fh.fy_last_day)
  df["SustainerJoinSrc"] = make_sustainer_join_src(df, date_col="GiftDate", 
                                                   first_date="FirstSustainerGiftDate", 
                                                   group_col="DonorID", 
                                                   agg_col="AppealID")
  df["SustainerJoinFiscal"] = make_sustainer_join_fiscal(df, first_date="FirstSustainerGiftDate", 
                                                         month=fh.fy_last_month, 
                                                         gift_date='GiftDate')
  df["SustainerJoinMonth"] = make_sustainer_join_month(df, first_date="FirstSustainerGiftDate", 
                                                       gift_date='GiftDate')
  df["SustainerJoinProgram"] = make_sustainer_join_program(df, gift_date="GiftDate", 
                                                           first_date="FirstSustainerGiftDate", 
                                                           group_col="DonorID", 
                                                           agg_col="Program")
  df["SustainerJoinChannel"] = make_sustainer_join_channel(df, ref_col="SustainerJoinSrc",
                                                           ctrl_col="SustainerJoinProgram")
  return df

