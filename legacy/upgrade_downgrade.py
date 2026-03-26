# Databricks notebook source
'''
Upgrade / Downgrade feature
'''

# COMMAND ----------

# Upgrade/Downgrade

def add_years(df, ref_col, year_col):
  '''
  Adds the boolean columns 'current_year' and 'previous_year' to the dataframe, 
    each true iff the year in question is one in which the donor gave a gift.
  Arguments:
  - df: a Pandas dataframe
  - ref_col: a string representing the name of the reference year associated 
      with that loop of the report script
  - year_col: a string representing the name of the column containing all of
      the years in which the donor gave
  Returns:
  - the original dataframe plus the two additional columns
  '''
  df['current_year'] = [bool(x[0] in x[1]) for x in df[[ref_col, year_col]].values.tolist()]
  df['previous_year'] = [bool(x[0]-1 in x[1]) for x in df[[ref_col, year_col]].values.tolist()]
  return df


def filter_years(df, gd, sd, ed):
  '''
  Creates a boolean vector for use in filtering dates to be included in the column
    containing all gift dates for each donor in each report period classification.
  Arguments:
  - df: a Pandas dataframe
  - gd: a string representing the name of the gift date column
  - sd: a string representing the name of the report period start date column
  - ed: a string representing the name of the report period end date column
  Returns:
  - a vector of booleans
      true when the gift date month falls inside the report period months
      false otherwise  
  '''
  gift_month = df[gd].dt.month
  start_month = df[sd].dt.month
  end_month = df[ed].dt.month
  return (gift_month >= start_month) | (gift_month <= end_month) 


def shift_year(df, sd, ed):
  '''
  Not implemented
  '''
  df['last_caclDate'] = df[sd] - pd.DateOffset(years=1)
  df['last_EndofRptPeriod'] = df[ed] - pd.DateOffset(years=1)
  return df


def apply_upgrade_downgrade_labels(df, diff_col, cy='current_year', py='previous_year'):
  '''
  Labels each row of a dataframe according to the given updgrade/downgrade rules.
  Arguments:
  - df: a Pandas dataframe
  - diff_col: a string representing the column that holds the difference from one row 
      to the next, i.e. when the diff is positive, the donor has upgraded, etc.
  -cy (default: 'current_year'): a string representing the column of booleans indicating
      if a donor gave a gift on the year associatiod with that row's report period
  -py (default: 'previous_year'): a string representing the column of booleans indicating
      if a donor gave a gift on the year associatiod with the previous report period year
  Returns:
  - a vector containing one of the six given labels, or None
  '''
  feature = "Upgrade/Downgrade"
  df[feature] = None
  df[feature] = np.where(df[diff_col]>0, "Upgrade", df[feature])
  df[feature] = np.where(df[diff_col]<0, "Downgrade", df[feature])
  df[feature] = np.where(df[diff_col]==0, "Same", df[feature])
  df[feature] = np.where(df[cy] & ~df[py], "NoPreviousYearGiving", df[feature])
  df[feature] = np.where(~df[cy] & df[py], "NoCurrentYearGiving", df[feature])
  df[feature] = np.where(~df[cy] & ~df[py], "NoRecentGiving", df[feature])
  return df[feature]


def get_all_report_periods_by_donor(df, 
                                    group_col,
                                    agg_col,
                                    col_name):
  '''
  Groups by each donor and collects all unique gift dates, after filtering out 
    gift dates not in the necessary report period.
  Arguments:
  - df: a Pandas dataframe
  - group_col: a string representing the column on which to group by to collect gift
      dates (e.g. "DonorID")
  - agg_col: a string representing the column on which to collect the unique set of dates
      (e.g. "ReportPeriod_YR")
  - col_name: a string representing the name to be given to the new column
  Returns:
  - the original dataframe plus the additional column     
  '''
  grouped = df[df['in_range']].groupby(group_col)[agg_col].unique().to_frame()
  grouped.columns = [col_name]
  df = df.merge(grouped, on=group_col, how='left')
  df[col_name] = df[col_name].apply(lambda d: d if isinstance(d, (list, np.ndarray)) else [])
  del grouped   
  return df


def get_earliest_report_period_by_donor(df, 
                                       group_col,
                                       agg_col,
                                       col_name):
  '''
  Groups by each donor and determines the earliest report period year
  Arguments:
  - df: a Pandas dataframe
  - group_col: a string representing the column on which to group by to collect gift
      dates (e.g. "DonorID")
  - agg_col: a string representing the column on which to determine the earliest year
      (e.g. "ReportPeriod_YR")
  - col_name: a string representing the name to be given to the new column
  Returns:
  - the original dataframe plus the additional column   
  '''
  grouped = df.groupby(group_col)[agg_col].min().to_frame()
  grouped.columns = [col_name]
  df = df.merge(grouped, on=group_col, how='left')
  del grouped
  return df


def get_gift_amounts_by_donor_and_period(df,
                                        group_col,
                                        ref_col,
                                        agg_col):
  '''
  Collects total donor gift amounts by ReportPeriod and calculates the difference from
    period to period.
  Arguments:
  - df: a Pandas dataframe
  - group_col: a list of strings representing the columns to group by, 
      in order of precedence
  - ref_col: a string representing the column name to be aggregated (i.e. summed)
  - agg_col: a string represnting the name to be applied to the aggregated column
  Returns:
  - the original Pandas dataframe including the aggregated column and
      a diff column containing the row wise difference of aggregates, 
      i.e. the change from row to row
  '''
  # group by Donor and Report Period, sum GiftAmount totals
  df = df.sort_values(by=group_col)
  grouped = df.groupby(group_col)[ref_col].sum().to_frame()
  grouped.columns = [agg_col]
  
  # calculate the difference in GiftAmount totals from period to period
  grouped[agg_col+'Diff'] = grouped[agg_col].diff()
  
  # merge back to the un-grouped dataframe
  df = df.merge(grouped, on=group_col, how='left')
  del grouped  
  return df


def fill_diff_col(df, feature_col, ref_col, diff_col):
  '''
  Helper function to fill NAs with 0 when the previously applied diff
    calc yields NA because there was no preceding row, 
    i.e. the feature_col and the ref_col are the same
  Arguments:
  - df: a Pandas dataframe
  - feature_col: a string representing the name of the ReportPeriod year column
  - ref_col: a string representing the name of the first report period column
      i.e. the column storing the value of the first ReportPeriod year
  - diff_col: a string represeting the name of the column containing the 
      row wise difference between aggregated gift totals
  Returns:
  - the transformed diff_col vector, with NAs -> 0 when applicable
  '''
  df[diff_col] = df[diff_col].fillna(0)
  boolean_mask = df[feature_col] == df[ref_col]
  df[diff_col] = np.where(boolean_mask, 0, df[diff_col])
  return df[diff_col]


def make_feature_upgrade_downgrade(df, group_col, ref_col, agg_col, diff_col):
  '''
  Wrapper around the upgrade / downgrade component functions.
  Arguments:
  - df: a Pandas dataframe
  - group_col: a list of strings representing the columns to group by, 
      in order of precedence
  - ref_col: a string representing the column name to be aggregated (i.e. summed)
  - agg_col: a string represnting the name to be applied to the aggregated gift amount column
  - diff_col: a string represeting the name of the column containing the 
      row wise difference between aggregated gift totals
  Returns:
  - the original dataframe, transformed to include the upgrade / downgrade column and 
      various helper columns
  '''
  feature='Upgrade_Downgrade'
  df['in_range'] = filter_years(df, 'GiftDate', 'calcDate', 'EndofRptPeriod')
  df = prepare_df_for_upgrade_downgrade(df, group_col, ref_col, agg_col)  
  df[diff_col] = fill_diff_col(df, 'ReportPeriod_YR', 'FirstReportPeriod', diff_col)
  df = add_years(df, "ReportPeriodReference", "AllReportPeriodYrs")  
  df[feature] = apply_upgrade_downgrade_labels(df, diff_col)  
  df[feature] = collect_labels(df, feature, ['DonorID', 'ReportPeriod'])
  return df
  
  
def prepare_df_for_upgrade_downgrade(df, group_col, ref_col, agg_col):
  '''
  Prepares the dataframe so that upgrade / downgrade logic can be applied.
    See indiviual function docstrings for details
  Arguments:
  - df: a Pandas dataframe
  - group_col: a list of strings representing the columns to group by, 
      in order of precedence
  - ref_col: a string representing the column name to be aggregated (i.e. summed)
  - agg_col: a string represnting the name to be applied to the aggregated gift amount column
  Returns:
  - the original dataframe, preprocessed and ready for upgrade / downgrade logic to be applied
  '''
  df = get_earliest_report_period_by_donor(df, 'DonorID', 'ReportPeriod_YR', 'FirstReportPeriod')
  df = get_all_report_periods_by_donor(df, 'DonorID', 'ReportPeriod_YR', 'AllReportPeriodYrs')
  df = get_gift_amounts_by_donor_and_period(df,  group_col, ref_col, agg_col)    
  return df
  
  
def collect_labels(df, label_col, group_cols):
  '''
  Sorts the given dataframe and applies the last label to each group
  Arguments:
  - df: a Pandas dataframe
  - label_col: a string representing the column containing the categorical labels
  - group_cols: a list of strings representing the columns to group by, 
      in order of precedence
  Returns:
  - the original dataframe, transformed to have all group_col members share the same label
  '''
  df = df.sort_values(group_cols)
  return df.groupby(group_cols)[label_col].transform('last')


# COMMAND ----------

