# Databricks notebook source
# MAGIC %md # utilities (production functions)

# COMMAND ----------

# This notebook provides commonly used utility functions to be shared throughout the various Mindset Direct client ETL workflows.


# COMMAND ----------

# DBTITLE 1,Mount Datalake
# COMMAND ----------

# DBTITLE 1,Imports
import csv
from datetime import datetime, timedelta #, datetime, timedelta
from functools import reduce
import json
import os
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_datetime64_dtype
from pandas.util import hash_pandas_object
import re
from shutil import copyfile
import time
import numpy as np

# import tqdm
# from tqdm import tqdm


# COMMAND ----------

# DBTITLE 1,Install


# COMMAND ----------

# DBTITLE 1,Encoding
ENCODING = "ISO-8859-1"

# COMMAND ----------

# DBTITLE 1,Functions A-E
# Helper functions A-E

def add_join_detail(df, feature, group_col, agg_col, agg_type, gd="GiftDate", fgd="FirstGiftDate"):
  '''TODO'''
  boolean_mask = df[gd] == df[fgd]
  df[feature] = groupby_and_transform(df, group_col, agg_col, agg_type, filter=boolean_mask)
  return df.fillna({feature: df.groupby(group_col)[feature].transform('first')})


def assert_dates(df, col):
  '''
  Asserts that the most recent file contains dates at least through last month, 
    and extends back at least until the beginning of the year.
  Arguments:
  - df: a Pandas dataframe
  - col: a string representing the column containing dates to inspect.
  Returns:
  - the validated dataframe
  '''
  now = datetime.now()
  last_month = int(str(now.year) + str(now.month -1).zfill(2)) * 100
  this_year = int(str(now.year).zfill(4) + "0101")
  try:
    assert df[col].max() >= last_month
    assert df[col].min() <= this_year
  except AssertionError as e:
    raise e
  return df
    

def assert_declarations(dataframe, type_dict, inner_key=None, view=False):
  '''
  Compares a given dataframe to a declaration manifest, prunes unexpected columns
    and casts / asserts correct data types.
  Arguments:
  - dataframe: a Pandas dataframe
  - type_dict: a dictionary of column : data type key value pairs
  - inner_key: a key representing the inner json object where the dtype data is
      stored, for instance in the raw declaration files
  Returns:
  - the Pandas dataframe of the correct shape and dtypes
  '''
  if inner_key:
    ref_dtypes = pd.Series(data = type_dict[inner_key])
  else:
    ref_dtypes = pd.Series(data = type_dict) 

  for col in dataframe.columns:
    if col in ref_dtypes:
      print('\t' + col)
      dataframe[col] = dataframe[col].astype(ref_dtypes[col])
    else:
      print("%s found in dataframe but not in reference declarations" %col)
      dataframe = dataframe.drop(col, axis=1)
      
  dataframe = dataframe.sort_index(axis=1)  
  ref_dtypes = ref_dtypes.sort_index()
  if view:
    print('-'*50)
    print('df columns: ', dataframe.columns)
    print('reference dtypes: ', ref_dtypes)
    print('-'*50)
  
  try:
    assert (dataframe.dtypes == ref_dtypes).all()
  except AssertionError as e:
    raise e
    
  return dataframe


def assert_existence_collected(filemap, schema):
  '''
  Checks that the files in the supplied Raw directory match
    those identified in the schema, and raises an error on 
    unexpected files.
  Arguments:
  -  filemap: a ./mount_datalake/Filemap object containing storage path details
  -  schema:  a dict containing the reference informaton from schema.json
  Returns:
  - True if the files in the Raw directory match those in the schema,
    False otherwise
  '''
  files = []
  schema_keys = [k.split('.')[0] for k in schema.keys()]
  for file_name in os.listdir(filemap.RAW):
    if os.path.isfile(filemap.RAW + file_name):
      try:
        file_name = file_name.split('.')[0]
        assert file_name in schema_keys
        files.append(file_name)      
      except AssertionError as e:
        print(file_name)
        raise e
  return sorted(files) == sorted(schema_keys)


def cast_all_dates(df, kw='Date'):
  '''
  Casts all columns containing the keyword kw in the column name
    to datetime.
  Arguments:
  - df: a Pandas dataframe
  - kw (default = 'Date'): a string representing the date keyword
      expected in the column names.
  Returns:
  - the Pandas df, with datetime data types where appropriate
  '''
  try:
    for col in df.columns:
      if kw in col:
        df[col] = pd.to_datetime(df[col])
    return df
  except Exception as e:
    raise e
    

def cast_columns(df, type_map):
  '''
  Casts the each column to the data type specified:
  Arguments:
  -  df: a Pandas dataframe
  -  type_map: a dictionary of the form
          {'column name': 'data type'}
  Returns:
  -  The dataframe with correct column dtypes
  '''
  for col in df.columns:
    if col in type_map.keys():
      if type_map[col] == 'datetimeYMD':
        df[col] = pd.to_datetime(df[col], format = "%Y%m%d")
      elif type_map[col] == 'datetimeY-M-D':
        df[col] = pd.to_datetime(df[col], format = "%Y-%m-%d")
      elif type_map[col] == 'date':
        df[col] = pd.to_datetime(df[col]).dt.date
      elif type_map[col] == 'datetime':
        df[col] = pd.to_datetime(df[col])
      else:
        df[col] = df[col].astype(type_map[col], errors="ignore")
    else:
      df[col] = df[col].astype('str')
  return df


def cast_given_dates(df, date_cols):  
  '''
  Casts a given list of dates as datetime objects.
  Arguments:
  - df: a Pandas dataframe
  - date_cols: a list of strings representing columns to be cast
  Returns:
  - df: a Pandas dataframe where the specified columns are datetimes
  '''
  for dc in date_cols:
    try:
      df[dc] = pd.to_datetime(df[dc])
    except Exception as e:
      print("Unable to cast %s as datetime" %dc)
  return df


def check_col(col, pattern):
  r = re.compile(pattern)
  regmatch = np.vectorize(lambda x: bool(r.match(x)))
  return regmatch(col)


def check_hashes(dataframe, directory, filename):
  '''
  Generates a row-wise hash vector for use in evaluating whether or not a 
    dataframe has changed from one iteration to the next.  Especially useful
    when testing new code application wide, but where updating the SQL database
    can be avoided for efficiency.
  Arguments:
  - dataframe: a Pandas dataframe
  - directory: a string representing the path where the hash vector is stored
  - filename: a string representing the name of the file containing the hash vector
  Returns:
  - previous_hash: a Pandas series containing the hash vector from a previous execution,
      or None.
  - current_hash: a Pandas series containing the hash vector from the current execution,
      or None.
  - hashes_agree: a Boolean, True if the current and previous hashes agree,
      False otherwise.
  '''
  current_hash = None
  previous_hash = None
  try:
    current_hash = hash_pandas_object(dataframe).reset_index(drop=True)
    if filename in os.listdir(directory):
      print('hash file found in directory')
      previous_hash = pd.read_csv(directory + filename).squeeze()
    else:
      print('hash file not found in directory!')
    hashes_agree = True if (current_hash == previous_hash).all() else False
  except Exception as e:
    print('exception: ', e)
    hashes_agree = False
  return previous_hash, current_hash, hashes_agree


def check_kw_in_col(df, kw_list, ref_col):
  '''
  Checks to see if any of a number of keywords are present in
    the specified column.
  Arguments:
  - df: a Pandas dataframe
  - kw_list: a list of keywords
  - ref_col: a string representing the name of the column to check 
      for the presence of keywords
  Returns:
  - a boolean vector
      True if any of the keywords is present in the reference column 
      False otherwise
  '''
  return [any([y in str(x) for y in kw_list]) for x in df[ref_col].values.tolist()]


def check_online_or_dm(df, feature, ref_col, ctrl_col, lab_a="Online", lab_b="DM"):
  '''
  Checks a reference column for any form of the word "ONLINE" and 
    assigns the label <lab_a> or <lab_b> accordingly.
  Arguments:
  - df: a Pandas dataframe
  - feature: a string representing the column to create to hold the label assignment
  - ref_col: a string representing the reference column to check for the keyword "ONLINE"
  - ctrl_col: a string representing a column to check for nulls
  - lab_a (default: "Online"): a string representing the desired label for online entries
  - lab_b (default: "DM"): a string representing the desired label for direct mail entries
  Returns:
  - a column named <feature> containing either of the given labels, or 'Other'
  '''
  cols = [lab_a, lab_b]
  df[lab_a] = df[ref_col].str.upper() == "ONLINE"
  df[lab_b] = df[ctrl_col].notna() & ~df[lab_a]
  
  df[feature] = labels_from_bool_cols(df, cols, feature)
  df[feature] = np.where(df[feature] == 'no_label', 'Other', df[feature])
  
  return df.drop(cols, axis=1)


def check_set_integrity(df_list, df, split_col='DonorID', check=False):
  '''
  Method to confirm that no rows are omitted while splitting up the dataframe 
    for improved processing efficiency.
  Arguments:
  - df_list: a list of Pandas dataframes split from a larger dataset
  - df: a Pandas dataframe representing the complete dataset
  - split_col (default='DonorID'): a string representing the name of the 
      column on which the larger dataset was split
  - check (default=False): a flag to control printing of the output to the console
  Returns:
  - a Boolean, True when the number of unique members of the split_col set is equal
      in both df and the aggregated df_list,
      False otherwise
  '''
  subset_ids = []
  for _df in df_list:
    subset_ids.extend(list(set(_df[split_col])))  
    
  sets_agree = len(set(df[split_col])) == len(subset_ids)
  
  if check:
    print('length of subset_ids: ', len(subset_ids))
    print('length of original ids: ', len(set(df[split_col])))
    print('sets agree: ', sets_agree)
    
  return sets_agree


def check_string_chars(df, col, idx, chars):
  '''
  Checks to see if the nth position in a string is a specified character.
  Arguments:
  - df: a Pandas dataframe
  - col: a string representing the name of the column to inspect
  - idx: an int representing the position of the character in the string
      to check
  - chars: a string representing a single character
  Returns:
  - a Boolean vector, True if the string value at position idx is the character
      passed as chars,
      False otherwise
  '''
  return df[col].str[idx] == chars


def check_string_length(df, col, length):
  '''
  Helper function for the common task of checking 
  the length of an element in a column of strings.
  Arguments:
  -  df: a Pandas dataframe
  -  col: a string representing the name of the column to check
  -  length: an int representing the length to check
  Returns:
  -  a boolean array, True where the length of the string is
      equal to the lenght specified, False otherwiese
  '''
  return df[col].str.len() == length


def check_string_firstchars(df, col, firstchars):
  '''
  Helper function for the common task of checking 
  if the first character of an element in a column of strings
  is present in the specified list of characters.
  Arguments:
  -  df: a Pandas dataframe
  -  col: a string representing the name of the column to check
  -  firstchars: a list of characters to match
  Returns:
  -  a boolean array, True where the first character of the 
      string is present in the list, False otherwiese
  '''
  return df[col].str[0].isin(firstchars)


def check_month_threshold(df, col, month_number):
  '''
  Helper function for the common task of checking 
  if the the month of a date is before or after a given 
  threshold, i.e. for use in determining fiscal year.
  Arguments:
  -  df: a Pandas dataframe
  -  col: a string representing the name of the column to check
  -  month_number: an int representing the threshold to check
  Returns:
  -  a boolean array, True where the month is greater than or
      equal to the month specified, False otherwiese
  '''
  return df[col].dt.month >= month_number

def client_schema_comparison(schema_field):
  for client in client_list():
      try:
          schema = get_schema(client)
          print(client)
          print(schema[schema_field])
      except Exception as e:
          print(f"Error processing {client}: {e}")
          
def coerce_and_cast(df, col='DonorID'):  
  '''
  TODO
  '''
  df[col] = pd.to_numeric(df[col], errors='coerce')
  df = df.dropna(subset=[col])
  return df[col].astype(int)


def collect_constituents(file_list, filemap, dir, dfs):
  '''
  Collects the data from multiple .csv files into a list of dataframes.
  Arguments:
  - file_list: a list of file names in the current working directory
  Returns:
  - a list of dataframes, each containing the data from one file.
  '''
  for filename in file_list: 
    try:
      if '.csv' in filename:
        df = pd.read_csv(filemap.RAW + dir + filename)  
      elif '.xlsx' in filename:
        df = pd.read_excel(filemap.RAW + dir + filename) 
      dfs.append(df)
    except Exception as e:
      raise e
  return dfs




def get_differences_split(df1, df2, join_key='GiftID', ignore_columns=None):
    """
    Compare two DataFrames and return only differences.
    Matching values are set to NaN. Fully matching rows are dropped.

    Returns:
    - df1_only: GiftIDs only in df1
    - df1_changed: GiftIDs in both but values differ (from df1)
    - df2_only: GiftIDs only in df2
    - df2_changed: GiftIDs in both but values differ (from df2)
    """
    ignore_columns = set(ignore_columns or [])

    # Drop ignored columns
    df1_clean = df1.drop(columns=ignore_columns, errors='ignore')
    df2_clean = df2.drop(columns=ignore_columns, errors='ignore')

    # Set index
    df1_keyed = df1_clean.set_index(join_key)
    df2_keyed = df2_clean.set_index(join_key)

    # Union of all keys and columns
    all_keys = df1_keyed.index.union(df2_keyed.index)
    all_cols = df1_keyed.columns.union(df2_keyed.columns)

    # Reindex to ensure consistent alignment
    df1_aligned = df1_keyed.reindex(index=all_keys, columns=all_cols)
    df2_aligned = df2_keyed.reindex(index=all_keys, columns=all_cols)

    # Find cell-level matches
    match_mask = (df1_aligned == df2_aligned) | (df1_aligned.isna() & df2_aligned.isna())
    df1_diff = df1_aligned.mask(match_mask)
    df2_diff = df2_aligned.mask(match_mask)

    # Restore join_key as column
    df1_diff = df1_diff.reset_index()
    df2_diff = df2_diff.reset_index()

    # Drop rows with no differences
    df1_diff = df1_diff.dropna(how='all', subset=[col for col in df1_diff.columns if col != join_key])
    df2_diff = df2_diff.dropna(how='all', subset=[col for col in df2_diff.columns if col != join_key])

    # Split into "only in df1" and "changed vs df2"
    df2_ids = set(df2[join_key])
    df1_ids = set(df1[join_key])

    df1_only = df1_diff[~df1_diff[join_key].isin(df2_ids)].copy()
    df1_changed = df1_diff[df1_diff[join_key].isin(df2_ids)].copy()

    df2_only = df2_diff[~df2_diff[join_key].isin(df1_ids)].copy()
    df2_changed = df2_diff[df2_diff[join_key].isin(df1_ids)].copy()

    return df1_only, df1_changed, df2_only, df2_changed

def drop_future_dates(df, col):
  '''
  Inspects data for unexpected (future) dates, drops these if found, and logs the event.
  Arguments:
  - df: a Pandas dataframe
  - col: a string representing the date to be inpsected
  Returns:
  - the filtered dataset 
  '''  
  if is_numeric_dtype(df[col]):
    now = datetime.now()
    year = str(now.year).zfill(4)
    month = str(now.month).zfill(2)
    day = str(now.day).zfill(2)
    today = int(year + month + day)
    today = int(year + month + day)
  elif is_datetime64_dtype(df[col]):
    today = pd.to_datetime('today')
  else:
    print("drop_future_dates(): Unexpected dtype in reference column")
    return df
  if (df[col] > today).any():
    print("Dropping unexpected (future) dates from dataset")
  return df[df[col] <= today]


def drop_null_columns(df):
    df = df.dropna(axis=1, how='all')
    return df


def drop_with_pattern(df, kw):
  '''
  Helper function for use after combining dataframes with common 
  column headers.  Pandas will append a string to the column names
  to differentiate between the columns from table A or table B.  
  This function removes the unwanted duplicate columns.
  Arguments:
  -  df: a Pandas dataframe
  -  kw: a string, representing the label appended to those columns
      that should be removed.
  Returns:
  -  the pruned Pandas dataframe
  '''
  # e.g. kw = "_ref"
  cols_to_drop = []
  for col in df.columns:
      if kw in col:
          cols_to_drop.append(col)
  return df.drop(cols_to_drop, axis=1)


def expand_id_list(df, col, char=";"):
  '''
  Takes the first value of each list in a column of lists.
  Arguments:
  - df: a Pandas dataframe
  - col: a string representing the name of a column
  - char (default=";"): a string representing the list delimiter
  Returns:
  - the transformed column
  '''
  df[col] = [str(x).split(char) if char in str(x) else x for x in df[col].values.tolist()]
  df[col] = [x[0] if isinstance(x, list) else x for x in df[col].values.tolist()]
  return df[col]


def expand_rfm_d(rfm_d):
  '''
  TODO
  '''
  keys = sorted(list(rfm_d.keys()))[::-1]
  d = {}
  mrk = 0
  for i in range(keys[0], 0, -1):
    if i in keys:
      d[i] = rfm_d[i]
      mrk = i
    else:
      d[i] = rfm_d[mrk]
  return d

# COMMAND ----------

# DBTITLE 1,Functions F-Z
# Helper functions F-Z


def filter_cols_condition_not(df, col, values):
  '''
  Helper function to prune rows from a dataframe when the 
  given condition is not met, in this case when the value of
  the element in the column specified is not contained in 
  the specified values.
  Arguments: 
  -  df: a Pandas dataframe
  -  col: a string representing the column to check
  -  values: a list of values to compare to the column elements
  Returns:
  -  the pruned Pandas dataframe
  '''
  rules = []
  for i in range(len(values)):
    rules.append(df[col] != values[i])
  combined = reduce(lambda x, y: x & y, rules)
  return df[combined]


def find_gift_date_gaps(df, date_col='GiftDate'):
    # Ensure GiftDate is a datetime type and sort
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(by=date_col).reset_index(drop=True)

    # Calculate the difference between consecutive dates
    df['PrevDate'] = df[date_col].shift(1)
    df['GapDays'] = (df[date_col] - df['PrevDate']).dt.days

    # Filter for gaps greater than 2 days
    gap_df = df[df['GapDays'] > 2].copy()

    # Sort by largest gap
    gap_df = gap_df.sort_values(by='GapDays', ascending=False)

    return gap_df[[date_col, 'PrevDate', 'GapDays']]


def fiscal_from_column(df, col, month_number):
  '''
  Determines the fiscal year of the elements of the specified column,
  given the first month of the client's fiscal year.
  Arguments:
  -  df: a Pandas dataframe
  -  col: a string representing the column to check
  -  month_number: an int representing the first month of the 
      client's fiscal year
  Returns:
  -  a vector of fiscal year values:
      year + 1 if the month is greater than that specified,
      otherwise, the given year
  '''
  # added if/else 8/3/2022, had been just code in the if block
  if month_number != 1:
    boolean_mask = check_month_threshold(df, col, month_number)
    return np.where(boolean_mask, ((df[col] + pd.offsets.DateOffset(years=1)).dt.year).astype('Int64'), (df[col].dt.year).astype('Int64'))
  else:
    return (df[col].dt.year).astype('Int64')
  
# changes to this function should be made to the parallel sql server function (dbo.fiscal_from_date) as well:
def fiscal_from_date(date, fym):
  '''TODO'''
  if fym == 1:
    return date.year
  elif date.month < fym:
    return date.year
  else:
    return date.year + 1


def flatten_schema(d, kw='pandas'):
  '''
  TODO
  '''
  _dtypes = {}
  for k,v in d.items():
    _dtypes[k] = v[kw]
  return _dtypes


def float_to_int(x):
  '''
  TODO
  '''
  if type(x) == float:
    return str(int(x))
  elif type(x) == int:
    return str(x)
  else:
    return (x)
  
  
def float_to_int_try(x):
  '''
  TODO
  '''
  if type(x) == float:
    return str(int(x))
  elif type(x) == int:
    return str(x)
  else:
    try:
      return(str(int(float(x))))
    except:
      return (x)


# def format_currency(vector, cur='$', delim=','):
#   '''
#   TODO
#   '''
#   vector = vector.astype(str)#.str.replace('None','0') <- 5/24/2023 temp to fix DAV package file CPP issue
#   vector = vector.str.replace(cur,'')
#   vector = vector.str.replace(delim,'')
#   return vector.astype(float)
# #   return vector.astype('Float64')

def format_currency(vector, cur='$', delim=','): #Updated 6/11/25
    vector = vector.astype(str).str.strip()
    vector = vector.str.replace(cur, '', regex=False)
    vector = vector.str.replace(delim, '', regex=False)
    vector = vector.replace(['-', ' - ', 'None', '', 'nan'], 0)
    return vector.astype(float)
  
def generate_budget_key_column(df, new_col_name, required_cols, optional_cols):
    """
    Generate a composite budget key column from required and optional columns.
    The resulting key is always uppercase and replaces NaN/None values with 'NULL'.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe to modify.
    new_col_name : str
        The name of the column to create (e.g., 'Budg_Client_CampCode').
    required_cols : list[str]
        Columns that must exist to build the key (e.g., ['Client', 'Budg_CampaignCode']).
    optional_cols : list[str]
        Columns that can optionally be appended (e.g., ['Budg_Program']).

    Returns
    -------
    pandas.DataFrame
        The modified dataframe with the new column added.
    """

    # --- Check required columns ---
    missing_required = [col for col in required_cols if col not in df.columns]
    if missing_required:
        print(f"⚠️ Missing required columns: {missing_required}")
        return df

    # Helper to clean and normalize values
    def clean_series(series):
        return (
            series.astype(str)
            .str.strip()
            .replace({"nan": "NULL", "None": "NULL", "NaT": "NULL"}, regex=False)
        )

    # --- Start with required components ---
    base = clean_series(df[required_cols[0]])
    for col in required_cols[1:]:
        base = base + "_" + clean_series(df[col])

    # --- Add optional columns (with logging for missing ones) ---
    missing_optional = []
    for col in optional_cols:
        if col in df.columns:
            base = base + "_" + clean_series(df[col])
        else:
            base = base + "_NULL"
            missing_optional.append(col)

    # --- Assign uppercase key to DataFrame ---
    df[new_col_name] = base.str.upper()

    # --- Logging ---
    if missing_optional:
        print(f"⚠️ Optional columns missing for {new_col_name}: {missing_optional}")
    print(f"✅ Added {new_col_name} column (with optional extensions) to {len(df):,} rows.")

    return df

def generate_FS_gifts(
    df,
    giftid_col="GiftID",
    donorid_col="DonorID",
    fiscal_col="GiftFiscal",
    base_rate=0.02,
    salt="FS_V1"):

    print("Starting FS gift generation")
    print("Original row count:", len(df))

    df = df.copy()

    # Remove already synthetic gifts if present
    base = df[~df[giftid_col].str.endswith("_FS", na=False)].copy()
    removed = len(df) - len(base)

    print("Existing synthetic gifts removed:", removed)
    print("Base row count:", len(base))

    max_fy = base[fiscal_col].max()
    start_year = max_fy - 5

    print("Max fiscal year:", max_fy)
    print("Start year for FS ramp:", start_year)

    # Calculate rate
    base["fs_rate"] = base[fiscal_col].apply(
        lambda fy: base_rate + max(0, fy - start_year + 1) * 0.03
    )

    print("Sample FS rates:")
    print(base[[fiscal_col, "fs_rate"]].drop_duplicates().sort_values(fiscal_col).tail(10))

    # Deterministic hash score (vectorized)
    hash_series = pd.util.hash_pandas_object(
        base[[giftid_col, fiscal_col]].astype(str) + salt
    )

    base["fs_score"] = (hash_series % 1_000_000) / 1_000_000

    print("Hash scores calculated")

    # Select rows to duplicate
    selected = base[base["fs_score"] < base["fs_rate"]].copy()

    print("Rows selected for synthetic duplication:", len(selected))

    synth = selected.copy()

    # Append FS identifiers
    synth[giftid_col] = synth[giftid_col].astype(str) + "_FS"
    synth[donorid_col] = synth[donorid_col].astype(str) + "_FS"

    synth["is_synthetic_fs"] = True
    base["is_synthetic_fs"] = False

    # Remove helper columns
    synth = synth.drop(columns=["fs_rate", "fs_score"])
    base = base.drop(columns=["fs_rate", "fs_score"])

    result = pd.concat([base, synth], ignore_index=True)

    print("Synthetic gifts created:", len(synth))
    print("Final row count:", len(result))
    print("Finished FS generation")

    return result


def generate_sets_to_split(df, n, split_col='DonorID', check_len=False):
  '''
  Splits the df[split_col] vector into n equally sized sets.
  Arguments:
  - df: a Pandas dataframe
  - n: an int representing the desired number of sets to create
  - split_col (default='DonorID'): a string representing the name of the column to split
  - check_len (defualt=False): a flag to control printing of the output to the console
  Returns:
  - a list of n equally sized numpy arrays containing the elements of df[split_col], 
      if the sum total of the values in n sets equals the number of distinct members 
      of df[split_col]
  - None otherwise
  '''
  col_set = np.array(list(set(df[split_col])))
  split_col_sets = np.array_split(col_set, n)
  
  sum = 0
  for i in range(len(split_col_sets)):
    sum += len(split_col_sets[i]) 
  agreement = sum == len(set(df[split_col]))
  
  if check_len:
    print('lengths agree: ', agreement)
    
  if agreement:
    return split_col_sets
  return None


def get_context(process):
  '''
  Extracts the client and dataset context from the process string generated Databricks.
  Arguments:
  - process: a string representing the path of the current script, as returned by the call to:
      dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
      e.g. '/Shared/TSE/Curated/PromoHistory/'
  Returns:
  - client: a string representing the client abbreviation
  - dataset: a string representing the applicable file in ADLS2 and table in the SQL db
  '''
  process_list = process.split('/')
  client = process_list[2]
  dataset = process_list[-1]  
  return client, dataset


def get_consecutive(l, t, p, r):
  if (p == 'fytd') & (r):
    l = [x-1 for x in l]
  if len(l) == 0:
    return ''
  l = sorted([x for x in set(l) if x < t])[::-1]
  if len(l) == 0:
    return ''
  if l[0] != t-1:
    return ''  
  c = []
  for i in range(len(l)):
    if i < len(l)-1:
      if l[i] - l[i+1] == 1:
        c.extend([l[i], l[i+1]])
      else:
        return str(len(set(c))) 
  ret = str(len(set(c)))
  return ret if ret != '0' else ''


def get_consecutive_dav(l, t, p):
#   if p == 'fytd':
#     l = [x-1 for x in l]
  if len(l) == 0:
    return ''
  l = sorted([x for x in set(l) if x < t])[::-1]
  if len(l) == 0:
    return ''
  if l[0] != t-1:
    return ''  
  c = []
  for i in range(len(l)):
    if i < len(l)-1:
      if l[i] - l[i+1] == 1:
        c.extend([l[i], l[i+1]])
      else:
        return str(len(set(c))) 
  ret = str(len(set(c)))
  return ret if ret != '0' else ''


def get_dtypes(type_map, client, dataset, isolate=False): 
  '''
  Returns the dictionary of columns to dtypes for all_clients + specific client.
  Arguments:
  - type_map: a dictionary containing column names and data types,
      e.g. curated_declarations or raw_declarations from /shared/python_modules
  - client: a string representing the client name, e.g. "TSE"
  - dataset: a string representing the table or file being curated
      e.g. "Data" or "PromoHistory"
  Returns:
  - a dictionary containing the complete column to data type mapping, or None
  '''
  if isolate:
    return type_map[client][dataset]
  _in_all = dataset in type_map['all_clients']
  _in_this = dataset in type_map[client]
  if (_in_all and _in_this):
    return {**type_map["all_clients"][dataset], **type_map[client][dataset]}
  elif _in_all:
    return type_map["all_clients"][dataset]
  elif _in_this:
    return type_map[client][dataset]
  else:
    return None
  
  
def get_fh_filters(fh_keys):
  '''
  TODO
  '''
  DEFAULT = [
    'DonorID', 'EndofRptPeriod', 'FHGroup', 
    'FHGroupDetail', 'GiftAmount', 'GiftDate',
    'GiftFiscal', 'Period', 'ReportPeriod',
    'calcDate'
  ]
  return [x for x in fh_keys.keys() if x not in DEFAULT]


def get_lapsed(l, t):
  l = sorted([x for x in set(l) if x < t])[::-1]
  if len(l) < 1:
    return ''
  return str(t - l[0])


def get_reinstated(l, t, p, r):
  if (p == 'fytd') & (r):
    l = [x-1 for x in l]
  if len(l) < 2:
    return ''
  l = sorted([x for x in set(l) if x < t])[::-1]
  if len(l) < 2:
    return ''
  if l[0] != t-1:
    return ''  
  if l[0] == l[1] + 1:
    return ''
  return str(l[0] - l[1])


def get_reinstated_dav(l, t, p):
  if p != 'fy':
    l = [x-1 for x in l]
  if len(l) < 2:
    return ''
  l = sorted([x for x in set(l) if x < t])[::-1]
  if len(l) < 2:
    return ''
  if l[0] != t-1:
    return ''  
  if l[0] == l[1] + 1:
    return ''
  return str(l[0] - l[1])
  
  
def get_schema_details(path, filename='schema.json'):
  '''
  Retrieves the client schema details from the path specified,
    typically ADLS2.
    Assumes the schema file is in json format.
  Arguments:
  - path: a string specifying a file location
  - filename (default='schema.json'): a string representing the schema file name
  Returns:
  - a dictionary representation of the data in the json file
  '''
  with open(os.path.join(path, filename)) as f:
    d = json.load(f)
  return d


def get_unique_and_null(df, col):
  '''
  Drops duplicates in a specified column while preserving Nans.
  Arguments:
  - df: a Pandas dataframe
  - col: a string representing the name of the column to be used to 
      drop duplicates
  Returns:
  - the modified dataframe
  '''
  return df[(~df[col].duplicated()) | (df[col].isnull())]
  

def get_unique_from_spark_col(df, col):
  return df.select(F.collect_set(col).alias(col)).first()[col]
  
  
def get_zip_components(zip_row, ret):
  '''
  Separates a 9-digit zip code into the first 5-digit part and the second 4-digit part.
  Arguments:
  - zip_row: the row of a Pandas dataframe (implied with df[zip_row].apply())
  - ret: a string representing the zip code component to return.  
      Options:  'first' or 'last'
  Returns:
  - a integer corresponding to the component of the zip code identified by 'ret'.
    i.e, if ret == 'first', the first 5 digits are returned (as an int).
         if ret == 'last', the last 4 digits are returned (as an int).
  '''
  if '-' in str(zip_row):
    zip = str(zip_row).split('-')
    if ret == 'first':
      return zip[0]
    elif ret == 'last':
      return zip[1]
  else:
    return zip_row
  
  
def groupby_and_fillna(df, groupby_col, target_col):
  '''
  Helper function to fill NAs with an aggregated group value, when appropriate.
  Arguments:
  -  df: a Pandas dataframe
  -  groupby_col: a string representing the column to be used in the call to groupby
  -  target_col: a string representing the column to be filled with the group value
  Returns:
  -  The aggregated column containing the group value.
  '''
  grouped = df.groupby(groupby_col)  
  df[target_col] = grouped[target_col].transform(lambda x: x.loc[x.first_valid_index()] if x.any() else None)
  del grouped  
  return df[target_col]


def groupby_and_transform(df, groupby_col, target_col, agg_type, filter = None):
  '''
  Helper function to simplify a common pattern found involving groupby and aggregate.
  Arguments:
  -  df: a Pandas dataframe
  -  groupby_col: a string representing the column to be used in the call to groupby
  -  target_col: the column over which to perform the aggregation
  -  agg_type: a string representing the aggregation type, i.e. "min", "max", etc
  -  filter: a boolean array: Optional, if specified will pre-condition 
      the dataframe before perfoming the groupby and aggregation.
  Returns:
  -  The aggregated value
  '''
  try:
    if not filter:
      return df.groupby(groupby_col)[target_col].transform(agg_type)
  except ValueError:
    if filter.any():
      return df[filter].groupby(groupby_col)[target_col].transform(agg_type)
  return None


# def initiate_client():  # Commented out 8/31/2023 to reflect removal of this code everywhere
#   ''' TODO '''

#   # establish context
#   process = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
#   client, dataset = get_context(process) 
#   dataset = 'CampPerf_Measures'
#   print('client: %s, dataset: %s' %(client, dataset))

#   # establish file storage directories
#   filemap = Filemap(client)

#   # establish context
#   logfile_name, logfile_loc = copy_logfile(filemap.LOGS)

#   # instantiate logger
#   logger = create_logger(logfile_loc)

#   #instantiate a Runner object
#   runner = Runner(process, logfile_loc, logfile_name, filemap.LOGS)

#   logger.critical("%s - logging for notebook: %s" % (str(datetime.now()), process))
#   update_adls(logfile_loc, filemap.LOGS, logfile_name)
  
#   return process, client, dataset, filemap, logger, runner


def labels_from_bool_cols(df, cols, new_col_name):
  '''
  Creates a column of labels given a dataframe with a number of boolean
    columns, and where the labels to be applied is the name of the boolean
    column == True for that row.  If no column is True for a given row, 
    the string 'no_label' is applied.
  Arguments: 
  - df: a Pandas dataframe
  - cols: a list of strings representing the set of columns to be used
      for evaluating the correct label to be assigned.
  - new_col_name: a string representing the name of the new column to be 
      appended to the original dataframe to hold the labels.
  Returns:
  - the original Pandas dataframe with an additional column (named the 
      value of new_col_name) holding the label for each row.
  '''
  temp = df[cols]
  temp['no_label'] = temp.sum(axis=1)
  temp['no_label'] = temp['no_label'] == 0
  for c in temp.columns:
    temp[c] = temp[c].astype(int)
  temp[new_col_name] = temp.idxmax(axis=1)
  df[new_col_name] = temp[new_col_name]
  del temp
  return df[new_col_name]


def make_feature_based_on_idx_pos(df, col, chars, idx, true_label, false_label, length=None):
  '''
  Adds a column of labels to the dataframe based on the presense of characters in a 
    given string at a given position, or the length of the string.
  Arguments:
  - df: a Pandas dataframe
  - col: a string representing the name of the column to inspect
  - chars: a string representing the characters to match 
  - idx: an int representing the position in the string in
      df[col] to compare 
  - true_label: the string that is assigned to the label column when the 
      conditions are met
  - false_label: the string that is assigned to the label column when the 
      conditions are not met
  - length (default=None): an int representing the length to check
  Returns:
  - a vector of labels
  '''
  be_1 = True
  if length:
    be_1 = check_string_length(df, col, length)
    
  be_2 = check_string_chars(df, col, idx, chars)
  boolean_mask = (be_1) & (be_2)
  
  return np.where(boolean_mask, true_label, false_label)


def month_name_from_column(df, col):
  '''
  Helper function for the common task of extracting the month 
  from a given column of dates.
  Arguments:
  -  df: a Pandas dataframe
  -  col: a string representing the column to check.
  Returns:
  -  a string representing the name of the month
  '''
  try:
    df[col] = pd.to_datetime(df[col])
  except:
    pass
  return df[col].dt.month#_name()

def show_nunique_and_missing(df):
    # Calculate the number of unique values for each column
    nunique_values = df.nunique()
    # Calculate the number of missing values for each column
    missing_values = df.isnull().sum()
    #Calculate unique + null values
    unique_null_sum = nunique_values + missing_values
    #Calculate non-null values
    non_null_sum = df.notnull().sum()
    # Get the total number of rows in the DataFrame
    total_rows = len(df)

    # Combine both series into a single DataFrame
    summary_df = pd.DataFrame({
        'unique': nunique_values,
        'null': missing_values,
        'unique + null': unique_null_sum,
        'not null': non_null_sum,
        'total': total_rows
    })
    return summary_df


def pop_nested_file(file_list, filename_string):
  '''
  Pops nested files to the Raw directory.  This is useful when files are grouped 
    together in SharePoint, but need to be processed differently.
    E.g. "PromoHistory.csv" is nested in /Matchbacks, but is processed differently.
  Arguments:
  - file_list: a list of files in the specified working directory
  - filename_string: a string representing the file name to pop
  Returns:
  - Nothing
  '''
  for filename in file_list:
    if filename_string in filename:
      file_to_pop = file_list.pop(file_list.index(filename))      
      print("popped file: %s" %filename_string)
      try:
        os.rename(filemap.RAW + dir + file_to_pop, filemap.RAW + file_to_pop)
      except Exception as e:
        raise e

        
def read_file(path, file_name, schema):
  '''
  Attempts to read the file into a Pandas dataframe, logs an error if unsuccessful.
  Assumes data files in either .csv or .xlsx format
  Arguments:
  -  path: a string representing the path to the file directory
  -  file_name: a string representing the file to open
  -  schema: a dictionary of file configuration details
  Returns:
  -  df: a Pandas dataframe containing the client data
  '''
  if 'encoding' in schema[file_name]:
    encoding = schema[file_name]['encoding']
  else:
    encoding = 'utf-8'
    
  try:
    if '.csv' in file_name.lower():
      try:
        df = pd.read_csv(path + file_name, encoding=encoding)
      except:
        df = pd.read_excel(path + file_name)  
    elif '.xlsx' in file_name:
      try:
        df = pd.read_excel(path + file_name)  
      except:
        df = pd.read_csv(path + file_name, encoding=encoding)
    return df
  
  except Exception as e:
    raise e
    
    
def remove_whitespace(df):
  '''
  TODO
  '''
  cols = []
  for c in df.columns:
    cols.append(c.replace(' ', ''))
  df.columns = cols
  return df


def sci_not(x):
  '''
  Prints values in scientific notation to make magnitudes more salient.
  '''
  try:
    [print('{:.3e}'.format(_)) for _ in x]
  except TypeError:
    print('{:.3e}'.format(x))
    
    
def shrink_dec(dec, cols):
  '''
  TODO
  '''
  staged_dec = {}
  for k,v in dec.items():
    if k in cols:
      staged_dec[k] = v
  return staged_dec    
    
    
def split_dataframe_by_sets(df, split_col_sets, n,                             
                            split_col='DonorID',
                            check=False):
  '''
  Splits the dataframe into n sets, each containing the split_col values found 
    in a corresponding member of split_col_sets.
  Arguments:
  - df: a Pandas dataframe
  - split_col_sets: a list of n equally sized numpy arrays containing the 
      elements of df[split_col] 
  - n: an int represensting the number of smaller dataframes to create from
      the original df
  - split_col (default='DonorID'):  a string representing the name of the column to split
  - check: a flag to control printing of the output to the console
  Returns:
  - a list of n dataframes, each a unique section of the original larger dataset,
      if the sum number of rows of the sub dataframes equals the length of the 
      original larger dataset
  - None otherwise
  '''
  _dfs = []
  for i in range(n):
    _dfs.append(
      df[df[split_col].isin(split_col_sets[i])]
    )  
  
  sum = 0
  for i in range(n):
    sum += len(_dfs[i])
  agreement = sum == df.shape[0]
  
  if check:
    print('lengths agree: ', agreement)
  
  if agreement:
    return _dfs
  return None


def split_delimited_fields(df, feature, delimiter):
  '''
  Takes the first element of a string of delimited elements in a dataframe column.
  Arguments:
  - df: a Pandas dataframe
  - feature: a string representing the name of the dataframe column which may 
      contain delimited string values
  - delimiter: a string representing the delimiter character (e.g. ",", ";", etc.)
  Returns:
  - a vector containing only the first element of the delimited string, when such a 
      string exists in the df[feature] vector
  - otherwise, the original value
  '''
  d = delimiter
  _split = [str(x).split(d) if d in str(x) else x for x in df[feature].values.tolist()]
  return [x[0] if isinstance(x, list) else x for x in _split]
    
    
def string_to_camel(string):
  '''
  Converts various delimited string patterns to camel case for
    consistency and readabality, especially for use with column
    headers.
  Arguments:
  - string: the delimited string to be converted
  Returns:
  - the CamelCase string (with the first char camel as well) if
      necessary, otherwise the original string
  '''
  delimiters = [' ', '_', '`', '/']
  
  if string.isupper() and any([x in string for x in delimiters]):
    string = string.lower()
  
  for d in delimiters:  
    if d in string:
      _list = string.split(d)
      return ''.join([x[0].upper() + x[1:] for x in _list])
  return string


def unpack_schema(schema):
  '''
  TODO
  '''
  ancillaryKeywords = schema['Ancillary']
  columns = schema['Columns']
  dataMapper = schema['DataMapper']
  directories = schema['Directories']
  duplicates = schema['DuplicateKeys']
  encoding = schema['Encoding']
  filesToExclude = schema['Exclusions']
  fhKeys = schema['FileHealth']
  keyMapper = schema['KeyMapper']
  fyMonth = schema["firstMonthFiscalYear"]  
  parquet_cols = schema['ParquetCols']
  suppressions = schema['Suppressions']
  return ancillaryKeywords, columns, dataMapper, directories, duplicates, encoding, filesToExclude, fhKeys, keyMapper, fyMonth, parquet_cols, suppressions


def update_id(string):
  '''
  Updates the sub-string 'id' or 'Id' to 'ID'.
  Arguments:
  - string: a string, typically representing a column name
  Returns:
  - the updated string if applicable, 
    the oringal string otherwise.
  '''
  pre = string[:-2]
  suf = string[-2:]
  
  ids = ['id', 'Id']
  for i in ids:
    if i in suf:
      suf = suf.replace(i, 'ID')
      
  return pre + suf


def validate_file(filemap, file_name, schema, view=False, coerce=False, cols=None):
  '''
  Primary staging function:
  -  Reads the data into memory from the /Raw directory
  -  Asserts the data is as expected given the json configuration file
  -  Writes the file (repaired if necessary) to the /Staged Directory
  Arguments:
  -  filemap: a ./mount_datalake/Filemap object containing storage path details
  -  file_name: a string corresponding to the name of the file to read and write
  -  schema: a dictionary containing the file configuration details
  -  coerce: boolean flag to control conversion of critical columns
  '''
  df = read_file(filemap.RAW, file_name, schema)
  # apply CamelCase convention to column headers
  df.columns = [string_to_camel(col) for col in df.columns]
  # make the sub-string 'id' or 'Id' all uppercase
  df.columns = [update_id(col) for col in df.columns]
  
  if coerce:
    for c in cols:
      if c in df.columns:
        df[c] = coerce_and_cast(df, col=c)
      
  # assert columns exist and data types
  df = assert_declarations(df, schema[file_name], inner_key='dtypes', view=view)
  # write the validated file to Staged
  write_file(df, filemap.STAGED, file_name)
  
  
def write_file(dataframe, path, file_name):
  '''
  Writes the validated dataframe to the /Staged directory in Azure Datalake
  Assumes the data is in either .csv or .xlsx formats
  Arguments:
  -  dataframe: a Pandas dataframe that has been validated against given configuration details
  -  path: a string containing the path to the /Staged directory in which the files should be written
  -  file_name: a string containing the name of the file to be written
  '''
  if '.csv' in file_name:
      dataframe.to_csv(path + file_name, index = False)
  elif '.xlsx' in file_name:
      dataframe.to_csv(path + file_name.replace('xlsx', 'csv'), index = False)
  logger.info('%s - %s  wrote %s to %s' % (str(datetime.now()), process, file_name, path))

# COMMAND ----------

# DBTITLE 1,PII Functions
# PII
def write_to_archive(client, client_files, etl=False):
  filemap = Filemap(client)
  if etl:
    path = os.path.join(filemap.RAW, 'RawData')
  else:
    path = filemap.RAW
  files = os.listdir(path)
  for f in files:
    if f in [x.lower().replace('xlsx', 'csv') for x in client_files]:
      if os.path.isfile(os.path.join(path, f)):
        os.rename(
          os.path.join(path, f),
          os.path.join(filemap.ARCHIVE, 'Raw', f)
        )
      print('wrote %s to archive' %f)
  for f in os.listdir(path):
    if f in client_files:
      os.remove(os.path.join(path, f))


        
def _remove_pii(df, pii_cols):
  print("Removing initial PII from pii_cols")
  for col in pii_cols:
    if col in df.columns:
      df = df.drop(col, axis=1)
  print("Initial PII from pii_cols removed")
  return df

        
        
# def remove_pii(client, client_files, etl=False):
#   filemap = Filemap(client)
#   schema = get_schema_details(filemap.SCHEMA)
#   pii_cols = schema['PII_Columns']
#   if etl:
#     path = os.path.join(filemap.RAW, 'Data')
#   else:
#     path = filemap.RAW  
#   files = os.listdir(path)
#   for f in files:
#     if f in client_files:
#       print('f in client files: ', f)
#       if '.csv' in f.lower():
#         df = pd.read_csv(os.path.join(path, f), encoding='ISO-8859-1')
#       elif '.xlsx' in f.lower():
#         df = pd.read_excel(os.path.join(path, f))
#       if pii_cols:
#         df = _remove_pii(df, pii_cols)
#       df.to_csv(os.path.join(path, f.lower().replace('xlsx', 'csv')), index=False)


# def remove_pii(client, client_files, etl=False):
#   filemap = Filemap(client)
#   schema = get_schema_details(filemap.SCHEMA)
#   pii_cols = schema['PII_Columns']
  
#   if etl:
#     path = os.path.join(filemap.RAW, 'RawData')
#   else:
#     path = filemap.RAW

#   files = os.listdir(path)
  
#   for f in files:
#     if f in client_files:
#       print('File in client files:', f)
      
#       try:
#         if '.csv' in f.lower():
#           df = pd.read_csv(os.path.join(path, f), encoding='ISO-8859-1')
#         elif '.xlsx' in f.lower():
#           df = pd.read_excel(os.path.join(path, f))
#         else:
#           print(f"Unsupported file format: {f}")
#           continue
        
#         if pii_cols:
#           df = _remove_pii(df, pii_cols)
        
#         # Determine the output file format based on the original file
#         output_format = 'csv' if '.xlsx' in f.lower() else f.lower().split('.')[-1]
#         output_file = f.lower().replace('xlsx', 'csv')
        
#         df.to_csv(os.path.join(path, output_file), index=False)
            
#       except UnboundLocalError:
#         print(f"Error occurred while processing {f}. Skipping this file.")


def remove_pii(client, client_files, etl=False): #Updated 6/9/25
    filemap = Filemap(client)
    schema = get_schema_details(filemap.SCHEMA)
    pii_cols = schema['PII_Columns']

    if etl:
        path = os.path.join(filemap.RAW, 'RawData')
    else:
        path = filemap.RAW

    files = os.listdir(path)

    for f in files:
        if f in client_files:
            print('File in client files:', f)
            try:
                f_lower = f.lower()
                file_path = os.path.join(path, f)

                if f_lower.endswith('.csv'):
                    df = pd.read_csv(file_path, encoding='ISO-8859-1')
                elif f_lower.endswith('.xlsx'):
                    df = pd.read_excel(file_path)
                else:
                    print(f"Unsupported file format: {f}")
                    continue

                if pii_cols:
                    df = _remove_pii(df, pii_cols)

                # Determine output file name
                if f_lower.endswith('.xlsx'):
                    output_file = f_lower.replace('.xlsx', '.csv')
                    delete_original = True
                else:
                    output_file = f_lower
                    delete_original = False

                output_path = os.path.join(path, output_file)
                df.to_csv(output_path, index=False)

                # Delete the original .xlsx file if converted
                if delete_original:
                    os.remove(file_path)
                    print(f"Deleted original file: {file_path}")

            except Exception as e:
                print(f"Error occurred while processing {f}. Skipping this file. Error: {e}")




# COMMAND ----------

# DBTITLE 1,Testing Functions
def write_df_testing_csv(df, client, filename):
    # Define the directory path for the client
    client_folder_path = f'/dbfs/mnt/msd-datalake/Testing/{client}'
    
    # Check if the directory exists, and create it if it doesn't
    if not os.path.exists(client_folder_path):
        os.makedirs(client_folder_path)
    
    # Write the dataframe to a CSV file in the specified client folder
    df.to_csv(f'{client_folder_path}/{filename}.csv', index=False)

def write_df_testing_parquet(df, client, filename):
    # Define the directory path for the client
    client_folder_path = f'/dbfs/mnt/msd-datalake/Testing/{client}'
    
    # Check if the directory exists, and create it if it doesn't
    if not os.path.exists(client_folder_path):
        os.makedirs(client_folder_path)
    
    # Write the dataframe to a CSV file in the specified client folder
    df.to_parquet(f'{client_folder_path}/{filename}.parquet', index=False)


# COMMAND ----------

# DBTITLE 1,Run Notebook
# 
def run_notebook(nb, timeout, args = {}):
  dbutils.notebook.run(nb, timeout_seconds = timeout, arguments = args)
  desc = 'executed notebook %s' % nb
  print(desc)

# COMMAND ----------

# DBTITLE 1,File Identification
def identify_new_files(ref, df):
  ref_files = set(ref.FileName.unique())
  new_files = set(df.FileName.unique())
  new_files = new_files.intersection(ref_files^new_files)
  return new_files

# COMMAND ----------

# DBTITLE 1,Text Help
def coalesce(*values):
    """Return the first non-None value or None if all values are None"""
    return next((v for v in values if v is not None), None)

# COMMAND ----------

# DBTITLE 1,Dynamic Column Selection
def remove_list_item_if_not_in_cols(list_or_dict, df):
  for i in list(list_or_dict):
    not_present = i not in df.columns
    if not_present:
      if type(list_or_dict)==dict:
        list_or_dict.pop(i)
      else:
        list_or_dict.remove(i)
      print(i)
  return list_or_dict

# COMMAND ----------

# DBTITLE 1,Loading Data
def read_to_dataframe(file_path):
  import pandas as pd
  extension = file_path.split('.')[-1]
  if extension == 'csv':
    df = pd.read_csv(file_path)
  elif extension in ['xlsx','xls']:
    df = pd.read_excel(file_path)
  elif extension == 'parquet':
    df = pd.read_parquet(file_path)
  else:
    df = pd.DataFrame(data=None)
    print('This extension is not currently supported')
  
  return df

# COMMAND ----------

# DBTITLE 1,get_schema
def get_schema(client, return_format='json'):
  """Read client schema from repo: schema/{client}/schema.json"""
  schema_root = _get_schema_root()
  schema_path = os.path.join(schema_root, client, 'schema.json')
  with open(schema_path) as f:
    d = json.load(f)
  if return_format == 'df':
    return pd.json_normalize(d)
  else:
    return d



# COMMAND ----------

# DBTITLE 1,Dynamically Adjust Dtypes
import numpy as np
import pandas as pd
from datetime import datetime
from distutils.util import strtobool


def correct_dtypes(df):

    def apply(series):

        # Prep character cleanup map
        replace_dict = {
            '·': '-',
            'é': 'e'
        }

        # Grab one non-null value to inspect type
        if series.notna().sum() > 1:
            val = series[series.notna()].iloc[0]
        else:
            val = ''

        # Normalize nulls
        series = (
            series.replace('nan', np.nan)
                  .replace('', np.nan)
                  .fillna(np.nan)
        )

        sample = series[series.notna()].head(100)
        record_count = sample.notna().sum()

        # ---- numeric cleanup check (commas, $, negatives) ----
        valid = {'0','1','2','3','4','5','6','7','8','9',',','-'}

        i = 0
        for row in sample[sample.notna()]:
            matched = [c in valid for c in str(row)]
            perc = pd.Series(matched).sum() / pd.Series(matched).count()
            if perc == 1:
                i += 1

        if record_count == i:
            series = (
                series.astype(str)
                      .str.replace(',', '', regex=False)
                      .replace('-', '')
                      .replace('', np.nan)
                      .str.replace('$', '', regex=False)
            )

        # ---- Nested dtype logic ----
        if record_count == 0:
            series = series.astype(float)

        else:
            if isinstance(val, (np.ndarray, list)):
                series = series.astype(str).replace(replace_dict, regex=True)

            else:
                perc_count = (sample.astype(str).str.endswith('%')).sum()

                # ✅ FIXED escape sequence
                array_count = sample.astype(str).str.contains(r"\[").sum()

                number_count = pd.to_numeric(sample, errors='coerce').notna().sum()

                int_count = (
                    pd.to_numeric(sample, errors='coerce').notna().round(0)
                    == pd.to_numeric(sample, errors='coerce')
                ).sum()

                def bool_convert(x):
                    try:
                        return isinstance(bool(strtobool(str(x))), bool)
                    except:
                        return False

                bool_count = sample.apply(bool_convert).sum()

                try:
                    dtseries = pd.to_datetime(sample, errors='coerce')
                    date_record_count = (
                        dtseries.notna()
                        & (sample.astype(str).str.len() > 4)
                        & (dtseries > datetime(2000, 1, 1))
                    ).sum()
                except:
                    date_record_count = 0

                # ---- Percents ----
                if record_count == perc_count:
                    series = series.str.replace('%', '', regex=False).astype('float64') / 100

                # ---- Arrays ----
                if record_count == array_count:
                    series = (
                        series.astype(str)
                              .str.replace(r"\[", "{", regex=True)
                              .str.replace(r"\]", "}", regex=True)
                    )

                else:
                    # ---- Booleans ----
                    if record_count == bool_count:
                        series = series.apply(lambda x: bool(strtobool(str(x))))

                    else:
                        # ---- Numbers ----
                        if (
                            number_count >= 0.95 * record_count
                            and number_count > 0
                            and record_count != date_record_count
                        ):
                            series = pd.to_numeric(series, errors='coerce')

                        else:
                            # ---- Dates ----
                            if record_count == date_record_count:
                                series = pd.to_datetime(series, errors='coerce')

                            # ---- Text fallback ----
                            else:
                                series = series.astype(str).replace(replace_dict, regex=True)

        return series

    # Apply column by column
    for col in df.columns:
        try:
            df[col] = apply(df[col])
        except Exception:
            df[col] = df[col].astype(str)

    return df


# COMMAND ----------

# DBTITLE 1,Schema Path (repo-based, no mount)
def _get_schema_root():
  """Return path to schema/ folder in repo. Tries config schema_root, workspace paths, then relative to this file."""
  try:
    cfg = _load_etl_config()
    root = cfg.get('schema_root')
    if root and os.path.exists(root):
      return root
  except Exception:
    pass
  for path in [
    '/Workspace/Shared/schema',
    '/Workspace/Repos/Shared/schema',
    os.path.join(os.getcwd(), 'schema'),
  ]:
    if path and os.path.exists(path):
      return path
  raise FileNotFoundError("Schema root not found. Set schema_root in config or ensure schema/ exists in repo.")

# COMMAND ----------

# DBTITLE 1,Load Data Files (Bronze Delta tables)
def _load_etl_config():
  try:
    for path in [
      '/Workspace/Shared/config/etl_config.json',
      '/Workspace/Repos/Shared/config/etl_config.json',
      'config/etl_config.json',
      os.path.join(os.getcwd(), 'config', 'etl_config.json'),
    ]:
      if os.path.exists(path):
        with open(path, 'r') as f:
          return json.load(f)
  except Exception:
    pass
  return {}

def _read_from_bronze(client, table_type):
  """Read from bronze Delta table. table_type: 'transactions'|'sourcecode'|'budget'|'gift'"""
  try:
    cfg = _load_etl_config()
    catalog = cfg.get('bronze_catalog', cfg.get('metadata_catalog', 'dev_catalog'))
    suffix_map = cfg.get('table_suffix_map', {})
    suffix = suffix_map.get(table_type, table_type)
    client_schema = client.lower()
    table_name = f"{catalog}.{client_schema}.{client_schema}_{suffix}"
    spark = __import__('pyspark.sql', fromlist=['SparkSession']).SparkSession.builder.getOrCreate()
    df = spark.table(table_name).toPandas()
    return df
  except Exception as e:
    print(f"_read_from_bronze({client}, {table_type}): {e}")
    return None

def load_parquet(client):
  """Load raw data from bronze Delta table (Data.parquet → gift_bronze)."""
  df = _read_from_bronze(client, 'gift')
  if df is not None:
    col = 'GiftDate' if 'GiftDate' in df.columns else 'gift_date'
    df[col] = pd.to_datetime(df[col], errors='coerce')
    df['Client'] = client
    if 'GiftDate' not in df.columns and 'gift_date' in df.columns:
      df['GiftDate'] = df['gift_date']
    return df
  print(f"load_parquet({client}): bronze table not available")
  return None

def load_parquet_prev(client):
  try:
    filemap = Filemap(client)
    data_prev_parquet_df  = pd.read_parquet(os.path.join(filemap.MASTER, 'Previous_Data.parquet'))
    data_prev_parquet_df['GiftDate'] = pd.to_datetime(data_prev_parquet_df['GiftDate'], errors='coerce')
    data_prev_parquet_df['Client'] = client
    return data_prev_parquet_df
  except Exception as e:
    print(repr(e))

def _read_transactions_from_silver(client):
  """Read processed transactions from Delta silver table (dbo_{client}_transactions_silver)."""
  try:
    cfg = _load_etl_config()
    catalog = cfg.get('silver_catalog', cfg.get('metadata_catalog', 'dev_catalog'))
    schema = cfg.get('client_schema', client.lower())
    tbl = f"dbo_{client.lower()}_transactions_silver"
    table_name = f"{catalog}.{schema}.{tbl}"
    spark = __import__('pyspark.sql', fromlist=['SparkSession']).SparkSession.builder.getOrCreate()
    return spark.table(table_name).toPandas()
  except Exception as e:
    print(f"_read_transactions_from_silver({client}): {e}")
    return None

def load_transactions(client):
  """Load processed transactions from Delta silver table."""
  df = _read_transactions_from_silver(client)
  if df is not None:
    col = 'gift_date' if 'gift_date' in df.columns else 'GiftDate'
    df[col] = pd.to_datetime(df[col], errors='coerce')
    if 'gift_date' not in df.columns and 'GiftDate' in df.columns:
      df['gift_date'] = df['GiftDate']
    df['client'] = client
    return df
  print(f"load_transactions({client}): silver table not available")
  return None

def load_stg(client):
  try:
    filemap = Filemap(client)
    stg  = pd.read_csv(filemap.STAGED+'StagedForFH_wFilters.csv')
    stg['GiftDate'] = pd.to_datetime(stg['GiftDate'], errors='coerce')
    stg['Client'] = client
    return stg
  except Exception as e:
    print(repr(e))

def load_sc(client):
  """Load source code from bronze Delta table."""
  df = _read_from_bronze(client, 'sourcecode')
  if df is not None:
    df['MailDate'] = pd.to_datetime(df['MailDate'], errors='coerce')
    df['Client'] = client
    return df
  print(f"load_sc({client}): bronze sourcecode table not available")
  return None

def load_budget(client):
  """Load budget from bronze Delta table."""
  df = _read_from_bronze(client, 'budget')
  if df is not None:
    return df
  print(f"load_budget({client}): bronze budget table not available")
  return None

def load_cp(client):
  try:
    filemap=Filemap(client)
    cp = pd.read_csv(os.path.join(filemap.CURATED, f'{client}_CampPerf_Data3.csv'))
    cp['GiftDate'] = pd.to_datetime(cp['GiftDate'], errors='coerce')
    cp['MailDate'] = pd.to_datetime(cp['MailDate'], errors='coerce')
    cp = cp.sort_values('GiftDate', ascending=False)

    cp["GiftID"] = cp["GiftID"].astype("string").str.replace(r"\.0$", "", regex=True)

    cp['Client'] = client
    return cp
  except Exception as e:
    print(repr(e))

def load_fh_old(client):
  try:
    filemap = Filemap(client)
    fh = pd.read_csv(os.path.join(filemap.CURATED, '{client}_FH_dataset.csv'.format(client=client)))
    fh['GiftDate'] = pd.to_datetime(fh['GiftDate'], errors='coerce')
    fh = fh.sort_values('GiftDate', ascending=False)
    fh['Client'] = client
    return fh
  except Exception as e:
    print(repr(e))

def load_fh(client):
  try:
    filemap = Filemap(client)
    fh = pd.read_csv(os.path.join(filemap.CURATED, '{client}_FH_v2024.csv'.format(client=client)))
    fh['GiftDate'] = pd.to_datetime(fh['GiftDate'], errors='coerce')
    fh['Client'] = client
    return fh
  except Exception as e:
    print(repr(e))

def load_master_response_curve():
  try:
    filemap = Filemap('Fuse')
    response_curve_master = pd.read_csv(os.path.join(filemap.MASTER, 'ResponseCurve/ResponseCurve_Master.csv'))

    return response_curve_master
  except Exception as e:
    print(repr(e))

def load_stg_fh_DAV(client, start_year):
  try:
    filemap = Filemap(client)
    filename = 'StagedForFH'
    path = os.path.join(filemap.STAGED, filename).split('/dbfs')[-1]
    full_path = f"/dbfs{path}"  # Ensure the full local path is used
    df = pd.read_parquet(full_path)

    #Rename DAV columns to better align with other clients (pandas)
    schema = get_schema_details(filemap.SCHEMA)
    df.rename(columns=schema['DAV_column_mapper'], inplace=True)
    #Sort by GiftDate
    df['GiftDate'] = pd.to_datetime(df['GiftDate'], errors = 'coerce')
    df = df[df['GiftDate']>= datetime(start_year, 1, 1)]
    df.sort_values('GiftDate', ascending =False, inplace = True)
    return df
  
  except Exception as e:
    print(repr(e))

def load_pledge_csv_DAV(client):
  try:
    filemap = Filemap(client)
    filename = 'PLEDGE.csv'
    path = os.path.join(filemap.STAGED, filename).split('/dbfs')[-1]
    full_path = f"/dbfs{path}"  # Ensure the full local path is used
    df = pd.read_csv(full_path)

    #Rename DAV columns to better align with other clients (pandas)
    schema = get_schema_details(filemap.SCHEMA)
    df.rename(columns=schema['DAV_column_mapper'], inplace=True)

    #Sort by Recurring Gift Start
    df['Recurring Gift Start'] = pd.to_datetime(df['Recurring Gift Start'], errors = 'coerce')
    df.sort_values('Recurring Gift Start', ascending=False, inplace=True)
    return df
  
  except Exception as e:
    print(repr(e))

def load_gifts_csv_DAV_spark(client):
  try:
    gift_cols = [
      'Lookup ID', 'Payment Method', 'Other Method', 'Contribution Date', 'DI Code',
      'Contribution Amount', 'Revenue ID', 'Source Code',
      'Designation Lookup ID', 'Recurring Gift ID', 
      'Inbound Channel', 'Types of Gifts', 'Appeal Name']
    file = 'GIFTS.csv'
    path = os.path.join(filemap.STAGED, file).split('/dbfs')[-1]
    df = spark.read.format("csv").option("header","true").load(path)
    df = df.select(gift_cols)

    #Rename DAV columns to better align with other clients (spark)
    # Get the column mapping dictionary
    schema = get_schema_details(filemap.SCHEMA)
    column_map = schema['DAV_column_mapper']  # This should be a dict like {"old_col": "new_col", ...}
    # Apply renaming for all columns
    for old_col, new_col in column_map.items():
        df = df.withColumnRenamed(old_col, new_col)

    #Rename "Appeal Name" column in gifts df to "GiftChannelName"
    df = df.withColumnRenamed('Appeal Name', 'GiftChannelName')


    return df
  except Exception as e:
    print(repr(e))

def load_gifts_csv_DAV_pandas(client, start_year):
    try:
        gift_cols = [
            'Lookup ID', 'Payment Method', 'Other Method', 'Contribution Date', 'DI Code',
            'Contribution Amount', 'Revenue ID', 'Source Code',
            'Designation Lookup ID', 'Recurring Gift ID',
            'Inbound Channel', 'Types of Gifts', 'Appeal Name'
        ]
        file = 'GIFTS.csv'
        path = os.path.join(filemap.STAGED, file)

        # Count total rows in file (excluding header)
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            total_rows = sum(1 for _ in f) - 1

        # Try fast C engine first
        try:
            df = pd.read_csv(path, usecols=gift_cols, dtype=str)
            skipped_lines = total_rows - len(df)
        except pd.errors.ParserError:
            print("C engine failed, falling back to Python engine with skip.")
            df = pd.read_csv(
                path,
                usecols=gift_cols,
                dtype=str,
                engine='python',
                on_bad_lines='skip'
            )
            skipped_lines = total_rows - len(df)
            print(f"{skipped_lines} bad line(s) were skipped during CSV load.")

        # Convert 'Contribution Amount' to float
        if 'Contribution Amount' in df.columns:
            df['Contribution Amount'] = pd.to_numeric(
                df['Contribution Amount'].str.replace(r'[\$,]', '', regex=True),
                errors='coerce'
            )

        # Convert 'Contribution Date' to datetime
        if 'Contribution Date' in df.columns:
            df['Contribution Date'] = pd.to_datetime(
                df['Contribution Date'],
                errors='coerce',
                infer_datetime_format=True
            )

        # Load schema mapping and apply column renames
        schema = get_schema_details(filemap.SCHEMA)
        column_map = schema.get('DAV_column_mapper', {})
        df = df.rename(columns=column_map)

        # Rename "Appeal Name" to "GiftChannelName" if not mapped
        df = df.rename(columns={'Appeal Name': 'GiftChannelName'})

        # Filter by start year
        if 'GiftDate' in df.columns:
            df = df[df['GiftDate'] >= datetime(start_year, 1, 1)]

            # Sort by date
            df.sort_values('GiftDate', ascending=False, inplace=True)

        return df

    except Exception as e:
        print(f"Error loading gifts CSV: {repr(e)}")
        return pd.DataFrame()
      

def load_recurring_gifts_csv_DAV_pandas(client, start_year):
    try:
        file = 'GIFTS.csv'
        path = os.path.join(filemap.STAGED, file)

        gift_cols = [
            'Lookup ID', 'Payment Method', 'Other Method', 'Contribution Date', 'DI Code',
            'Contribution Amount', 'Revenue ID', 'Source Code',
            'Designation Lookup ID', 'Recurring Gift ID',
            'Inbound Channel', 'Types of Gifts', 'Appeal Name'
        ]

        def try_read_csv():
            try:
                return pd.read_csv(path, usecols=gift_cols, dtype=str)
            except pd.errors.ParserError:
                print("C engine failed, falling back to Python engine with skip.")
                return pd.read_csv(path, usecols=gift_cols, dtype=str, engine='python', on_bad_lines='skip')

        df = try_read_csv()

        # Drop rows with missing or blank Recurring Gift ID
        df = df[df['Recurring Gift ID'].notna() & (df['Recurring Gift ID'].str.strip() != '')]

        # Clean and convert columns
        df['Contribution Amount'] = pd.to_numeric(
            df['Contribution Amount'].str.replace(r'[\$,]', '', regex=True),
            errors='coerce'
        )
        df['Contribution Date'] = pd.to_datetime(df['Contribution Date'], errors='coerce')

        # Rename columns based on schema
        schema = get_schema_details(filemap.SCHEMA)
        df = df.rename(columns=schema.get('DAV_column_mapper', {}))
        df = df.rename(columns={'Appeal Name': 'GiftChannelName'})

        # Filter by start year
        if 'GiftDate' in df.columns:
            df = df[df['GiftDate'] >= datetime(start_year, 1, 1)]
            df.sort_values('GiftDate', ascending=False, inplace=True)

        return df

    except Exception as e:
        print(f"Error loading recurring gifts: {e}")
        return pd.DataFrame()

def load_package_csv_DAV(client):
  try:
    package_cols = ['Source Code', 'Segment Description', 'Marketing Effort Name', 'Appeal Name']
    file = 'PACKAGE.csv'
    path = os.path.join(filemap.STAGED, file).split('/dbfs')[-1]
    df_package = spark.read.format("csv").option("header","true").load(path)
    df_package = df_package.select(package_cols)
    return df_package
  except Exception as e:
    print(repr(e))

def load_package_csv_DAV_pandas(client):
    try:
        package_cols = ['Source Code', 'Segment Description', 'Marketing Effort Name', 'Appeal Name']
        file = 'PACKAGE.csv'
        path = os.path.join(filemap.STAGED, file).split('/dbfs')[-1]
        full_path = '/dbfs' + path  # Full local path for pandas to read from DBFS

        df_package = pd.read_csv(full_path, usecols=package_cols)
        return df_package
    except Exception as e:
        print(repr(e))




def column_values_across_clients(file_func, column_name): #Load unique values from same column across clients into DF
    result_df = pd.DataFrame()  # Initialize an empty DataFrame
    for client in client_list():
        try:
            # Call file_func with the client to get the DataFrame
            df = file_func(client)
            if column_name in df.columns:
                # Get unique values from the column
                unique_values = df[column_name].drop_duplicates().reset_index(drop=True)
                # Add to result DataFrame
                result_df[f'{client}_{column_name}'] = unique_values
            else:
                print(f'Warning: {column_name} not found in {client} data.')
        except Exception as e:
            print(f'Error processing {client}: {repr(e)}')

    return result_df

# COMMAND ----------

# DBTITLE 1,Timestamp in ET
def ts_now():
  import pandas as pd
  ts = pd.Timestamp.now(tz='US/Eastern').strftime("%Y-%m-%d %H:%M:%S")
  return ts

# COMMAND ----------

# DBTITLE 1,get_table_schemas
def get_table_schemas():
  """Read Fuse table schemas from repo: schema/Fuse/table_schemas.json"""
  schema_root = _get_schema_root()
  schema_path = os.path.join(schema_root, 'Fuse', 'table_schemas.json')
  with open(schema_path) as f:
    table_schema = json.load(f)
  return table_schema