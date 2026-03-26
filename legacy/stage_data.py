# Databricks notebook source
'''
Unused.  The functions previously contained here now located in /python_modules/utilities.
What remains here are legacy functions that can likely be omited from the codebase.

Previous docstring:
This file provides the utility functions used commonly across clients for the staging of data:
- Reading files from /Raw
- Validating columns and datatypes
- Extracting the latest data file from potentially many
- Writing the files to /Staged
- etc.
'''

# COMMAND ----------

import csv
from datetime import datetime
import json
import os
import pandas as pd
import re
from shutil import copyfile
import time


# COMMAND ----------

## NOT USED

def assert_existence(filemap, schema): 
  '''
  Asserts that the expected files are present in the /Raw directory.
  Assumes that files belong to one of three common categories:
    -  Ancillary:  support files
    -  Monthly files:  primary periodic data published by the client
    -  Special files:  in the same format as Monthly files, but not refreshed regularly
  Arguments:
  -  filemap: a ./mount_datalake/Filemap object containing storage path details
  -  schema:  a dict containing the reference informaton from schema.json
  Returns:
  -  monthly: a list of the monthly file names
  -  special: a list of the special file names
  -  ancillary: a list of the ancillary (support) file names
  '''
  monthly = []
  special = []
  ancillary = []
  for file_name in os.listdir(filemap.RAW):
    try:
      assert file_name in schema.keys()
      ancillary.append(file_name)
    except AssertionError:
      if MONTHLY_PATTERN.match(file_name):
        monthly.append(file_name) 
      elif SPECIAL_PATTERN.match(file_name):
        special.append(file_name)                    
      try:
        assert (monthly or special)
      except AssertionError as e:
        desc = 'file not found'
        log_error(runner, desc, e)
  return monthly, special, ancillary 

def get_latest(monthly):
  '''
  Inspects the monthly files in a directory to determine the newest file based on file name.
  Arguments:
  -  monthly: a list of the file names identified as containing periodic data published by the client
  Returns:
  -  monthly: a string containing the file name identified as the most recent
  '''
  #all_months = monthly
  mdict = {}

  # extract the month and year from the file name
  # extract just the digits
  for i in range(len(monthly)):
    mdict[monthly[i]] = {"id":int(re.findall('\d+', monthly[i])[0])}  
  # split into month and year
  for k in mdict.keys():
    mdict[k]['year'] = mdict[k]['id']%100
    mdict[k]['month'] = int(round(mdict[k]['id']/100,0))

  # get the latest year
  mdict = get_latest_key(mdict, 'year') 

  # get the latest month
  mdict = get_latest_key(mdict, 'month')

  # update the 'monthly' object with only the file name 
  # corresponding to the latest month and year
  monthly = [next(iter(mdict))]
  return monthly

def get_latest_key(date_dict, key_name):
  '''
  Extracts the latest date component from a dictionary of dates.
  Arguments:
  - date_dict: a dictionary representing the dates as extracted from the client file names
  - key_name: a string representing the date component to be compared
  Returns:
  - the modified dictionary containing only the latest of the date component specified
  '''
  dates = []
  for k in date_dict.keys():
    dates.append(date_dict[k][key_name])
  for k,v in list(date_dict.items()):
    if v[key_name] < max(set(dates)):
      del date_dict[k]
  return date_dict

  

    






  

    

# COMMAND ----------

