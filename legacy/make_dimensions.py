# Databricks notebook source
'''
This script builds common dimension tables:
- Inflates / extracts dimension lookup tables

Inputs: 
- Compressed dimension mappings
  - FHgroup
  - FHgroupdetail
  - Sustainer
- the lookup tables defined in /Reference/RFM_Values.xlsx:
  - FreqID
  - MrcID
  - MrcAmtID
  - SustMrcAmtID

Outputs:
- /Shared/FHGroup.csv
- /Shared/FHGroupDetail.csv
- /Shared/Sustainer.csv
- /Shared/FreqID.csv
- /Shared/MrcIDcsv
- /Shared/SustMrcID.csv
- /Shared/MrcAmtID.csv
'''

# COMMAND ----------

import base64
import csv
from datetime import datetime
import json
import logging
import numpy as np
import os
import pandas as pd
import re
import zlib

# COMMAND ----------

# MAGIC %run ./mount_datalake

# COMMAND ----------

# MAGIC %run ./logger

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# Receive the client context from the calling notebook.
# E.g., in TSE_ETL.ipynb, this notebook is invoked with the argument {"CLIENT" : "TSE"}
try:
  CLIENT = dbutils.widgets.get("CLIENT")
except:
  CLIENT = "TSE" 

# establish file storage directories
filemap = Filemap(CLIENT)

# establish context
logfile_name, logfile_loc = copy_logfile(filemap.LOGS)
process = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# instantiate logger
logger = create_logger(logfile_loc)

#instantiate a Runner object
runner = Runner(process, logfile_loc, logfile_name, filemap.LOGS)

logger.critical("%s - logging for notebook: %s" % (str(datetime.now()), process))
update_adls(logfile_loc, filemap.LOGS, logfile_name)

# COMMAND ----------

def cast_dim_columns(df):  
  '''
  Wrapper around the utilities/cast_columns function, used for convenience..
  Arguments:
  -  df: a Pandas dataframe containing columns to be cast
  Returns:
  -  a Pandas dataframe with converted column types
  '''
  type_map = {
    "Min" : "Int64",
    "Max" : "Int64"  
    }
  try:
    return cast_columns(df, type_map)
  except Exception as e:
    desc = 'error casting column'
    log_error(runner, desc, e)
    
def inflate(compressed_string, header):
  '''
  Decompresses the given string and converts to a Pandas dataframe.
  Arguments:
  -  compressed_string: a string representing tabular data which has been compressed by PowerBI
  -  header: the column name to use in the Pandas dataframe
  Returns:
  -  a Pandas dataframe
  '''
  try:
    inflated = zlib.decompress(base64.b64decode(compressed_string), wbits = -zlib.MAX_WBITS)
    return pd.DataFrame(eval(inflated), columns = [header])
  except Exception as e:
    desc = 'error inflating compressed string'
    log_error(runner, desc, e)

# COMMAND ----------

# MAGIC %md ## Inflate lookups and make dimension tables

# COMMAND ----------

# FHgroup

'''
= Table.FromRows(Json.Document(
    Binary.Decompress(
        Binary.FromText(
            "i45W8kstV4rVAdMKPonFJQqRqYlFYBHn/Lzi1OTSksyyVAV3IAERDUrNzCsuSSxJTUFT7pNYUJyaAjErH6g+My/dI7O4JL+oUik2FgA=", 
                BinaryEncoding.Base64), 
            Compression.Deflate)), 
        let _t = ((type text) meta [Serialized.Text = true]) in type table [FHgroup = _t])
'''
FHgroup_c = "i45W8kstV4rVAdMKPonFJQqRqYlFYBHn/Lzi1OTSksyyVAV3IAERDUrNzCsuSSxJTUFT7pNYUJyaAjErH6g+My/dI7O4JL+oUik2FgA="

# Inflate the string
FHgroup_df = inflate(FHgroup_c, "FHgroup")

# Write to disk
try:
  FHgroup_df.to_csv(filemap.SHARED + "FHGroup.csv", index=False)
except Exception as e:
  desc = 'error writing to file'
  log_error(runner, desc, e)
  
# FHgroup_df

# COMMAND ----------

# FHgroupdetail

'''
= Table.FromRows(Json.Document(
    Binary.Decompress(
        Binary.FromText(
            "i45W8kstV4rVAdMKPonFJQqRqYlFYBHn/Lzi1OTSksyyVAV3IFGkYKKtEFlUjEPSGI+cEVwuKDUzr7gksSQ1BWGbgqGxrpEJbmkjU11jM9zSxua6Jha4pU0stcGSPokFxUAJhF1QAYTpUAGEeVABmAl++UDfZOale2QWl+QXVSrFxgIA", 
                BinaryEncoding.Base64), 
            Compression.Deflate)), 
        let _t = ((type text) meta [Serialized.Text = true]) in type table [Fhgroupdetail = _t])
'''
FHgroupdetail_c = "i45W8kstV4rVAdMKPonFJQqRqYlFYBHn/Lzi1OTSksyyVAV3IFGkYKKtEFlUjEPSGI+cEVwuKDUzr7gksSQ1BWGbgqGxrpEJbmkjU11jM9zSxua6Jha4pU0stcGSPokFxUAJhF1QAYTpUAGEeVABmAl++UDfZOale2QWl+QXVSrFxgIA"

# Inflate the string
FHgroupdetail_df = inflate(FHgroupdetail_c, "FHGroupDetail")

# Write to disk
try:
  FHgroupdetail_df.to_csv(filemap.SHARED + "FHGroupDetail.csv", index=False)
except Exception as e:
  desc = 'error writing to file'
  log_error(runner, desc, e)
  
# FHgroupdetail_df

# COMMAND ----------

# Sustainer

'''
= Table.FromRows(Json.Document(
    Binary.Decompress(
        Binary.FromText(
            "i45WCi4tLknMzEstUorViVYyjFBwzywDcWIB", 
            BinaryEncoding.Base64), 
        Compression.Deflate)), 
    let _t = ((type text) meta [Serialized.Text = true]) in type table [SustainerFlag = _t])
'''
Sustainer_c = "i45WCi4tLknMzEstUorViVYyjFBwzywDcWIB"

# Inflate the string
Sustainer_df = inflate(Sustainer_c, "SustainerFlag")

# Write to disk
try:
  Sustainer_df.to_csv(filemap.SHARED + "Sustainer.csv", index=False)
except Exception as e:
  desc = 'error writing to file'
  log_error(runner, desc, e)
  
# Sustainer_df

# COMMAND ----------

# MAGIC %md ## Extract additional dimension tables

# COMMAND ----------

# Make Dimension tables:
# Read data

'''
= Excel.Workbook(Web.Contents(
    "https://mindsetdirect.sharepoint.com/Shared%20Documents/PowerBI/generalSupportDocs/RFM_Values.xlsx"), 
    null, true)
    
= Source{[Item="MRCid",Kind="Sheet"]}[Data]

= Table.PromoteHeaders(MRCid_Sheet, [PromoteAllScalars=true])

'''
try:
  FreqID = pd.read_excel(filemap.SHARED + "Reference/RFM_Values.xlsx", sheet_name = "Freqid")
  MrcID = pd.read_excel(filemap.SHARED + "Reference/RFM_Values.xlsx", sheet_name = "MRCid")
  AmountIDLookup = pd.read_excel(filemap.SHARED + "Reference/RFM_Values.xlsx", sheet_name = "MRCamtid")
  HpcID = pd.read_excel(filemap.SHARED + "Reference/RFM_Values.xlsx", sheet_name = "MRCamtid")
except Exception as e:
  desc = 'error reading file'
  log_error(runner, desc, e)
  

# COMMAND ----------

# Freqid

'''
= Table.TransformColumnTypes(#"Promoted Headers",
    {{"Alpha", type text}, 
    {"Min", Int64.Type}, 
    {"Max", Int64.Type}, 
    {"Value", type text}})
'''
# cast columns    
FreqID = cast_dim_columns(FreqID)
        
# Rename columns
'''
= Table.RenameColumns(#"Changed Type",{{"Value", "FreqID"}})
'''
FreqID = FreqID.rename(columns={"Value" : "FreqID"})

# Write to disk
try:
  FreqID.to_csv(filemap.SHARED + "FreqID.csv", index=False)
except Exception as e:
  desc = 'error writing to file'
  log_error(runner, desc, e)
  

# COMMAND ----------

# Hpcid

# cast columns    
HpcID = cast_dim_columns(HpcID)
        
# Rename columns
HpcID = HpcID.rename(columns={"Value" : "HpcID"})

# Write to disk
try:
  HpcID.to_csv(filemap.SHARED + "HpcID.csv", index=False)
except Exception as e:
  desc = 'error writing to file'
  log_error(runner, desc, e)
  

# COMMAND ----------

# MRCid and Sust_MRCid

MrcID = MrcID[MrcID["Alpha"] != "Z"]

'''
= Table.TransformColumnTypes(#"Promoted Headers",
    {{"Alpha", type text}, 
    {"Min", Int64.Type}, 
    {"Max", Int64.Type}, 
    {"Value", type text}})
'''
# cast columns
MrcID = cast_dim_columns(MrcID)

# Rename columns
'''
= Table.RenameColumns(#"Changed Type",{{"Value", "MRCid"}})
'''
MrcID = MrcID.rename(columns={"Value" : "MrcID"})
SustMrcID = MrcID.rename(columns={"MrcID" : "SustMrcID"})

# Write to disk
try:
  MrcID.to_csv(filemap.SHARED + "MrcID.csv", index=False)
  SustMrcID.to_csv(filemap.SHARED + "SustMrcID.csv", index=False)
except Exception as e:
  desc = 'error writing to file'
  log_error(runner, desc, e)
  

# COMMAND ----------

# AmountIDLookup

'''
= Table.TransformColumnTypes(#"Promoted Headers",
    {{"Alpha", type text}, 
    {"Min", Int64.Type}, 
    {"Max", Int64.Type}, 
    {"Value", type text}})
'''
# cast columns
AmountIDLookup = cast_dim_columns(AmountIDLookup)
        
# Update value
'''
= Table.ReplaceValue(#"Changed Type","NoMRCamt","NoAmt",Replacer.ReplaceText,{"Value"})
'''
AmountIDLookup["Value"] = np.where(AmountIDLookup["Value"] == "Z: NoMRCamt", "Z: NoAmt", AmountIDLookup["Value"])

# Rename columns
'''
= Table.RenameColumns(#"Replaced Value",{{"Value", "MRCamtid"}})
'''
AmountIDLookup = AmountIDLookup.rename(columns={"Value" : "AmountIDLookup"})

# Write to disk
try:
  AmountIDLookup.to_csv(filemap.SHARED + "AmountIDLookup.csv", index=False)
except Exception as e:
  desc = 'error writing to file'
  log_error(runner, desc, e)
  

# COMMAND ----------

# write to log file in ADLS2
logger.critical("%s - exiting notebook: %s" % (str(datetime.now()), process))
update_adls(logfile_loc, filemap.LOGS, logfile_name)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))