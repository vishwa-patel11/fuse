# Databricks notebook source
# MAGIC %md # MC Source Code History from ADLS (ETL1 and ETL2 SourceCode Production)

# COMMAND ----------

# MAGIC %md ## Imports and runs

# COMMAND ----------

#  Unzip promo files and update table


# COMMAND ----------

# DBTITLE 1,imports
from datetime import datetime
import numpy as np
import os
import subprocess
import time
import zipfile

# COMMAND ----------

# DBTITLE 1,load Sharepoint
# pause the process until at least the Office 365 Sharepoint library has loaded

libs = []
while not libs:
  libs = subprocess.check_output(['pip', 'freeze'])
  libs = [x for x in libs.decode('utf-8').split('\n') if 'office365' in x.lower()]
  print('no')
  time.sleep(5)

# COMMAND ----------

# DBTITLE 1,mount_datalake
# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# DBTITLE 1,utilities
# MAGIC %run ../python_modules/utilities

# COMMAND ----------

# DBTITLE 1,transfer_files
# MAGIC %run ../python_modules/transfer_files

# COMMAND ----------

# MAGIC %run ../python_modules/source_code_validation

# COMMAND ----------

# DBTITLE 1,helpers
# # helpers
def get_date():
  date_str = str(datetime.now().date()).replace('-','')
  date_str = ''.join(['Counts', date_str]) + '.csv'
  return '/'.join(['', 'tmp', date_str])

def pad_values(df, m, d, char):
  if d == 'left':
    for k,v in m.items():
      df[k] = df[k].str.ljust(v, char)
  elif d == 'right':
    for k,v in m.items():
      df[k] = df[k].str.rjust(v, char)
  return df

# COMMAND ----------

# MAGIC %md ##Establish client context

# COMMAND ----------

# DBTITLE 1,Initiate Client
# initiate client
client = 'MC'
filemap = Filemap(client)

# COMMAND ----------

# MAGIC %md ##Transfer data from SharePoint

# COMMAND ----------

# DBTITLE 1,Transfer Data from Sharepoint (Commented Out)
# # get content metadata
# SP_URL = 'Shared Documents/Reporting 2.0/%s/Monthly' %client
# files = get_all_contents(client_context, SP_URL)
# print(files)  

# for file in files:
#   # point to the individual zip file
#   source_file = client_context.web.get_file_by_server_relative_url(file)
#   file_name = file.split('/')[-1]
#   local_file_name = os.path.join('/tmp', file_name)

#   # transfer file from Share Point to dbfs
#   execute_transfer(source_file, local_file_name, check=True)

#   # unzip file to the dbfs cluster
#   with zipfile.ZipFile(os.path.join('/tmp', file_name),"r") as zip_ref:
#     zip_ref.extractall(path='/tmp')
  
# # take just the promo files
# files = [x for x in os.listdir('/tmp') if 'promo' in x.lower()]
# files

# COMMAND ----------

# MAGIC %md #Read the data into memory

# COMMAND ----------

# # collect the promo files
# dfs = []
# for promo in files:
#   print('file: ', promo)
#   _df = pd.read_csv(os.path.join('/tmp', promo), encoding='ISO-8859-1')
#   _df.to_csv(os.path.join(filemap.RAW, 'PromoFiles', promo), index=False)
#   dfs.append(_df)
# df = pd.concat(dfs)
# print(df.shape)
# df.head(2)

# COMMAND ----------

# DBTITLE 1,Collect Files
# collect the promo files
dfs = []
_path = os.path.join(filemap.RAW, 'PromoFiles')
for promo in os.listdir(_path):
  print('file: ', promo)
  _df = pd.read_csv(os.path.join(_path, promo), encoding='ISO-8859-1')
  dfs.append(_df)
df = pd.concat(dfs)
print(df.shape)
df.head(2)

# COMMAND ----------

# MAGIC %md #Format the data

# COMMAND ----------

# DBTITLE 1,Rename columns, take only relevant campaigns
# rename the columns
m = {
  'Assigned Appeal Date': 'MailDate',
  'Assigned Appeal ID': 'CampaignCode',
  'Assigned Package ID': 'PackageCode',
  'Assigned Appeal Marketing Segment': 'ListCode',
  'Assigned Appeal Marketing Source Code': 'SourceCode',
  'Assigned Package Description': '_PackageName'
}
df = df.rename(columns=m)
df.MailDate = pd.to_datetime(df.MailDate)

# take just the relevant campaigns
m1 = df.CampaignCode.str.startswith(('AG', 'AH', 'AN', 'AW', 'QQ'))
m2 = df.MailDate.dt.year > 2021
mask = (m1) & (m2)
df = df.loc[mask]

print(df.shape)
df.head(2)

# COMMAND ----------

# df['OriginalListCode'] = df.ListCode.copy()


# COMMAND ----------

# DBTITLE 1,Continue Formatting
# fill the segment codes for AG campaigns
df.ListCode = np.where(
  df.CampaignCode.str.startswith('AG').fillna(value=False),
  df.ListCode.fillna(value='LLLL'),
  df.ListCode  
)

# left pad values with 'Z's
m = {
  'CampaignCode': 8,
  'PackageCode': 2
}
pad_values(df, m, 'left', 'Z')

# right pad values with 'L's
m = {'ListCode': 4}
pad_values(df, m, 'right', 'L')

# fill null list codes with 'Z's
df.ListCode = df.ListCode.fillna(value='LLLL')
 
# construct the source codes
df['SourceCode'] = np.where(
  df.CampaignCode.str.startswith('QQ'),
  df.SourceCode.str.replace(' ',''),
  df.CampaignCode + df.ListCode + df.PackageCode
)
df.SourceCode.str.len().describe()

# COMMAND ----------

# mask = df.OriginalListCode != df.ListCode
# df.loc[mask]

# COMMAND ----------

# DBTITLE 1,Create and pad list codes for acquisitions
# create and pad list codes for acquisitions
df.ListCode = np.where(
  df.CampaignCode.str.startswith('QQ').fillna(value=False),
  df.SourceCode.str[8:12].str.ljust(4,'Z'),
  df.ListCode
)
df.ListCode.str.len().describe()

# COMMAND ----------

# MAGIC %md ##Perform aggregations

# COMMAND ----------

# drop duplicates
cols = ['SourceCode', 'Constituent ID']
df = df.drop_duplicates(cols)

# counts by source code 
counts = df.groupby('SourceCode').size().to_frame(name='Quantity')

# source code mail dates
col = 'MailDate'
df[col] = pd.to_datetime(df[col]).dt.date
dates = df.groupby('SourceCode')[col].max().to_frame(name='MailDate')

# join aggregated results
_df = pd.merge(counts, dates, on='SourceCode').reset_index()

# join additional codes
cols = ['SourceCode', 'CampaignCode', 'ListCode', 'PackageCode', '_PackageName']
_df = pd.merge(_df, df[cols].drop_duplicates(cols[0]), on=cols[0], how='left')
_df

# COMMAND ----------

# MAGIC %md # Remove acquisitions

# COMMAND ----------

# # added ex post to reflect Madeline's suggestion that acquistions should be done manually
# mask = _df.CampaignCode.str.startswith('QQ')

# # edited 6/6/2023 to reflect Mike's update that starting w/ FY23 March acquisitions, 
# # we will need to get source codes from the promo history.
mask = (_df.CampaignCode.str.startswith('QQ')) & (_df.MailDate < datetime(2023,3,1).date())

print(_df.shape)
_df = _df.loc[~mask]
_df.shape

# COMMAND ----------

mask = (_df.CampaignCode.str.startswith('QQ')) & (_df.CampaignCode.str.endswith('0423'))
# _df.loc[mask].groupby('CampaignCode')['Quantity'].sum()
_df.loc[mask, 'Quantity'].sum()

# COMMAND ----------

# MAGIC %md #Write counts file to SharePoint

# COMMAND ----------

# write and stage files
try:
  # write counts file to cluster
  counts_file = get_date()
  _df.to_csv(counts_file, index=False)  
except Exception as e:
  raise e

# COMMAND ----------

# write flat file to SharePoint
try:
  # establish share point context
  # relative_url = 'Shared Documents/Reporting 2.0/MC/SourceCodeDocs/Counts'
  relative_url = 'Shared Documents/Reporting 2.0/SourceCodeDocs/MC/Archive/Counts (Legacy)'
  libraryRoot = client_context.web.get_folder_by_server_relative_url(relative_url)
  libraryRoot

  # create and load file object
  info = FileCreationInformation()    
  with open(counts_file, 'rb') as content_file:
      info.content = content_file.read()

  info.url = counts_file.split('/')[-1]
  info.overwrite = True
  upload_file = libraryRoot.files.add(info)

  # transfer file to share point
  client_context.execute_query()
  
except Exception as e:
  raise e

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))