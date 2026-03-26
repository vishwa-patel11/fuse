# Databricks notebook source
'''
Broker Report:
  Fixes white mail issues and errant source codes
  Aggregates returns and revenue for Aquisitions
  Joins to relevant source code data
  Writes to ADLS2
'''

# COMMAND ----------

from datetime import datetime
import numpy as np
import json
import os
import pandas as pd

from office365.runtime.auth.authentication_context import AuthenticationContext

import warnings
warnings.filterwarnings("ignore")


# COMMAND ----------

# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# MAGIC %run ../python_modules/parser

# COMMAND ----------

# MAGIC %run ../python_modules/utilities

# COMMAND ----------

# MAGIC %run ../python_modules/email

# COMMAND ----------

# MAGIC %run ../python_modules/transfer_files

# COMMAND ----------

# MAGIC %run ../python_modules/source_code_validation

# COMMAND ----------

# # establish context
CLIENT_SITE_URL = "https://mindsetdirect.sharepoint.com/sites/ClientFiles"
filemap = Filemap('Fuse')
with open(os.path.join(filemap.SCHEMA, 'schema.json'), 'r') as f:
  USER = json.load(f)['SharePointUID']
PW = dbutils.secrets.get(scope='fuse-etl-key-vault', key='sharepoint-pwd')

try:
  client = 'MC'
  filemap = Filemap(client)
except Exception as e:
  raise e




# COMMAND ----------

# Read in reference data
try:
  # Read config data from schema
  schema = get_schema_details(filemap.SCHEMA)
#   path = schema['Directories']['CampaignCode']

  # read master source code doc from adls2
  sc = pd.read_csv(os.path.join(filemap.MASTER, 'SourceCode.csv'))
  print(sc.shape)
  
except Exception as e:
  raise e

# COMMAND ----------

# read most recent transaction history
try:
  df = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
  mapper = {
    'SegmentCode': 'ListCode',
    'PackageID': 'PackageCode'
    }
  df = df.rename(columns=mapper)
  df = update_client_codes(df, client)
except Exception as e:
  raise e

print('data: ', df.shape)
df.head().T

# COMMAND ----------

df.CampaignCode = np.where(
  df.CampaignCode.isna(),
  df.SourceCode.str[:8],
  df.CampaignCode
)

# COMMAND ----------


sc['MailDate'] = pd.to_datetime(sc['MailDate'])
sc['Fiscal'] = fiscal_from_column(sc, 'MailDate', schema['firstMonthFiscalYear'])
sc.Fiscal.describe()

# COMMAND ----------

df.CampaignCode = df.CampaignCode.fillna(value='')

# COMMAND ----------

# process source code data

try:
  # take only source codes from 2021 or later
  mask = sc['Fiscal'] > 2020
  sc = sc[mask]

  # drop duplicate source codes
  sc = sc.drop_duplicates('SourceCode')

  # take only appeals that start with 'QQ'
  mask = sc['CampaignCode'].str.startswith('QQ').fillna(value=False)
  sc = sc[mask]

  # overwrite those source codes that start with XXXX/, e.g. D36A/ from July 2021 Acquisition
  sc['SourceCode'] = sc['SourceCode'].str.split('/')
  sc['SourceCode'] = [x[-1] for x in sc['SourceCode'].values.tolist()]

  # fix error source codes
  sc_errors = schema['SourceCodeErrors']
  condition = df['SourceCode'].isin(sc_errors)
  df['SourceCode'] = np.where(condition, df['SourceCode'].str.replace('QQ1N', 'QQ2N'), df['SourceCode'])

  # collect all relevant source codes
  codes_to_use = sc['SourceCode'].unique()

  # save appeal id to campaign name mapping
  cols = ['CampaignCode', 'CampaignName']
  _sc = sc.drop_duplicates('CampaignCode', keep='last')[cols]
  appealsToCampaigns = dict(zip(_sc['CampaignCode'], _sc['CampaignName']))

  # remove whitespace
  sc['SourceCode'] = [x.replace(' ','') for x in sc['SourceCode'].values.tolist()]
  
except Exception as e:
  raise e

print(sc.shape)
sc.head()

# COMMAND ----------

df.head().T

# COMMAND ----------

# process transaction data
try:
  # data through
  data_through = str(df['GiftDate'].max().date())
#   data_through = str(data_through.date())
  print('data_through: ', data_through)
  
  # take only appeal ids that start with 'QQ'
  df['CampaignCode'] = [x.split(';')[0] for x in df['CampaignCode'].values.tolist()]
  mask = df['CampaignCode'].str.startswith('QQ').fillna(value=False)
  df = df[mask]

  # take only gift dates from 2021 and later
  df['GiftDate'] = pd.to_datetime(df['GiftDate'])
  mask = df['GiftDate'].dt.year >= 2021
  df = df[mask]
  
  # create boolean vectors for filtering
  df['CodesInSC'] = df['SourceCode'].isin(codes_to_use)
  df['WhiteMail'] = df['SourceCode'].isna()
#   df['WhiteMail'] = ((df['PackageCode'].isna()) | (df['ListCode'].isna())) & (df['SourceCode'].isna())

  # fix segment codes
  condition = df['ListCode'].str.len() > 4
  df['ListCode'] = np.where(condition, None, df['ListCode'])
  
  # create white mail source codes
  df['SynthSC'] = df['CampaignCode'] + df['ListCode'].fillna(value='LLLL') + df['PackageCode'].fillna(value='XX')
  df['SourceCode'] = np.where(df['WhiteMail'], df['SynthSC'], df['SourceCode'])
  
  # create gift level filter
  df = GiftLevel(df)
  
except Exception as e:
  raise e

print(df.shape)
df.head()


# COMMAND ----------

# calculate returns and revenue by source code

try:
  d = {'GiftID': 'nunique', 'GiftAmount': 'sum'}
  cols = ['SourceCode', 'GiftLevel']
  grouped = df.groupby(cols).agg(d).reset_index()
  mapper = {
    'GiftID': 'Returns',
    'GiftAmount': 'Revenue'
  }
  grouped = grouped.rename(columns=mapper)

  _df = grouped.groupby('SourceCode').size().to_frame(name='SC_Count')
  grouped = pd.merge(grouped, _df, on='SourceCode', how='left')
  
except Exception as e:
  raise e
  
print(grouped.shape)
grouped.head()

# COMMAND ----------

# Sanity check

a = set(df['SourceCode'].unique())
b = set(sc['SourceCode'].unique())
no_sc = a.intersection(a^b)
print(len(no_sc))
print(df[df['WhiteMail']]['SourceCode'].nunique())
print(sc['SourceCode'].nunique())

# COMMAND ----------

# join aggregated transaction data to source code data and calculate further results
try:
  sc = pd.merge(sc, grouped, on='SourceCode', how='outer')

  # fix merge issues
  cols = ['Revenue', 'Returns', 'SC_Count']
  for col in cols:
    sc[col] = sc[col].fillna(value=0)

  # normalize mail values
  condition = sc['SC_Count'] > 0
  sc['Quantity'] = np.where(condition, sc['Quantity'] / sc['SC_Count'], sc['Quantity'])
  sc['ListCost'] = (sc['ListCPP'] * sc['Quantity']).fillna(value=0)
  sc['PackageCost'] = (sc['PackageCPP'] * sc['Quantity']).fillna(value=0)

  # derive KPIs
  sc['Net'] = sc['Revenue'] - sc['ListCost'] - sc['PackageCost']
  sc['NCTA'] = sc['Net'] / sc['Returns']
  sc['NCTA'] = sc['NCTA'].replace(-np.inf, 0)

  # map in campaign names
  sc['CampaignCode'] = sc['CampaignCode'].fillna(value=sc['SourceCode'].str[:8])
  sc['CampaignName'] = sc['CampaignCode'].map(appealsToCampaigns)

  # fix fiscal years
  fym = schema['firstMonthFiscalYear']
  sc['AppealMonth'] = sc['CampaignCode'].str[-4:-2].astype(float)
  sc['AppealYear'] = (2000 + sc['CampaignCode'].str[-2:].astype(float)).astype('Int64')
  sc['AppealYear'] = np.where(sc['AppealMonth']>=fym, sc['AppealYear']+1, sc['AppealYear'])
  sc['Fiscal'] = np.where(sc['Fiscal'].isna(), sc['AppealYear'], sc['Fiscal']).astype(int)

  # drop helper columns
  cols = ['AppealMonth', 'AppealYear']
  sc = sc.drop(cols, axis=1)

  # add data through
  sc['DataThrough'] = data_through

  # drop unwanted source codes
  mask = sc['SourceCode'].str.startswith('QQ')
  sc = sc[mask]

  # add Unsourced to whitemail list names
  codes = ['LLLL', 'XX']
  pattern = '|'.join(codes)
  condition = sc['SourceCode'].str.contains(pattern).fillna(value=False)
  sc['ListName'] = np.where(condition, 'Unsourced', sc['ListName'])
  
except Exception as e:
  raise e
  
print(sc.shape)
print(sc['SourceCode'].nunique())
sc.head()

# COMMAND ----------

# write and stage files
try:
  # write to ADLS2
  sc.to_csv(os.path.join(filemap.CURATED, 'MC_BrokerReport.csv'), index=False)
  
  # write temp file to cluster
  file_name = "MC_BrokerReport.csv"
  sc.to_csv(os.path.join('/tmp', file_name), index=False)
  
except Exception as e:
  raise e

# COMMAND ----------

# write flat file to SharePoint
try:
  # transfer the files to the ClientFiles site
  relative_url = '%s/Reporting 2.0' %client
  client_context = get_sharepoint_context_app_only(CLIENT_SITE_URL)
  res = transfer_from_cluster(client_context, relative_url, file_name)
  print(res)
# except ClientRequestException as e:
#   print(e)
except Exception as e:
  raise e

# COMMAND ----------

# forward data to client team
try:

  subject = 'New Broker Report'
  body = "Hello-\nPlease see the attached refreshed broker report.\nThanks,\n%s" % USER
  attachments = [os.path.join('/tmp', file_name)]

  recipients, bcc = data_team_recipients()
  
  send_email(USER, recipients+bcc, subject, body, attachments, PW)
    
except Exception as e:
  raise e

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))