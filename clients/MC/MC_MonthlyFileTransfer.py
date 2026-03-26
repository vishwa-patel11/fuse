# Databricks notebook source
# MAGIC %md #MC Monthly File Transfer (ETL1 & ETL2 Production)

# COMMAND ----------

# MAGIC %md ## Imports and Runs

# COMMAND ----------

# DBTITLE 1,Imports
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

from datetime import datetime
import json
import os
import pandas as pd

import subprocess
import shutil

# COMMAND ----------

# DBTITLE 1,mount_datalake
# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# DBTITLE 1,transfer_files
# MAGIC %run ../python_modules/transfer_files

# COMMAND ----------

# DBTITLE 1,utilities
# MAGIC %run ../python_modules/utilities

# COMMAND ----------

# DBTITLE 1,Set Sharepoint Settings
# SharePoint settings
DATA_SHARE_URL = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"

filemap = Filemap('Fuse')
with open(os.path.join(filemap.SCHEMA, 'schema.json'), 'r') as f:
  USER = json.load(f)['SharePointUID']
PW = dbutils.secrets.get(scope='fuse-etl-key-vault', key='sharepoint-pwd')


# COMMAND ----------

# MAGIC %md ## Read from Client Data Share

# COMMAND ----------

# read in reference log
filemap = Filemap('MC')
ref = pd.read_csv(os.path.join(filemap.MASTER, 'Monthly_log.csv'))
ref

# COMMAND ----------

# initiate client context (transfer_files)
ctx = get_sharepoint_context_app_only(DATA_SHARE_URL)

# get site contents (transfer_files)
client_files = []
client_files = get_all_contents(ctx, 'Shared Documents/MC/Monthly Files')

# take only the client files
# client_files = [x for x in client_files if x.split('/')[4] in registered_clients]
print('Number of files: ', len(client_files))

# # convert the new file list to a dataframe and write to ADSL2
df = pd.DataFrame(
  client_files, 
  index=[i for i in range(len(client_files))], 
  columns=['FileName']
  )


# Identify only the new files (utilities)
new_files = identify_new_files(ref, df)
new_files

# COMMAND ----------

if len(new_files) > 0:
  # download the file from sharepoint
  f = list(new_files)[0]
  f = '/'.join(f.split('/')[3:])
  res, client, file_name = download_from_sp(ctx, f)

  # remove whitespace from file names to facilitate linux unzip
  _f = f.split('/')[-1]
  os.rename(os.path.join('/tmp', _f), os.path.join('/tmp', _f.replace(' ','_')))
  _f = _f.replace(' ','_')

  # unzip the file
  src = os.path.join('/tmp', _f)
  dst = os.path.join('/tmp', 'MC')
  command = 'unzip %s -d %s' %(src, dst)
  subprocess.call(command, shell=True)

  # identify the promo file from the unzipped contents
  file_name = [x for x in os.listdir('/tmp/MC') if 'offline promos' in x.lower()][0]

  # transfer to Raw/PromoFiles
  shutil.copyfile(
    os.path.join('/tmp', 'MC', file_name), 
    os.path.join(filemap.RAW, 'PromoFiles', file_name)
    )

  # remove the zip file from the cluster to free resources
  command = 'rm %s' %(os.path.join('/tmp', _f))
  subprocess.call(command, shell=True)

  # remove the folder of unzipped contents
  command = 'rm -r %s' %(os.path.join('/tmp', 'MC'))
  subprocess.call(command, shell=True)
  
  # record the log
  df.to_csv(os.path.join(filemap.MASTER, 'Monthly_log.csv'), index=False)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "timestamp (UTC)": str(datetime.now())
}))