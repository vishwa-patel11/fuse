# Databricks notebook source
# MAGIC %md
# MAGIC # Create MC Monthly Master Files

# COMMAND ----------



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

# MAGIC %md
# MAGIC ## Sharepoint Setup

# COMMAND ----------

# DBTITLE 1,Set Sharepoint Settings
# SharePoint settings
DATA_SHARE_URL = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"

filemap = Filemap('Fuse')
with open(os.path.join(filemap.SCHEMA, 'schema.json'), 'r') as f:
  USER = json.load(f)['SharePointUID']
PW = dbutils.secrets.get(scope='fuse-etl-key-vault', key='sharepoint-pwd')


# COMMAND ----------

filemap = Filemap('MC')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Files

# COMMAND ----------

# Get file names
ctx = get_sharepoint_context_app_only(DATA_SHARE_URL)
client_files = []
client_files = get_all_contents(ctx, 'Shared Documents/MC/Monthly Files')

# COMMAND ----------

# Reading in individual files from ZIPs

import os
import zipfile
import io
import re

# Keywords (group types)
patterns = {
    "email": r"email", 
    "offline": r"offline\s*promo",
    "appeals": r"appeals",
    "account": r"account\s*info"
}

# Where to store matched files by pattern
files_by_pattern = {key: [] for key in patterns}

for zip_path in client_files:
    if not zip_path.lower().endswith('.zip'):
        continue

    print(f"📦 Downloading: {zip_path}")
    
    try:
        # Download file as bytes from SharePoint
        response = File.open_binary(ctx, zip_path)
        bytes_file = io.BytesIO(response.content)
        
        # Open the ZIP from memory
        with zipfile.ZipFile(bytes_file, 'r') as z:
            for file_name in z.namelist():
                lower_name = file_name.lower()
                for key, pattern in patterns.items():
                    if re.search(pattern, lower_name):
                        files_by_pattern[key].append({
                            "zip_file": os.path.basename(zip_path),
                            "internal_file": file_name
                        })
                        break  # prevent duplicates
    except Exception as e:
        print(f"⚠️ Error reading {zip_path}: {e}")

# Print grouped results
for pattern, matches in files_by_pattern.items():
    print(f"\n📁 Files containing '{pattern}': ({len(matches)} found)")
    for m in matches:
        print(f"   {m['zip_file']} → {m['internal_file']}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Offline Promos Master

# COMMAND ----------

# Get all Offline Promo Files
offline_promo_dfs = []

for file_info in files_by_pattern['offline']:
    zip_name = file_info['zip_file']
    internal_file = file_info['internal_file']
    
    try:
        # Download ZIP from SharePoint
        response = File.open_binary(ctx, f"/sites/ClientDataShare/Shared Documents/MC/Monthly Files/{zip_name}")
        bytes_file = io.BytesIO(response.content)
        
        # Open ZIP and read the specific CSV
        with zipfile.ZipFile(bytes_file, 'r') as z:
            with z.open(internal_file) as f:
                df = pd.read_csv(f)
                df['source_zip'] = zip_name  # optional: keep track of source
                df['source_file'] = internal_file
                offline_promo_dfs.append(df)
                
    except Exception as e:
      print(f"⚠️ Error reading {zip_name} → {internal_file}: {e}")

# COMMAND ----------

# Concatenate
offline_promos_master = pd.concat(offline_promo_dfs, ignore_index=True)

# COMMAND ----------

offline_promos_master.head()

# COMMAND ----------

# Deduplicating on Marketing Finding Number

# Sort by date descending so the most recent is first and keeping first duplicate
offline_promos_master = offline_promos_master.sort_values(by='Assigned Appeal Date', ascending=False)
offline_promos_master = offline_promos_master.drop_duplicates(subset=['Marketing Finder Number'], keep='first')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Mercy Corps Accounts Master

# COMMAND ----------

import re
from datetime import datetime

def extract_zip_date(zip_name):
    # Extract digits from name: 'FUSE Data Upload 0725.zip' -> '0725'
    match = re.search(r'(\d{4})', zip_name)
    if match:
        date_str = match.group(1)
        # Assume first two digits = month, last two = year (2-digit)
        month = int(date_str[:2])
        year = int(date_str[2:])
        # Adjust year for 2000+ (if needed)
        year += 2000
        return datetime(year, month, 1)
    else:
        return datetime.min

# Sort account files by ZIP date descending
account_files_sorted = sorted(
    files_by_pattern['account'],
    key=lambda x: extract_zip_date(x['zip_file']),
    reverse=True
)

# Pick the most recent ZIP
most_recent_file_info = account_files_sorted[0]
zip_name = most_recent_file_info['zip_file']
internal_file = most_recent_file_info['internal_file']

print("Most recent account file determined from ZIP name:", zip_name)


# COMMAND ----------

accounts_dfs = []
try:
    # Download ZIP from SharePoint
    response = File.open_binary(ctx, f"/sites/ClientDataShare/Shared Documents/MC/Monthly Files/{zip_name}")
    bytes_file = io.BytesIO(response.content)
    
    # Open ZIP and read the specific CSV
    with zipfile.ZipFile(bytes_file, 'r') as z:
        with z.open(internal_file) as f:
            df = pd.read_csv(f, encoding='latin1', low_memory=False)
            df['source_zip'] = zip_name  # optional: keep track of source
            df['source_file'] = internal_file
            accounts_dfs.append(df)
            
except Exception as e:
  print(f"⚠️ Error reading {zip_name} → {internal_file}: {e}")

# COMMAND ----------

accounts_dfs[0]

# COMMAND ----------

# Concatenate
accounts_master = pd.concat(accounts_dfs, ignore_index=True)

# COMMAND ----------

# DBTITLE 1,Deduplicating on Constituent ID
# Deduplicating on Constituent ID
accounts_master = accounts_master.sort_values(by='Donor_Acquisition_Date', ascending=False)
accounts_master = accounts_master.drop_duplicates(subset=['Constituent_ID'], keep='first')
accounts_master.columns = accounts_master.columns.str.replace('^\ufeff', '', regex=True) # extra column name cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appeals and Campaigns Master

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Emails Master

# COMMAND ----------

# import pandas as pd
# import io
# import zipfile

# # This will store all email dataframes
# email_dfs = []

# for file_info in files_by_pattern['email']:
#     zip_name = file_info['zip_file']
#     internal_file = file_info['internal_file']
    
#     try:
#         # Download ZIP from SharePoint
#         response = File.open_binary(ctx, f"/sites/ClientDataShare/Shared Documents/MC/Monthly Files/{zip_name}")
#         bytes_file = io.BytesIO(response.content)
        
#         # Open ZIP and read the specific CSV
#         with zipfile.ZipFile(bytes_file, 'r') as z:
#             with z.open(internal_file) as f:
#                 df = pd.read_csv(f)
#                 df['source_zip'] = zip_name  # optional: keep track of source
#                 df['source_file'] = internal_file
#                 email_dfs.append(df)
                
#     except Exception as e:
#         print(f"⚠️ Error reading {zip_name} → {internal_file}: {e}")

# # Combine all email files into a single DataFrame if desired
# all_emails = pd.concat(email_dfs, ignore_index=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exporting Master Files

# COMMAND ----------

# DBTITLE 1,Write to Datalake

offline_promos_master.to_csv(os.path.join(filemap.CURATED, "MC_MonthlyMail_Masters", "offline_promos_master.csv"), index=False)
accounts_master.to_csv(os.path.join(filemap.CURATED, "MC_MonthlyMail_Masters", "accounts.csv"), index=False)