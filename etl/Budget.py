# Databricks notebook source
# MAGIC %run ../common/sql

# COMMAND ----------

# MAGIC %run /Workspace/Shared/python_modules/transfer_files

# COMMAND ----------

# DBTITLE 1,Download & Move
# point to the individual log file
client_context = get_sharepoint_context_app_only("https://mindsetdirect.sharepoint.com/sites/ClientDataShare")

file_name = '/sites/ClientDataShare/Shared%20Documents/Krause%20Analytics/Client%20Budgets.xlsx'
source_file = client_context.web.get_file_by_server_relative_url(file_name)
local_fn = '/dbfs/tmp/tmp_budgets.xlsx'
execute_transfer(source_file, local_fn)

df = pd.read_excel(local_fn)

# 
import numpy as np

df['Campaign'] = np.where(df['TMP Mapping Attempt'].isna(), df['Campaign'], df['TMP Mapping Attempt'])
df.rename(columns={'Campaign':'campaign_name'},inplace=True)
df.drop(columns=['TMP Mapping Attempt'],inplace=True)

# Data types 
float_cols = ['Budget Mail Quantity', 'Budget Gifts', 'Budget Gross $', 'Budget Total Costs', 'Reforecast Mail Quantity', 'Reforecast Gifts', 'Reforecast Gross $', 'Reforecast Total Costs']

for col in float_cols:
  print(col) #col = 'Qty'
  #trim, replace '-'
  df[col]=df[col].astype(str).str.strip().replace('-','').replace(r'^\s*$', np.nan, regex=True).astype(float)

df['data_processed_at']=ts_now()

sql_import(df,'budget')

# COMMAND ----------

df

# COMMAND ----------

# DBTITLE 1,QA
errors = 0 # placeholder

if errors == 0:
  publish_to_curated('budget')

# COMMAND ----------

