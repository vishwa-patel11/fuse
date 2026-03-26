# Databricks notebook source
# MAGIC %run ../common/sql

# COMMAND ----------

# MAGIC %run ../common/utilities

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# DBTITLE 1,def add_to_error_table
'''
check = 'check'
dataset = 'dataset'
error_description = 'desc'
fail = 0
check_level = 'testing'
'''
def add_to_error_table(client, check, dataset, error_description, fail, check_level):
  tbl = get_metadata_table_path('etl2_qa_results')
  # Escape single quotes in error_description for SQL
  err_esc = str(error_description).replace("'", "''")
  sql_exec_only(f"DELETE FROM {tbl} WHERE client = '{client}' AND dataset = '{dataset}' AND \"check\" = '{check}'")
  sql_exec_only(f"INSERT INTO {tbl} SELECT '{client}', '{dataset}', '{check}', '{err_esc}', {int(fail)}, current_timestamp(), '{check_level}'")



# COMMAND ----------

# DBTITLE 1,def query_qa_errors
# client = 'FWW'
def query_qa_errors(client, datasets=None):
  tbl = get_metadata_table_path('etl2_qa_results')
  qa_results = sql(f"SELECT * FROM {tbl} WHERE client = '{client}' AND fail = 1")

  # datasets specified
  if datasets is not None: #datasets=['Transactions.parquet','Gift']
    qa_results=qa_results[qa_results['dataset'].isin(datasets)]

  warnings = qa_results[qa_results['check_level']=='warning']['check'].count()
  errors = qa_results[qa_results['check_level']=='error']['check'].count()
  
  return warnings, errors

# COMMAND ----------

# DBTITLE 1,def check_pii
def check_pii(client):
  schema = get_schema(client)
  trx = load_parquet(client)
  # check cols 
  still_present = list(set(schema['PII_Columns']) & set(trx.columns))
  add_to_error_table(client, 'PII', 'Data.parquet', 'PII column(s) still present: '+', '.join(still_present), len(still_present)>0)

# COMMAND ----------

"""
data = load_parquet(client)
parq_ts = data['data_processed_at'].max()

trx = load_transactions(client)
trx = trx['data_processed_at'].max()

cp = load_cp(client)
cp['data_processed_at'].max()

fh_script = 'select max(data_processed_at) from {client}_fh'.format(client=client)"""