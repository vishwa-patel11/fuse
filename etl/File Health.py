# Databricks notebook source
# MAGIC %md #File Health (ETL2 Production)

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %run ../common/sql

# COMMAND ----------

# DBTITLE 1,TMP: Upgrade Pandas
pip install --upgrade pandas

# COMMAND ----------

# DBTITLE 1,TMP
pip install fastparquet

# COMMAND ----------

# DBTITLE 1,qa
# MAGIC %run ../fuse/python_modules/qa

# COMMAND ----------

# DBTITLE 1,transfer_files
# MAGIC %run ../python_modules/transfer_files

# COMMAND ----------

# DBTITLE 1,utilities
# MAGIC %run ../python_modules/utilities

# COMMAND ----------

# DBTITLE 1,mount_datalake
# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# DBTITLE 1,email
# MAGIC %run ../python_modules/email

# COMMAND ----------

import os
import numpy as np
import pandas as pd
import datetime 
import warnings
from dateutil.relativedelta import relativedelta

warnings.filterwarnings("ignore")

# COMMAND ----------

# DBTITLE 1,def notification_exception_raised
def notification_exception_raised(client, code_location, error_message):
  # Add to ETL2 Monitoring
  etl2_status_entry(client,'[RAISED ERROR] {loc}: {msg}'.format(loc=code_location, msg=error_message))

  # Send Email
  body = """
  <h1>File Health Error: {cl}</h1>
  <p>{loc}: {e}</p>
  <br>
  <p>Quick Links</p>
  <ul>
    <li><a href="https://adb-1969403299592450.10.azuredatabricks.net/editor/notebooks/333345960673760?o=1969403299592450">File Health Code</a></li>
    <li><a href="https://portal.azure.com/#@fusefundraising.com/resource/subscriptions/af3498c3-b201-46bc-b478-f6d117ee11b4/resourceGroups/MSD_BI/providers/Microsoft.Storage/storageAccounts/msdstore/storagebrowser">Logs (Right click in the file row whitespace -> View/edit)</a></li>
  </ul>
  """.format(cl=client, loc=code_location, e=error_message)

  # Apply style from shared css
  body = inline_formatter(body)

  # Send email
  to, cc = data_team_recipients()
  send_email_from_html(to, "{cl} File Health Error".format(cl=client), body)

# COMMAND ----------

# DBTITLE 1,*Set Client
import ast
try:
  client = ast.literal_eval(dbutils.widgets.get('client'))[0]
  print(client)
  force_fh_process = ast.literal_eval(dbutils.widgets.get('force_fh_process'))
  print(force_fh_process)
except Exception as e:
  print(repr(e))
  client = 'FS' # change this to manually run
  force_fh_process = True
  print(client)

# COMMAND ----------

# DBTITLE 1,*Set do_not_run_fh_client_list
#Add clients to do_not_run_client_list
do_not_run_fh_client_list = ['HKI', 'MC']


# COMMAND ----------

# DBTITLE 1,Logging
# Set up logging
import logging
import traceback
log_file = '/dbfs/mnt/msd-datalake/logs/{client}_file_health_processing.log'.format(client=client)

logger = logging.getLogger()
file_handler = logging.FileHandler(log_file, mode='w')
file_handler.setLevel(logging.WARNING)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

# COMMAND ----------

# MAGIC %md
# MAGIC # File Health Processing

# COMMAND ----------

# DBTITLE 1,Spark Functions
def register_spark_functions():
  spark.sql("""
  CREATE OR REPLACE TEMPORARY FUNCTION fiscal_from_date(date_val DATE, fym INT)
  RETURNS INT
  RETURN CASE 
      WHEN fym = 1 THEN YEAR(date_val)
      WHEN MONTH(date_val) < fym THEN YEAR(date_val)
      ELSE YEAR(date_val) + 1
  END
  """)

# COMMAND ----------

# DBTITLE 1,def process_file_health
def process_file_health(client, period):
  """
  Reads SQL from file, injects parameters, and executes.
  """
  # 1. Resolve Path
  BASE_DIR = os.path.dirname(os.path.dirname(__file__))
  SQL_PATH = os.path.join(BASE_DIR, "stored_procedures", "etl2_file_health.sql")

  # 2. Register Functions
  register_spark_functions()
  
  # 3. Read SQL
  with open(SQL_PATH, 'r') as f:
    sql_template = f.read()
  
  # 4. Inject Parameters
  sql_final = sql_template.format(client=client, period=period)
  
  # 5. Execute as a single call
  spark.sql(sql_final)

# COMMAND ----------

def process_fh(client):
  logger.warning('*** FH: Starting Proc')

  # Prep
  spark.sql(f"DELETE FROM dev_catalog.metadata.etl2_status WHERE client = '{client}' AND process = 'File Health'")

  # Pull which time periods we should loop through for this client
  query = f"SELECT * FROM dev_catalog.metadata.fh_report_periods WHERE client = '{client}'"
  tbl = spark.sql(query).toPandas()
  
  # Loop & Execute processing 
  for period in tbl['period']:
    logger.warning(period)
    process_file_health(client, period)
  
  logger.warning('*** FH: Proc Complete')

# COMMAND ----------

# DBTITLE 1,def fh_qa
# client = 'AFHU'

def fh_qa(client):
  etl2_status_entry(client,'FH: SP Started')
  # Exec QA stored procedure
  script = "exec etl2_file_health_qa @client='{cl}'".format(cl=client.lower())
  sql_exec_only(script, appname=client+'fh qa') 
  etl2_status_entry(client,'FH: SP Complete')

  # Pull qa results
  warnings, errors = query_qa_errors(client,['File Health'])
  logger.warning('Warnings: '+str(warnings))
  logger.warning('Errors: '+str(errors))
  etl2_status_entry(client,'FH: QA Complete ({err} errors)'.format(err=errors))
  
  return warnings, errors
  logger.warning('*** FH: QA Complete')

# COMMAND ----------

# DBTITLE 1,hold: older result store

def get_fh_results(client):
  # Ingest the group file from sql
  filemap = Filemap(client.upper())
  output_file = filemap.CURATED+client+'_FH_v2024.csv'
  query = f"SELECT * FROM {get_curated_table_path(client.lower() + '_fh_group')}"

  try:
    conn, params = connection(exec_method='sqlalchemy')
    fh_output = pd.DataFrame(data=None)
    df_reader = pd.read_sql(query, con=conn, chunksize=10**6) # we're using an iterator over chunks to handle performance for large datasets like MC
    for chnk in df_reader:
      df = pd.DataFrame(data=chnk)
      fh_output = pd.concat([fh_output,df])
    logger.warning('*** FH: Results imported back to python')
  except Exception as e:
    logger.warning(repr(e))
    logger.error("get_fh_results: %s", traceback.format_exc())
  
  return fh_output, output_file

def publish_to_datalake(fh_output, output_file):
  try:
    fh_output.to_csv(output_file)
    logger.warning('*** FH: Published to datalake')
  except Exception as e:
    logger.warning(repr(e))
    logger.error("publish_to_datalake: %s", traceback.format_exc())
    notification_exception_raised(client, code_location='publish_to_datalake', error_message=repr(e))
    raise
  
def publish_to_sharepoint(fh_output, output_file):
  # Publish to ClientFiles sharepoint
  file_name = os.path.basename(output_file)
  try:
    xl = file_name #.replace('.csv','.xlsx') # xlsx is pretty expensive
    fh_output.to_csv('/tmp/'+xl) #to_excel('/tmp/'+xl)
    client_context = get_sharepoint_context("https://mindsetdirect.sharepoint.com/sites/ClientFiles")
    relative_url = '{cl}/Reporting 2.0'.format(cl=client)
    upload_to_sharepoint(client_context, relative_url, xl)
    os.remove('/tmp/'+xl)
    logger.warning('*** FH: Published to SharePoint')
  except Exception as e:
    logger.warning(repr(e))

# COMMAND ----------

# DBTITLE 1,def publish_fh
# client = 'AFHU'

def publish_fh(client):
  ### Publish to curated DB schema 
  # Promote from dbo silver to curated silver using utility
  publish_to_curated(f"{client.lower()}_fh_group", drop_original=False)
  publish_to_curated(f"{client.lower()}_fh_date", drop_original=False)
  logger.warning('*** FH: Moved to Curated DB Schema')

  ### Update Data Updated date
  script = f"""
  UPDATE {get_metadata_table_path('max_gift_dates')} SET file_health = fh.mx
  FROM (
  	SELECT client, MAX(end_date) as mx
  	FROM {get_metadata_table_path('fh_report_periods')} 
  	GROUP BY client
  ) fh
  WHERE {get_metadata_table_path('max_gift_dates')}.client = fh.client AND fh.client = '{client}'
  """
  sql_exec_only(script, appname=client+'fh updated date') 

  ### Query results
  #fh_output, output_file = get_fh_results(client)

  ### Export Group designations to a csv in the curated datalake
  #publish_to_datalake(fh_output, output_file)
  
  ### Export Group designations to sharepoint
  #publish_to_sharepoint(fh_output, output_file)



# COMMAND ----------

# MAGIC %md # Where FH Runs

# COMMAND ----------

# DBTITLE 1,*Set run_fh_process to True/False
if force_fh_process:
  # If Force is true then Run
  run_fh_process=True
  etl2_status_entry(client,'File Health: Force Process')
else:
  run_fh_process = False
  etl2_status_entry(client,'File Health: Check if to run')
  # Otherwise, decide if the run should happen based on the dates
  try:
    # Get the max FH date available today and add month
    script = f"SELECT MAX(date) as mx_fh_date FROM {get_curated_table_path(client.lower() + '_fh_date')}"
    result_fh_date = spark.sql(script).toPandas()
    current_fh_date = result_fh_date['mx_fh_date'][0]
    next_month_fh_date = result_fh_date['mx_fh_date'][0] + relativedelta(months=1)


    # Get the max processed date time from curated gift table
    gift_script = f"SELECT MAX(gift_date) as mx_gift_date FROM {get_dbo_table_path(client.lower() + '_gift')}"
    result_gift = spark.sql(gift_script).toPandas()
    mx_gift_date = result_gift['mx_gift_date'][0]


    # Compare and decide if run is needed if gift max date is more than 1 month from to curated.fh_date max
    run_fh_process = (mx_gift_date.year, mx_gift_date.month) > (next_month_fh_date.year, next_month_fh_date.month)
    print(f"run_fh_process:{run_fh_process}")


    # # Get the max processed date time from curated FH date table
    # script_fh_processed = 'select max(data_processed_at) mx_processed_fh from curated.{cl}_fh_date'.format(cl=client.lower())
    # result_fh_processed = sql(script_fh_processed)
    # current_fh_processed_date = result_fh_processed['mx_processed_fh'][0]

    # # Get the max processed date time from curated gift table
    # gift_script = 'select max(data_processed_at) mx_processed_gift from curated.{cl}_gift'.format(cl=client.lower())
    # result_gift = sql(gift_script)
    # mx_gift_processed_date = result_gift['mx_processed_gift'][0]

    # # Compare and decide if run is needed if the curated gift processed datetime is greater than the curated fh_date processed datetime
    # run_fh_process = mx_gift_processed_date > current_fh_processed_date

    # Share results
    logger.warning('FH: Max Date in Curated FH_Date Table: '+str(current_fh_date))
    etl2_status_entry(client,'FH: Max Date in Curated FH_Date Table: '+str(current_fh_date))
    logger.warning('FH: Max Gift Date Curated Gift table: '+str(mx_gift_date))
    etl2_status_entry(client,'FH: Max Gift Date Curated Gift table: '+str(mx_gift_date))
    email_contents = '''
    Max Processed in in Curated FH Table: {dt1}
    Max Processed in Curated Gift Table: {dt2}
    '''.format(dt1=str(current_fh_date ), dt2=str(mx_gift_date))
  except Exception as e:
    etl2_status_entry(client,'FH: '+str(repr(e)))
    logger.error("Cannot Determine FH Run: %s", traceback.format_exc())
    to, cc = data_team_recipients()
    send_email_from_html(to=to, subject=client+': Cannot Determine FH Run', contents = repr(e))

logger.warning('FH: Run Process='+str(run_fh_process))
etl2_status_entry(client,'FH: Run Process='+str(run_fh_process))

#Set run_fh_process to False if client in do not run list
if client in do_not_run_fh_client_list:
  run_fh_process = False
  logger.warning('FH: Run Process='+str(run_fh_process)+' - Client in Do Not Run List')

  

# COMMAND ----------

# DBTITLE 1,*Process FH (if run_fh_process True)
#Run FH process if run_fh_process is True
print(f"FH: Run Process = {run_fh_process}")
if run_fh_process:
  try:
    # Process Table
    process_fh(client)

    # QA Results
    warnings, errors = fh_qa(client)

    # Publish if they pass QA
    if errors == 0:
      publish_fh(client)
  
  except ValueError as ve:
    logging.error("run_fh_process ValueError: %s", ve)
  
  except Exception as e:
    logging.error("run_fh_process: %s", traceback.format_exc())
    notification_exception_raised(client, code_location='File Health', error_message=repr(e))
    raise

# COMMAND ----------

# DBTITLE 1,Housekeeping
# garbage clean up
import gc
gc.collect()

# close log
logging.shutdown()

# COMMAND ----------



# COMMAND ----------

