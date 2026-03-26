# Databricks notebook source
# MAGIC %md
# MAGIC ##Imports

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,sql
# MAGIC %run ../common/sql

# COMMAND ----------

# DBTITLE 1,utilities
# MAGIC %run /Workspace/../python_modules/utilities

# COMMAND ----------

# MAGIC %run ../python_modules/email

# COMMAND ----------

# DBTITLE 1,imports
import ast
import gc

# COMMAND ----------

# DBTITLE 1,Logging
# Set up logging
import logging
import traceback

log_file = '/dbfs/mnt/msd-datalake/logs/etl2.log'
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
# logging.shutdown()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# DBTITLE 1,Manual Run Variables
### CLIENTS TO RUN 
# Use only if you want to manually run just a subset. If blank, all will be attempted
manual_run_client_list = ['FWW']

### SETTINGS
# Set to True if you'd like to completely skip that process.
skip_base_table_gen = False
skip_fh = False

# COMMAND ----------

# DBTITLE 1,* "Full ETL Process Manual Run" Notebook Parameters  *
### Manual Run Job dbutils ###
# Read incoming clients from parent notebook and append to manual if manually run
try:
    incoming_clients = ast.literal_eval(dbutils.widgets.get("clients"))
except:
    incoming_clients = []

manual_run_client_list += incoming_clients

# Read force process flags from parent notebook and change if manually run (for FH only)
try:
    force_fh_process = dbutils.widgets.get("force_fh_process") == "True"
except:
    force_fh_process = False

# COMMAND ----------

# DBTITLE 1,Automated Settings
### IGNORE MANUAL ENTRY IF THIS PROCESS IS CALLED FROM WORKFLOW
try:
  is_workflow = (dbutils.widgets.get("workflow")=='True')
except:
  is_workflow = False

### CLIENTS TO RUN 
if (len(manual_run_client_list)==0) | (is_workflow):
  # Config-driven client list when in workflow; fallback to client_list() from DB
  config_clients = get_clients_from_config()
  if config_clients:
    clients = config_clients
  else:
    clients = client_list()

  # Do not run list
  removes = ['DAV', 'DAV2', 'SEVA', 'TSE', 'UPMC', 'SICL','HFHI','CCHMC']
  clients = list(set(clients)-set(removes))
  
  # Run alphabetically
  clients.sort()
else:
  clients = manual_run_client_list

# Put MC last
if 'MC' in clients:
  clients = list(set(clients)-set(['MC']))
  clients = clients+['MC']

### SETTINGS
skip_fh = False if force_fh_process else skip_fh

logger.warning('***********************************************')
logger.warning('***** Loop Starting')
logger.warning('***********************************************')
logger.warning('***** Query live progress at dbo.etl2_status')
logger.warning('***** Clients to run')
logger.warning(clients)
logger.warning('***** Settings')
if skip_base_table_gen:
  base = 'Skipped'
else:
  base = 'Attempt if Needed (always run when not skipped)'

if force_fh_process:
  fh = 'Forced'
elif skip_fh:
  fh = 'Skipped'
else:
  fh = 'Attempt if Needed'

print('Base Table Processing: '+str(base))
print('File Health Processing: '+str(fh))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

# DBTITLE 1,call_base_table_gen
def call_base_table_gen(client):
  finished = 0
  logger.warning('*** Base Table Gen: Start')
  notebook = '/Workspace/Shared/fuse/production_etl/Base Table Generation'
  try:
    args={
      'client':str([client])
    }
    # Run
    etl2_status_entry(client,'Base Table Gen: Start')
    run_notebook(notebook, (60*60*5), args=args)
    etl2_status_entry(client,'Base Table Gen: Complete')
    finished = 1
  except Exception as e:
    logger.error(client+" Base Table Gen: %s", traceback.format_exc())
    etl2_status_entry(client,'Base Table Gen: FAILURE - '+repr(e))
  logger.warning('*** Base Table Gen: Complete')
  return finished
    


# COMMAND ----------

# DBTITLE 1,call_fh
# client = 'AFHU'
def call_fh(client,force_fh_process):
  finished = 0
  logger.warning('*** FH: Start')
  notebook = '/Workspace/Shared/fuse/production_etl/File Health'
  schema = get_schema(client)
  if 'FileHealth' in schema:
    try:
      # Set Up
      args={
        'client':str([client]),
        'force_fh_process':str(force_fh_process)
        }
      # Run
      etl2_status_entry(client,'FH: Start')
      run_notebook(notebook, (60*60*3), args=args)
      etl2_status_entry(client,'FH: Complete')
      finished = 1
    except Exception as e:
      logger.error(client+" File Health: %s", traceback.format_exc())
      etl2_status_entry(client,'FH: FAILURE - '+repr(e))
  else:
    logger.warning('Client is not configured for FileHealth in their schema')
    etl2_status_entry(client,'FH: Client is not configured for FileHealth in their schema')
  logger.warning('*** FH: Complete')
  return finished

# COMMAND ----------

# DBTITLE 1,Notifications
def notification_qa_results(client):
  # Query 
  script = '''
  select check_level, dataset, "check", error_description, r.ts, fail
  from etl2_qa_results r 
  --join etl2_status es on r.client = es.client and es.step = 'Table generation started' and r.ts >= es.ts -- Results are since most recent run started. This can be uncommented once everyone is comfortable with the process and wants to reduce noise.
  where r.client = '{cl}' and fail = 1 and check_level = '{lvl}'
  order by r.ts desc
  '''

  # Query each check_level type and turn the results from a dataframe to an html table
  err = sql(script.format(cl=client, lvl='error'))
  error_table = inline_formatter(err)
  warn = sql(script.format(cl=client, lvl='warning'))
  warning_table = inline_formatter(warn)

  # Create the HTML Structure
  body = """
  <h1>ETL2 QA Results: {cl}</h1>
  <h2>Errors</h2>
  {err_table}<br>
  <br>
  <h2>Warnings</h2>
  {warn_table}<br>
  <br>
  <p>Quick Links</p>
  <ul>
    <li><a href="https://app.powerbi.com/groups/c2eaa33b-1a9d-4b4d-b11c-9ba2a8c043d2/reports/2c82f7a6-3169-4fe6-8af3-1e7a87bed334/046d6a6eabcb49580ed8?experience=power-bi">ETL2 Monitoring</a></li>
    <li><a href="https://adb-1969403299592450.10.azuredatabricks.net/editor/notebooks/3327395016494432?o=1969403299592450#command/3498571594386095">Base Table Generation</a></li>
    <li><a href="https://adb-1969403299592450.10.azuredatabricks.net/editor/notebooks/333345960673760?o=1969403299592450#command/1889050292677254">File Health</a></li>
  </ul>
  """.format(cl=client, err_table=error_table, warn_table=warning_table)

  # Apply style from shared css
  body = inline_formatter(body)

  # Send email
  to, cc = data_team_recipients()
  send_email_from_html(to, "{cl} QA Results".format(cl=client), body)

def notification_exception_raised(client, code_location, error_message):
  # Add to ETL2 Monitoring
  etl2_status_entry(client,'[RAISED ERROR] {loc}: {msg}'.format(loc=code_location, msg=error_message))

  # Send Email
  body = """
  <h1>ETL2 Error: {cl}</h1>
  <p>{loc}: {e}</p>
  <br>
  <p>Quick Links</p>
  <ul>
    <li><a href="https://adb-1969403299592450.10.azuredatabricks.net/jobs/223948621911322?o=1969403299592450">Process ETL2 Job</a></li>
    <li><a href="https://adb-1969403299592450.10.azuredatabricks.net/editor/notebooks/2741135447638261?o=1969403299592450#command/1970987630118817">ETL2 Code</a></li>
    <li><a href="https://portal.azure.com/#@fusefundraising.com/resource/subscriptions/af3498c3-b201-46bc-b478-f6d117ee11b4/resourceGroups/MSD_BI/providers/Microsoft.Storage/storageAccounts/msdstore/storagebrowser">Logs (Right click in the file row whitespace -> View/edit)</a></li>
  </ul>
  """.format(cl=client, loc=code_location, e=error_message)

  # Apply style from shared css
  body = inline_formatter(body)

  # Send email
  to, cc = data_team_recipients()
  send_email_from_html(to, "{cl} ETL2 Error".format(cl=client), body)

# COMMAND ----------

# MAGIC %md
# MAGIC # Processing

# COMMAND ----------

# DBTITLE 1,Budget
etl2_status_clear('all')
try:
  notebook = '/Workspace/Shared/fuse/production_etl/Budget'
  run_notebook(notebook, (60*120))
  etl2_status_entry('all','Budget Updated')
  logger.warning("Budget complete")
except Exception as e:
  print(repr(e))
  logger.error("Budget: %s", traceback.format_exc())

  
  # # Send email
  # to, cc = data_team_recipients()
  # send_email_from_html(to, "{cl} Budget Not Updated".format(cl=client), repr(e))

# COMMAND ----------

clients

# COMMAND ----------

# DBTITLE 1,Client Loop
start_ts = datetime.now()
for client in clients:
  logger.warning('****************************')
  logger.warning('***** '+(client))
  logger.warning('****************************')
  # client = 'AFHU'
  etl2_status_clear(client)
  base_finished = 0
  fh_finished = 0
  try:
    if skip_base_table_gen==False:
      base_finished = call_base_table_gen(client)
    if skip_fh==False:
      fh_finished = call_fh(client,force_fh_process)
    if (base_finished==1 or fh_finished==1):
      notification_qa_results(client)
  except Exception as e:
    print(repr(e))
    logger.error(client+": %s", traceback.format_exc())
    notification_exception_raised(client, code_location='Client Loop', error_message=repr(e))
  
  # garbage
  gc.collect()

  # close log
  logging.shutdown()