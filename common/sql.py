# Databricks notebook source
# MAGIC %md
# MAGIC ## SQL - Databricks / Delta
# MAGIC Uses Spark/Delta. No DROP/TRUNCATE - overwrite mode handles data replacement.

# COMMAND ----------

# DBTITLE 1,Imports & Config
import os
import json
import pandas as pd
import numpy as np

_ETL_CONFIG_PATH = '/Workspace/Shared/config/etl_config.json'

def _load_etl_config():
  try:
    path = _ETL_CONFIG_PATH
    if path.startswith('dbfs:/') or path.startswith('abfss://'):
      try:
        content = dbutils.fs.head(path, 65535)
        return json.loads(content)
      except (NameError, Exception):
        pass
    if os.path.exists(path):
      with open(path, 'r') as f:
        return json.load(f)
    for path in [
        '/Workspace/Repos/Shared/config/etl_config.json',
        '/Workspace/Shared/config/etl_config.json',
        'config/etl_config.json',
        os.path.join(os.getcwd(), 'config', 'etl_config.json'),
    ]:
      if os.path.exists(path):
        with open(path, 'r') as f:
          return json.load(f)
  except Exception as e:
    print(f"ETL config load warning: {e}")
  return {
    'metadata_catalog': 'dev_catalog',
    'metadata_schema': 'metadata',
    'metadata_tables': ['etl2_status', 'client', 'max_gift_dates', 'etl2_qa_results', 'etl2_column_qa', 'fh_report_periods'],
    'metadata_table_prefixes': ['template_'],
  }

def _get_spark():
  from pyspark.sql import SparkSession
  return SparkSession.builder.getOrCreate()

def _get_metadata_table_path(table_name):
  cfg = _load_etl_config()
  catalog = cfg.get('metadata_catalog', 'dev_catalog')
  schema = cfg.get('metadata_schema', 'metadata')
  return f"{catalog}.{schema}.{table_name}"

def get_metadata_table_path(table_name):
  return _get_metadata_table_path(table_name)

def get_clients_from_config():
  """Return client list from config. Empty list if not configured."""
  cfg = _load_etl_config()
  return cfg.get('clients') or []

def _is_metadata_table(table_name):
  if not table_name or '.' in table_name:
    return False
  cfg = _load_etl_config()
  metadata_tables = tuple(cfg.get('metadata_tables') or [])
  prefixes = tuple(cfg.get('metadata_table_prefixes') or [])
  if table_name in metadata_tables:
    return True
  return any(table_name.startswith(p) for p in prefixes)

def _is_client_silver_table(table_name):
  if not table_name or '.' in table_name:
    return False
  if _is_metadata_table(table_name):
    return False
  return '_' in table_name

def _get_client_from_table_name(table_name):
  base = table_name.split('.')[-1]
  return base.split('_')[0].lower() if '_' in base else 'default'

def _client_silver_path(table_name, layer):
  cfg = _load_etl_config()
  catalog = cfg.get('silver_catalog', cfg.get('metadata_catalog', 'dev_catalog'))
  base = table_name.split('.')[-1]
  client = _get_client_from_table_name(table_name)
  schema = cfg.get('client_schema', client)
  prefix = 'dbo' if layer == 'dbo' else 'curated'
  tbl = f"{prefix}_{base}_silver" if not base.endswith('_silver') else f"{prefix}_{base}"
  return f"{catalog}.{schema}.{tbl}"

def _resolve_table_path(table_name):
  if '.' in table_name:
    return table_name
  if _is_client_silver_table(table_name):
    return _client_silver_path(table_name, 'dbo')
  return _get_metadata_table_path(table_name)

# COMMAND ----------

# DBTITLE 1,sql_exec_only / sql
def sql_exec_only(query, appname=None):
  try:
    spark = _get_spark()
    sdf = spark.sql(query)
    return sdf.toPandas()
  except Exception as e:
    print(repr(e))
    raise

def sql(query, exec_method='sqlalchemy', appname=None):
  if appname is None:
    appname = query[:50]
  if 'select' not in query.lower():
    sql_exec_only(query, appname)
    return None
  try:
    spark = _get_spark()
    sdf = spark.sql(query)
    return sdf.toPandas()
  except Exception as e:
    print(repr(e))
    raise

# COMMAND ----------

# DBTITLE 1,get_schema_from_df
from pyspark.sql.types import *

def get_schema_from_df(pandas_df):
  structure_dict = {
    'object': StringType(),
    'int': IntegerType(),
    'int64': LongType(),
    'float': FloatType(),
    'date': TimestampType(),
    'datetime64[ns]': TimestampType(),
    'float64': FloatType(),
    'Int32': IntegerType(),
    'Int64': LongType(),
    'datetime64[ns, US/Eastern]': TimestampType(),
    'datetime64[us]': TimestampType(),
    'bool': BooleanType()
  }
  defn = pd.DataFrame(pandas_df.dtypes).reset_index().rename(columns={'index': 'col', 0: 'type'})
  schema = StructType()
  for row, iterrow in defn.iterrows():
    translated_type = structure_dict.get(str(iterrow['type']))
    if translated_type is None:
      if 'date' in str(iterrow['type']):
        translated_type = TimestampType()
      elif 'int' in str(iterrow['type']):
        translated_type = LongType()
      elif 'time' in str(iterrow['type']):
        translated_type = TimestampType()
      else:
        translated_type = StringType()
    schema.add(StructField(iterrow['col'], translated_type, nullable=True))
  return schema

# COMMAND ----------

# DBTITLE 1,create_sql_table
def create_sql_table(table_name, definition_as_dictionary, primary_key=None):
  dtype_map = {
    'object': 'STRING',
    'datetime64[ns]': 'TIMESTAMP',
    'int': 'INT',
    'Int64': 'BIGINT',
    'Int32': 'INT',
    'float': 'DOUBLE',
    'Float64': 'DOUBLE',
    'bool': 'BOOLEAN',
  }
  if not definition_as_dictionary:
    raise ValueError(f"No columns provided for {table_name}")
  full_name = _get_metadata_table_path(table_name) if '.' not in table_name else table_name
  cols_sql = []
  for col, dtype in definition_as_dictionary.items():
    dtype_str = str(dtype).strip() if isinstance(dtype, str) else dtype
    sql_type = dtype_map.get(dtype_str, 'STRING')
    if col.endswith('_key'):
      sql_type = 'BIGINT'
    cols_sql.append(f"  `{col}` {sql_type}")
  create_sql = f"CREATE TABLE IF NOT EXISTS {full_name} (\n" + ",\n".join(cols_sql) + "\n) USING DELTA"
  try:
    spark = _get_spark()
    spark.sql(create_sql)
    print(f"Table {full_name} created successfully.")
  except Exception as e:
    print(f"Error creating table: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,create_table_from_template (no DROP/TRUNCATE)
def create_table_from_template(template_table_name, new_table_name):
  """Create Delta table from template if not exists. No DROP/TRUNCATE - overwrite mode handles data replacement."""
  spark = _get_spark()
  template_path = _get_metadata_table_path(template_table_name) if '.' not in template_table_name else template_table_name
  new_path = _resolve_table_path(new_table_name) if '.' not in new_table_name else new_table_name
  try:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {new_path} AS SELECT * FROM {template_path} WHERE 1=0")
    print(f"Table {new_path} ensured (CREATE IF NOT EXISTS).")
  except Exception as e:
    try:
      schema_df = spark.sql(f"DESCRIBE TABLE {template_path}")
      cols = [r for r in schema_df.collect() if r.col_name and r.col_name != '']
      col_defs = [f"  `{r.col_name}` {r.data_type}" for r in cols]
      spark.sql(f"CREATE TABLE IF NOT EXISTS {new_path} (\n" + ",\n".join(col_defs) + "\n) USING DELTA")
      print(f"Table {new_path} ensured (CREATE IF NOT EXISTS from schema).")
    except Exception as e2:
      print(f"create_table_from_template: {e2}")
      raise

# COMMAND ----------

# DBTITLE 1,sql_import
def sql_import(df, table_name, overwrite_or_append='overwrite'):
  formatted = database_headers(df)
  mode = 'append' if overwrite_or_append == 'append' else 'overwrite'
  full_name = _resolve_table_path(table_name)
  ints = ['int', 'Int32', 'Int64']
  int_cols = formatted.select_dtypes(include=ints).columns.tolist()
  null_cols = [c for c in int_cols if formatted[c].isnull().any()]
  if null_cols:
    formatted = formatted.copy()
    formatted[null_cols] = formatted[null_cols].astype(float)
  spark = _get_spark()
  schema = get_schema_from_df(formatted)
  sdf = spark.createDataFrame(formatted, schema=schema)
  sdf.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(full_name)

# COMMAND ----------

# DBTITLE 1,get_dbo_table_path / get_curated_table_path / publish_to_curated
def get_dbo_table_path(table_name):
  return _client_silver_path(table_name, 'dbo')

def get_curated_table_path(table_name):
  return _client_silver_path(table_name, 'curated')

def publish_to_curated(table_name, drop_original=False):
  dbo_path = get_dbo_table_path(table_name)
  curated_path = get_curated_table_path(table_name)
  spark = _get_spark()
  try:
    df = spark.table(dbo_path)
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(curated_path)
  except Exception as e:
    print(f"publish_to_curated: {e}")
    raise
  if drop_original:
    spark.sql(f"DROP TABLE IF EXISTS {dbo_path}")

# COMMAND ----------

# DBTITLE 1,drop_pk (no-op for Delta)
def drop_pk(table_name, primary_key):
  """No-op for Delta: Delta tables don't use SQL Server-style constraints."""
  pass

# COMMAND ----------

# DBTITLE 1,database_headers
def underscore_camel_case(df):
  import re
  df.columns = pd.Series(df.columns).apply(lambda x: re.sub(r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))', r'\1_', x))
  return df

def remove_client_prefix(column_list, client):
  import re
  new_cols = [re.sub('' + re.escape(client.lower() + '_'), '', x) for x in column_list]
  new_cols = [re.sub('' + re.escape(client.upper() + '_'), '', x) for x in new_cols]
  return new_cols

def database_headers(df):
  df_new = df.copy()
  df_new = underscore_camel_case(df_new)
  cols = pd.Series(df_new.columns)
  cols = cols.str.lower()
  symbols = ['~', ':', "'", '+', '[', '\\', '@', '^', '{', '%', '(', '-', '"', '*', '|', ',', '&', '<', '`', '}', '.', '=', ']', '!', '>', ';', '?', '$', ')', '/', ' ', '·']
  for ch in symbols:
    cols = cols.str.replace(ch, '_')
  cols.replace({'#': 'nmb'}, inplace=True, regex=True)
  df_new.columns = cols
  df_new.columns = df_new.columns.str.replace('__', '_')
  df_new.columns = np.where(df_new.columns.str[-1:] == '_', df_new.columns.str[:-1], df_new.columns)
  return df_new

# COMMAND ----------

# DBTITLE 1,etl2_status_entry
def etl2_status_entry(client, step_description):
  try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    process_name = notebook_path.split('/')[-1]
  except Exception:
    process_name = 'Unknown'
  step_description = step_description[:100]
  ts = pd.Timestamp.now(tz='US/Eastern').strftime("%Y-%m-%d %H:%M:%S")
  df = pd.DataFrame(data=[[client, process_name[:21], step_description, ts]], columns=['client', 'process', 'step', 'ts'])
  sql_import(df, 'etl2_status', 'append')
  return df
