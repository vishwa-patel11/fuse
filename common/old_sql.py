# Databricks notebook source
# MAGIC %md
# MAGIC ## TMP
# MAGIC Move these to server after upgrading pandas

# COMMAND ----------

# DBTITLE 1,Install MS ODBC SQL Driver (Old)

# %sh
# curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# sudo apt-get update
# sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17


# COMMAND ----------

# DBTITLE 1,Install MS ODBC SQL Driver (New)
# MAGIC %sh
# MAGIC set -e
# MAGIC
# MAGIC apt-get update
# MAGIC apt-get install -y curl gnupg apt-transport-https ca-certificates unixodbc unixodbc-dev
# MAGIC
# MAGIC # Add Microsoft signing key (modern method)
# MAGIC curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg
# MAGIC
# MAGIC # Microsoft repo for Ubuntu 24.04 (noble)
# MAGIC echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/24.04/prod noble main" \
# MAGIC   > /etc/apt/sources.list.d/microsoft-prod.list
# MAGIC
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install -y msodbcsql18
# MAGIC
# MAGIC python - <<'PY'
# MAGIC import pyodbc
# MAGIC print("Installed drivers:", pyodbc.drivers())
# MAGIC PY
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC set -x
# MAGIC cat /etc/os-release || true
# MAGIC whoami || true
# MAGIC id || true
# MAGIC which apt-get || true
# MAGIC apt-get --version || true

# COMMAND ----------

# DBTITLE 1,Upgrade Pandas
pip install --upgrade pandas

# COMMAND ----------

# DBTITLE 1,Upgrade Pyarrow
pip install --upgrade pyarrow

# COMMAND ----------

# MAGIC %md
# MAGIC ##Connect
# MAGIC

# COMMAND ----------

# DBTITLE 1,Shared Params
secret = dbutils.secrets.get(scope='fuse-etl-key-vault', key='msdbiadmin')
server='msdbi.database.windows.net'
database='msdbidb'
jdbcUrl = "jdbc:sqlserver://{server}:1433;database={db};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=10;".format(server=server, db=database)

params={
  "url": jdbcUrl,
  "username": "MSDbiAdmin",
  "password": secret,
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "odbc_driver": '{ODBC Driver 18 for SQL Server}',
  "server": server,
  "database": database,
  "port":1433
}

# COMMAND ----------

# DBTITLE 1,connection
# def connect_sqlalchemy(appname=None,autocommit=False): 8/22/25 commented out
#   from sqlalchemy import create_engine
#   from sqlalchemy.engine import URL
  
#   connection_url = URL.create(
#     "mssql+pyodbc",
#     username=params['username'],
#     password=params['password'],
#     host=params['server'],
#     port=1433,
#     database=params['database'],
#     query={
#         "driver": "ODBC Driver 17 for SQL Server",
#         "timeout": "60",        # Set command timeout to 60 seconds
#         "loginTimeout": "30"    # Set login timeout to 30 seconds
#     },
#   )
  
#   if autocommit:
#     conn = create_engine(connection_url, connect_args={"autocommit": True})
#   else:
#     conn = create_engine(connection_url)
#   return conn

def connect_sqlalchemy(appname=None, autocommit=False): #UPdated version with fast_executemany added 8/22/25
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    connection_url = URL.create(
        "mssql+pyodbc",
        username=params['username'],
        password=params['password'],
        host=params['server'],
        port=1433,
        database=params['database'],
        query={
            "driver": "ODBC Driver 18 for SQL Server",
            "timeout": "60",
            "loginTimeout": "30"
        },
    )

    # always enable fast_executemany for bulk inserts
    engine_args = {
        "fast_executemany": True,  # <-- key performance boost
        "pool_pre_ping": True,
        "future": True,
    }

    if autocommit:
        engine_args["connect_args"] = {"autocommit": True}

    conn = create_engine(connection_url, **engine_args)
    return conn

def connect_pyodbc(appname=None):
  import pyodbc
  conn = pyodbc.connect('DRIVER='+params['odbc_driver']+';SERVER='+params['server']+';DATABASE='+params['database']+';UID='+params['username']+';PWD='+ params['password'] )
  return conn

def connect_spark(appname=None):
  from pyspark.sql import SparkSession
  conn = SparkSession.builder.appName(appname).getOrCreate()
  return conn

def connection(exec_method='sqlalchemy', appname='query'):
  import sys

  conn = getattr(sys.modules[__name__], "connect_%s" % exec_method)(appname)
  return conn, params

# COMMAND ----------

# MAGIC %md
# MAGIC ##Execute

# COMMAND ----------

# DBTITLE 1,sql_exec_only
# client = 'AFHU'
# query = "delete from etl2_column_qa where client = '{cl}'".format(cl=client)
def sql_exec_only(query, appname=None):
  import pandas as pd
  from sqlalchemy import text
  from sqlalchemy.exc import SQLAlchemyError  # SQLAlchemy exception handling

  engine, x = connection('sqlalchemy',appname)
  with engine.connect() as conn: # conn = engine.connect()
    try:
      cursor_result = conn.execute(text(query))
      # Check if the result has rows before fetching
      if cursor_result.returns_rows:  # Only if rows are returned
        df = pd.DataFrame(data=cursor_result.fetchall(), columns=cursor_result.keys())
        return df
      conn.commit()
    except SQLAlchemyError as e:
      print(repr(e))
      raise Exception(repr(e))


# COMMAND ----------

# DBTITLE 1,sql (old 2.10.26)
# # query = "select * from fh_report_periods where client = 'HKI'"
# def sql(query, exec_method='sqlalchemy', appname = None):
#   import pandas as pd
#   if appname is None:
#     appname = query[:50]

#   # if there is no 'select' statement in the query, automatically go to exec only
#   if 'select' not in query:
#     sql_exec_only(query,appname)
#     return None
#   # otherwise, try to read as a query and return a table of results
#   else:
#     conn, x = connection(exec_method, appname)  
#     try:
#         df = pd.read_sql(query, conn)
#         return df
#     except Exception as e:  # try once more just in case there was a select statement but there are no end results.
#         print(repr(e))
#         query = query + "; select 1 status"
#         try:
#           df = pd.read_sql(query, conn)
#           return df
#         except Exception as e:
#           raise Exception(repr(e))
#           print(repr(e))
#   conn.close()


# COMMAND ----------

# DBTITLE 1,sql (new)
# query = "select * from fh_report_periods where client = 'HKI'"
def sql(query, exec_method='sqlalchemy', appname=None):
    import pandas as pd
    from sqlalchemy import text

    if appname is None:
        appname = query[:50]

    # If there is no SELECT statement, treat as exec-only
    if 'select' not in query.lower():
        sql_exec_only(query, appname)
        return None

    # Otherwise, run as a query and return results
    engine, _ = connection(exec_method, appname)  # for sqlalchemy this is an Engine

    try:
        with engine.connect() as c:
            return pd.read_sql(text(query), c)

    except Exception as e:
        # try once more (your original fallback behavior)
        print(repr(e))
        query2 = query + "; select 1 status"
        try:
            with engine.connect() as c:
                return pd.read_sql(text(query2), c)
        except Exception as e2:
            raise Exception(repr(e2))


# COMMAND ----------

# DBTITLE 1,sql_update
def sql_update(query, exec_method='sqlalchemy', appname=None):
    import pandas as pd

    if appname is None:
        appname = query[:50]

    # non-select → exec-only
    if 'select' not in query.lower():
        sql_exec_only(query, appname)
        return None

    engine, x = connection(exec_method, appname)

    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn)

    except Exception as e:
        print(repr(e))
        query = query + "; select 1 status"
        try:
            with engine.connect() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            raise Exception(repr(e))

# COMMAND ----------

# DBTITLE 1,sql_spark_pd
def sql_spark(query, appname="sql_spark"):
    spark, props = connection(exec_method="spark", appname=appname)

    sdf = (spark.read.format("jdbc")
           .option("url", props["url"])
           .option("driver", props["driver"])
           .option("user", props["username"])
           .option("password", props["password"])
           .option("query", query)
           .load())
    return sdf.toPandas()

# COMMAND ----------

# DBTITLE 1,sql_spark
def sql_spark(query, appname="sql_spark"):
    spark, props = connection(exec_method="spark", appname=appname)

    sdf = (spark.read.format("jdbc")
           .option("url", props["url"])
           .option("driver", props["driver"])
           .option("user", props["username"])
           .option("password", props["password"])
           .option("query", query)
           .load())
    return sdf

# COMMAND ----------

# DBTITLE 1,Needs clean up: Updating pyspark dtypes
from pyspark.sql.types import *

def get_schema_from_df(pandas_df):
  structure_dict = {
    'object':StringType(),
    'int': IntegerType(),
    'float': FloatType(),
    'date': TimestampType(),
    'datetime64[ns]': TimestampType(),
    'float64':FloatType(),
    'Int32': IntegerType(),
    'Int64': IntegerType(),
    'datetime64[ns, US/Eastern]': TimestampType(),
    'datetime64[us]': TimestampType(),
    'bool': BooleanType()
  }
  defn = pd.DataFrame(pandas_df.dtypes).reset_index().rename(columns={'index': 'col', 0:'type'})

  schema = StructType()

  for row,iterrow in defn.iterrows():
    translated_type = structure_dict.get(str(iterrow['type']))
    if translated_type is None:
      if 'date' in str(iterrow['type']):
        translated_type = TimestampType()
      elif 'int' in str(iterrow['type']):
        translated_type = IntegerType()
      elif 'time' in str(iterrow['type']):
        translated_type = TimestampType()
      else:
        translated_type = StringType()
    schema.add(StructField(iterrow['col'], 
                          translated_type, 
                          nullable=True))

  return schema


def convert_pyspark_voids(pandas_df, pyspark_df):
  orig_dtypes = pd.DataFrame(pandas_df.dtypes).reset_index().rename(columns={'index': 'col', 0:'type'})
  new_dtypes = pd.DataFrame(pyspark_df.dtypes).rename(columns={0:'col',1:'type'})

  comb = pd.merge(orig_dtypes, new_dtypes, how='outer',on='col', suffixes=['_orig','_new'])
  voids = comb[comb['type_new']=='void']

  for row,iterr in voids.iterrows():
    # Choose the new dtype method
    if iterr['type_orig']=='object':
      cast_to=StringType()
    elif 'int' in iterr['type_orig']: # not tested
      cast_to=IntegerType()
    elif 'float' in iterr['type_orig']: # not tested
      cast_to=FloatType()
    elif 'date' in iterr['type_orig']: # not tested
      cast_to=TimestampType()
    else:
      cast_to=StringType()

    pyspark_df = pyspark_df.withColumn(iterr['col'], pyspark_df[iterr['col']].cast(cast_to))

  return pyspark_df


# COMMAND ----------

# DBTITLE 1,def create_sql_table (LEGACY)
# """
# table_name = 'temp'
# definition_as_dictionary = {'col':'object'}
# """

# dtype_dict = {
#   'object':'varchar(max)',
#   'datetime64[ns]':'datetime',
#   'int': 'int',
#   'Int64': 'int',
#   'float':'float',
#   'Float64':'float',
#   'bool':'bit'
# }

# def create_sql_table(table_name, definition_as_dictionary, primary_key=None):
#   cmd = '''drop table if exists {tbl};
#   create table {tbl} 
#   ('''.format(tbl=table_name)
#   for x in definition_as_dictionary.items():
#     #nn = ' not null' if x[0]==primary_key else ''
#     dtype = dtype_dict.get(x[1])
#     dtype = 'varchar(100)' if dtype is None else dtype
#     dtype = 'bigint' if x[0].endswith('_key') else dtype
#     cmd = cmd +(x[0]+' '+dtype+',')
  
#   cmd=cmd[:-1]+''');
#   '''
#   try:
#     #print(cmd)
#     sql_exec_only(cmd)
#   except Exception as e:
#     print(repr(e))
#     raise

#   # Key
#   if primary_key is not None:
#     add_pk(table_name, primary_key)

# COMMAND ----------

# DBTITLE 1,def create_sql_table NEW
def create_sql_table(table_name, definition_as_dictionary, primary_key=None):
    dtype_dict = {
        'object': 'varchar(255)',
        'datetime64[ns]': 'datetime',
        'int': 'int',
        'Int64': 'int',
        'float': 'float',
        'Float64': 'float',
        'bool': 'bit'
    }

    # --- Debug: show incoming schema ---
    print(f"\n🧩 Creating table: {table_name}")
    print(f"📦 Definition dict received: {definition_as_dictionary}")
    print(f"⚙️ Keys: {list(definition_as_dictionary.keys())}")

    if not definition_as_dictionary:
        raise ValueError(f"❌ No columns provided for {table_name}")

    # --- Build column definitions ---
    cols_sql = []
    for col, dtype in definition_as_dictionary.items():
        dtype_str = str(dtype).strip() if isinstance(dtype, str) else dtype
        sql_type = dtype_dict.get(dtype_str, 'varchar(100)')
        if col.endswith('_key'):
            sql_type = 'bigint'
        cols_sql.append(f"[{col}] {sql_type}")

    # --- Assemble CREATE TABLE SQL ---
    create_sql = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        {', '.join(cols_sql)}
    );
    """

    # --- Clean and debug SQL string ---
    create_sql = " ".join(create_sql.split())  # flatten newlines/whitespace
    print("\n🧾 Final SQL to execute:\n", create_sql, "\n")

    try:
        sql_exec_only(create_sql)
        print(f"✅ Table {table_name} created successfully.")
    except Exception as e:
        print("❌ Error creating table:")
        print(create_sql)
        raise

    # --- Add primary key if defined ---
    if primary_key:
        pk_dtype = dtype_dict.get(definition_as_dictionary.get(primary_key), 'varchar(100)')
        if primary_key.endswith('_key'):
            pk_dtype = 'bigint'

        alter_not_null = f"ALTER TABLE {table_name} ALTER COLUMN [{primary_key}] {pk_dtype} NOT NULL;"
        add_pk = f"ALTER TABLE {table_name} ADD PRIMARY KEY ([{primary_key}]);"

        print(f"\n🔑 Adding primary key: {primary_key}")
        print("⚙️ PK column dtype:", pk_dtype)
        print("🧾 PK SQL:")
        print(alter_not_null)
        print(add_pk)

        try:
            sql_exec_only(alter_not_null)
            sql_exec_only(add_pk)
            print(f"✅ Primary key [{primary_key}] added to {table_name}")
        except Exception as e:
            print(f"⚠️ Error adding primary key {primary_key} to {table_name}: {e}")


# COMMAND ----------

# DBTITLE 1,def add_pk
'''
primary_key = 'donor_id'
table_name = 'test_donor'
'''
def add_pk(table_name, primary_key):
  script = '''
  select DATA_TYPE + case when CHARACTER_MAXIMUM_LENGTH is null then '' else ' (' + cast(character_maximum_length as varchar)+')' end dt
  from information_schema.columns 
  where table_name = '{tbl}' and column_name = '{pk}'
  '''.format(pk = primary_key, tbl=table_name)
  dtype = sql(script)['dt'][0]
  #dtype = dtype_dict.get(definition_as_dictionary.get(primary_key))
  #dtype = 'varchar(100)' if dtype is None else dtype
  #dtype = 'bigint' if x[0].endswith('_key') else dtype
  cmd1 = 'alter table {tbl} alter column {k} {dtype} not null;'.format(tbl=table_name, k=primary_key, dtype=dtype)
  cmd2 = 'alter table {tbl} add primary key ({k});'.format(tbl=table_name, k=primary_key, dtype=dtype)
  
  try:
    sql_exec_only(cmd1)
    sql_exec_only(cmd2)
  except Exception as e:
    print(repr(e))

def drop_pk(table_name, primary_key):
  script = '''
  declare @name VARCHAR(100)
  declare @script varchar(max)

  SELECT @name = CONSTRAINT_NAME
  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
  WHERE TABLE_NAME = '{tbl}' AND CONSTRAINT_TYPE = 'PRIMARY KEY'
  set @script = 'ALTER TABLE {tbl} DROP CONSTRAINT ' + QUOTENAME(@name) + ';'
  exec(@script)

  select @name = DATA_TYPE + case when CHARACTER_MAXIMUM_LENGTH is null then '' else ' (' + cast(character_maximum_length as varchar)+')' end
  from information_schema.columns 
  where table_name = '{tbl}' and column_name = '{key}'
  set @script = 'alter table {tbl} alter column {key} '+@name+' null'
  exec(@script)
  '''.format(tbl=table_name, key=primary_key)
  sql_exec_only(script)


# COMMAND ----------

# DBTITLE 1,sql_import

def sql_import(df, table_name, overwrite_or_append='overwrite'):
  engine,params = connection(exec_method='sqlalchemy', appname='import '+table_name)
  exists = 'append' if overwrite_or_append == 'append' else 'replace'
  formatted = database_headers(df)
  formatted.to_sql(table_name, con=engine, if_exists=exists, index=False)

# to do: add "nan"/null handling so sql server receives null instead of the hardcoded text
def sql_import_pyspark(df, table_name, overwrite_or_append='overwrite', appname=None):
  import pandas as pd 
  if appname is None:
    appname = 'import {tbl}'.format(tbl=table_name)
  try:
    spark, props = connection(exec_method='spark', appname=appname)
    formatted = database_headers(df)
    
    # change int cols that contain nulls to float (pyspark can't handle)
    ints = ['int', 'Int32', 'Int64']
    int_cols = formatted.select_dtypes(include=ints).columns.tolist()
    null_cols = formatted[int_cols].columns[formatted[int_cols].isnull().any()].tolist()
    formatted[null_cols]=formatted[null_cols].astype(float)

    schema = get_schema_from_df(formatted)
    psdf = spark.createDataFrame(formatted, schema=schema)
    #try: # this should now be handled by passing the schema definition directly
    #  psdf = convert_pyspark_voids(formatted, psdf)
    #except Exception as e:
    #  print(repr(e))

    psdf.write.format("jdbc") \
      .mode(overwrite_or_append) \
      .option("url", props['url']) \
      .option("driver", props['driver']) \
      .option("user",props['username']) \
      .option("password", props['password']) \
      .option("dbtable", table_name) \
      .save()
  except Exception as e:
    print(repr(e))
    raise Exception(repr(e))


# COMMAND ----------

# template_table_name = 'template_donor' new_table_name = 'test_donor'
def create_table_from_template(template_table_name, new_table_name):
  query = '''
  drop table if exists {new_table_name};
  select * into {new_table_name} from {template_table_name} where 1=0;
  '''.format(new_table_name=new_table_name, template_table_name=template_table_name)
  sql_exec_only(query)

# COMMAND ----------

# DBTITLE 1,publish_to_curated
# table_name = 'njh_gift'
def publish_to_curated(table_name,drop_original=True):
  query = '''
  drop table if exists curated.{tbl};
  select * 
  into curated.{tbl}
  from dbo.{tbl}
  ;'''.format(tbl=table_name)

  print(query)
  sql_exec_only(query)

  if drop_original==True:
    query = 'drop table if exists dbo.{tbl};'.format(tbl=table_name)
    sql_exec_only(query)

# COMMAND ----------

# DBTITLE 1,format headers
def pretty_headers(df):
    # Pretty colnames
    df.columns = df.columns.str.replace("_", " ")
    df.columns = df.columns.str.title()
    return df

def underscore_camel_case(df):
    import re
    df.columns = pd.Series(df.columns).apply(lambda x: re.sub(r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))', r'\1_', x))
    return df

def remove_client_prefix(column_list, client):
    import re
    new_cols = [re.sub('' + re.escape(client.lower()+'_'), '', x) for x in column_list]
    new_cols = [re.sub('' + re.escape(client.upper()+'_'), '', x) for x in new_cols]
    return new_cols

def database_headers(df):
    import numpy as np
    df_new = df.copy()
    df_new = underscore_camel_case(df_new)
    cols = pd.Series(df_new.columns)
    cols = cols.str.lower()
    symbols = ['~', ':', "'", '+', '[', '\\', '@', '^', '{', '%', '(', '-', '"', '*', '|', ',', '&', '<', '`', '}', '.', '=', ']', '!', '>', ';', '?', '$', ')', '/', ' ', '·']
    for ch in symbols:
        cols = cols.str.replace(ch, '_')
    dict = {
        '#': 'nmb'
    }
    cols.replace(dict, inplace=True, regex=True)
    df_new.columns = cols
    df_new.columns = df_new.columns.str.replace('__','_')
    df_new.columns = np.where(df_new.columns.str[-1:]=='_', df_new.columns.str[:-1], df_new.columns)
    return df_new

# COMMAND ----------

# DBTITLE 1,WIP
# table_name = 'choa_source_code'
# schema_name = 'curated'

def prep_append(df, table_name, schema_name = 'dbo'):
  to_append = database_headers(df)

  # get col info
  query = "select column_name from information_schema.columns where table_name = '{tbl}' and table_schema = '{sch}' order by ORDINAL_POSITION".format(tbl=table_name, sch=schema_name)
  table_cols = sql(query)['column_name'].to_list()

  # grab any columns that are in the data to insert. this will ignore columns that are not in the destination table.
  avail_cols = to_append.columns
  to_use = list(set(avail_cols) & set(table_cols))
  to_append=to_append[to_use].drop_duplicates().reset_index(drop=True)

  # create dataframe that matches the existing sql table schema
  insert_df = pd.DataFrame(columns=table_cols,data=None)
  insert_df = pd.concat([insert_df,to_append])
  sql_import(insert_df, table_name, overwrite_or_append='append')


# COMMAND ----------

# DBTITLE 1,Pyspark

# this is an alternative method that can be explicitly called as needed. It may have performance improvements but is not as generally flexible as pyodbc.

# query = 'select * from njh_fh'
"""
def sql_pyspark(query, appname=None):
  import pandas as pd 
  if appname is None:
    appname = query[:50]
  spark, props = connection(exec_method='spark', appname=appname)

  pydf = spark.read.format("jdbc") \
    .option("url", props['url']) \
    .option("driver", props['driver']) \
    .option("query", query) \
    .option("user",props['username']) \
    .option("password", props['password']) \
    .load()
  
  if pydf.count()>0:
    df = pydf.toPandas()
    return df
  else:
    print('No results to return')
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##TMP !!!
# MAGIC Move these items back to their original home scripts after pandas is upgraded and we no longer require temporary in script pandas upgrades for sqlalchemy compatibility

# COMMAND ----------

# DBTITLE 1,Client List (Utilities)
def client_list():
  df = sql('select client from client where active = 1')
  active_clients = df['client'].to_list()
  return active_clients

# COMMAND ----------

# DBTITLE 1,ETL2 Status
#client = 'RADY'

import pandas as pd
def etl2_status_clear(client):
  query = "delete from etl2_status where client = '{cl}'".format(cl=client)
  sql_exec_only(query)

def etl2_status_entry(client, step_description):
  import inspect

  # Determine what notebook this is running from
  try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    process_name = notebook_path.split('/')[-1]
  except:
    process_name = 'Unknown'
  
  # Cut down step_description
  step_description = step_description[:100]

  # Insert passed record to table
  ts = pd.Timestamp.now(tz='US/Eastern').strftime("%Y-%m-%d %H:%M:%S")
  df = pd.DataFrame(data=[[client,process_name, step_description,ts]],columns=['client','process','step','ts'])
  df['process'] = df['process'].str[:21] #limit to length of SQL table column
  sql_import(df,'etl2_status','append')
  return df

#etl2_status_entry('XXX',step_description='Table generation started')

# COMMAND ----------

