# Databricks notebook source
# MAGIC %md
# MAGIC # Create Metadata Tables
# MAGIC Run this **once** before the first Base Table Gen run to create all metadata tables in `{metadata_catalog}.{metadata_schema}`.
# MAGIC
# MAGIC **Tables created:**
# MAGIC - etl2_status
# MAGIC - etl2_qa_results
# MAGIC - etl2_column_qa
# MAGIC - max_gift_dates
# MAGIC - client
# MAGIC - fh_report_periods
# MAGIC
# MAGIC **Seeding:** client, max_gift_dates, and fh_report_periods are seeded with rows for each client from `config/etl_config.json` so that `client_list()`, `update_max_date`, and File Health work on first run.

# COMMAND ----------

# MAGIC %run ../common/sql

# COMMAND ----------

import json
import os

# Resolve schema path (from config or workspace defaults)
def _get_metadata_schema_path():
    cfg = _load_etl_config()
    root = cfg.get('schema_root', '/Workspace/Shared/schema')
    path = os.path.join(root, 'metadata_tables.json')
    if os.path.exists(path):
        return path
    for p in ['/Workspace/Shared/schema/metadata_tables.json', '/Workspace/Repos/Shared/schema/metadata_tables.json']:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"metadata_tables.json not found. Set schema_root in config.")

metadata_schema_path = _get_metadata_schema_path()

with open(metadata_schema_path) as f:
    metadata_schemas = json.load(f)

# COMMAND ----------

for table_name, definition in metadata_schemas.items():
    try:
        create_sql_table(table_name, definition)
        print(f"Created: {table_name}")
    except Exception as e:
        print(f"Error creating {table_name}: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed client, max_gift_dates, fh_report_periods
# MAGIC Inserts initial rows for clients from config so that:
# MAGIC - `client_list()` returns clients (ETL2 fallback when config empty)
# MAGIC - `update_max_date` / `update_max_gift_date` can UPDATE existing rows on first run
# MAGIC - File Health has at least one period per client (placeholder 'ALL'; replace with real periods if needed)

# COMMAND ----------

clients = get_clients_from_config()
if not clients:
    print("No clients in config. Skipping seed.")
else:
    client_tbl = get_metadata_table_path('client')
    mgd_tbl = get_metadata_table_path('max_gift_dates')
    spark = _get_spark()

    for c in clients:
        cl = str(c).strip() if isinstance(c, str) else str(c)
        # Seed client (MERGE: insert if not exists)
        try:
            spark.sql(f"""
              MERGE INTO {client_tbl} AS t
              USING (SELECT '{cl}' AS client, 1 AS active) AS s
              ON t.client = s.client
              WHEN NOT MATCHED THEN INSERT (client, active) VALUES (s.client, s.active)
            """)
            print(f"Seeded client: {cl}")
        except Exception as e:
            print(f"Client seed warning for {cl}: {e}")

        # Seed max_gift_dates (MERGE: insert if not exists)
        try:
            spark.sql(f"""
              MERGE INTO {mgd_tbl} AS m
              USING (SELECT '{cl}' AS client) AS s
              ON m.client = s.client
              WHEN NOT MATCHED THEN INSERT (client) VALUES (s.client)
            """)
            print(f"Seeded max_gift_dates: {cl}")
        except Exception as e:
            print(f"max_gift_dates seed warning for {cl}: {e}")

        # Seed fh_report_periods (MERGE: insert if not exists) - File Health needs rows to loop over
        fhrp_tbl = get_metadata_table_path('fh_report_periods')
        try:
            spark.sql(f"""
              MERGE INTO {fhrp_tbl} AS t
              USING (SELECT '{cl}' AS client, 'ALL' AS period, CAST(NULL AS TIMESTAMP) AS max_gift_date) AS s
              ON t.client = s.client AND t.period = s.period
              WHEN NOT MATCHED THEN INSERT (client, period, max_gift_date) VALUES (s.client, s.period, s.max_gift_date)
            """)
            print(f"Seeded fh_report_periods: {cl}")
        except Exception as e:
            print(f"fh_report_periods seed warning for {cl}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC All metadata tables have been created and seeded. You can now run Base Table Generation.
