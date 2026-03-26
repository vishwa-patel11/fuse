# Databricks notebook source
# MAGIC %md
# MAGIC ## Base Table Generation - Client-Specific Logic (AFHU style)
# MAGIC Functions named `{Client}_{hook}`. Dispatch via `apply_client_*` helpers.
# MAGIC Add new clients by defining e.g. `def WFP_source_code_missing_transform(missing, client): ...`

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dispatch helpers

# COMMAND ----------

def _get_base_table_fn(client, fn_suffix):
    """Get client-specific base table function from clients.{client}.base_table or globals."""
    try:
        from common.clients_loader import get_client_base_table_fn
        fn = get_client_base_table_fn(client, fn_suffix)
        if fn is not None:
            return fn
    except ImportError:
        pass
    return globals().get(f"{client}_{fn_suffix}")


def apply_client_source_code_transform(sc, client):
    fn = _get_base_table_fn(client, "source_code_transform")
    return fn(sc, client) if callable(fn) else sc


def apply_client_source_code_missing_transform(missing, client):
    fn = _get_base_table_fn(client, "source_code_missing_transform")
    return fn(missing, client) if callable(fn) else missing


def apply_client_skip_trx_qa(client):
    fn = _get_base_table_fn(client, "skip_trx_qa")
    return fn(client) if callable(fn) else False


def apply_client_skip_gift_qa(client):
    fn = _get_base_table_fn(client, "skip_gift_qa")
    return fn(client) if callable(fn) else False


def apply_client_force_publish_source_code_despite_errors(client):
    fn = _get_base_table_fn(client, "force_publish_source_code_despite_errors")
    return fn(client) if callable(fn) else False


def apply_client_skip_cp_filters(client):
    fn = _get_base_table_fn(client, "skip_cp_filters")
    return fn(client) if callable(fn) else False


def apply_client_after_gift_before_qa(gift, client):
    fn = _get_base_table_fn(client, "after_gift_before_qa")
    if callable(fn):
        fn(gift, client)


def apply_client_donor_post_process(donor, trx, client):
    fn = _get_base_table_fn(client, "donor_post_process")
    return fn(donor, trx, client) if callable(fn) else donor


def apply_client_donor_csl(donor, client):
    fn = _get_base_table_fn(client, "donor_csl")
    return fn(donor, client) if callable(fn) else donor


def apply_client_after_publish_bi_tables(client):
    fn = _get_base_table_fn(client, "after_publish_bi_tables")
    if callable(fn):
        fn(client)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FFB - moved to clients/FFB/base_table.py

# COMMAND ----------

# MAGIC %md
# MAGIC ## WFP - moved to clients/WFP/base_table.py

# COMMAND ----------

# MAGIC %md
# MAGIC ## DAV2, USO, DAV, MC, SICL, NJH - moved to clients/{Client}/base_table.py
