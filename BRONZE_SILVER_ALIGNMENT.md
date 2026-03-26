# Bronze ↔ Silver Layer Alignment Report

## Bronze Layer (from Bronze_layer config)

| Source File | Bronze Table | Catalog | Schema |
|-------------|--------------|---------|--------|
| Data.parquet | **gift_bronze** | dev_catalog | {client}.lower() |
| SourceCode.csv | sourcecode_bronze | dev_catalog | {client}.lower() |
| Budget.csv | budget_bronze | dev_catalog | {client}.lower() |

**Full table path:** `{catalog}.{client_schema}.{client_schema}_{table}`  
Example: `dev_catalog.wfp.wfp_gift_bronze`

---

## Critical Fix Required

**load_parquet** (Data.parquet) was reading from `transactions_bronze` but Bronze writes Data.parquet to **gift_bronze**.

**Fix:** load_parquet must use table_type `'gift'` to read from gift_bronze.

---

## Alignment Checklist

| Component | Bronze | Silver (Base Table Gen) | Status |
|-----------|--------|------------------------|--------|
| Raw data (Data.parquet) | gift_bronze | load_parquet → process_transactions | ✅ Fixed |
| Source code | sourcecode_bronze | load_sc → sc_load_and_process | ✅ Aligned |
| Budget | budget_bronze | load_budget → budget_load_and_process | ✅ Aligned |
| Catalog | dev_catalog | bronze_catalog, silver_catalog in config | ✅ Aligned |
| Schema | client.lower() | client_schema: null → uses client | ✅ Aligned |

---

## Silver Tables Created by Base Table Generation

| Table | Layer | Created By |
|-------|-------|------------|
| {client}_source_code | dbo → curated | sc_load_and_process_csv |
| {client}_transactions | dbo | process_transactions (intermediate) |
| {client}_gift | dbo → curated | process_gift_table |
| {client}_donor | dbo → curated | process_donor_table |
| {client}_budget | dbo → curated | process_budget_table |

---

## Column Name Consideration

Bronze `clean_column_names` replaces spaces/special chars with underscores (e.g. "Gift Date" → "Gift_Date").  
Base Table Generation expects GiftDate, gift_date, etc. If Data.parquet has "GiftDate" (no space), no mapping needed. If column names differ, add mapping in load_parquet.

---

## Prerequisites Before Running

1. **Bronze tables exist** for each client: gift_bronze, sourcecode_bronze, budget_bronze
2. **Metadata tables** exist: etl2_status, max_gift_dates, etl2_qa_results, etl2_column_qa, client
3. **Template tables** exist: template_gift, template_source_code, template_donor, template_budget
4. **config/etl_config.json** has correct schema_root, clients, table_suffix_map
