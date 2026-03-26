# Databricks notebook source
# MAGIC %md
# MAGIC # Documentation & Notes
# MAGIC _**Objective**_: Create, QA & publish the base sql tables for use in Power BI and ad hoc querying.
# MAGIC
# MAGIC [Documentation](https://mindsetdirect.sharepoint.com/sites/data-analytics/docs/SitePages/TEST%20ETL2%20Documentation.aspx?login_hint=meghank%40fusefundraising.com)
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

# MAGIC %md
# MAGIC ## Temporary Upgrades
# MAGIC These are being made in session only (vs made as instance wide updates that apply to all processes). These upgrades can have negative impacts if applied globally without testing scripts. For example, pd.append is sunset in the latest pandas version required. Scripts need to be searched for pd.append and updated to use pd.concat before making the upgrade globally. This enables us to buy time and make those adjustments on a rainy day.

# COMMAND ----------

# DBTITLE 1,Temp: Upgrade Pandas
# pip install --upgrade pandas

# COMMAND ----------

# DBTITLE 1,Temp: Upgrade Pyarrow
# pip install --upgrade pyarrow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# DBTITLE 1,Import sql
# MAGIC %run ../common/sql

# COMMAND ----------

# DBTITLE 1,TEST2 create_sql_table Function
import inspect

function_name = create_sql_table

# Replace my_function with the actual function name
print(inspect.getsource(function_name))

# COMMAND ----------

# DBTITLE 1,Import qa
# MAGIC %run ../common/qa

# COMMAND ----------

# DBTITLE 1,Import source_code_validation
# MAGIC %run ../common/source_code_validation

# COMMAND ----------

# DBTITLE 1,Import parser
# MAGIC %run ../common/parser

# COMMAND ----------

# DBTITLE 1,Import utilities
# MAGIC %run ../common/utilities

# COMMAND ----------

# DBTITLE 1,Import email
# MAGIC %run ../common/email

# COMMAND ----------

# DBTITLE 1,Import base_table_clients (AFHU-style client-specific logic)
# MAGIC %run ../common/base_table_clients

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Email Notification Function


# COMMAND ----------

# DBTITLE 1,Main Execution with Error Handling


# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check if processing needs to occur

# COMMAND ----------

# DBTITLE 1,*def set_run_etl2
def _check_if_new_data_available(client): # 
  # Grab the most recent gift date from data.parquet
  trx = load_parquet(client)
  mx_trx = trx['GiftDate'].max()

  # Update most recent data.parquet date
  update_max_date(client, mx_trx, 'data_parquet')

  # Grab the most recent gift date processed into the gift table
  script = 'select max(gift_date) mx_processed from curated.{cl}_gift'.format(cl=client.lower())
  try:
    result = sql(script) 
    run_process = result['mx_processed'][0] < mx_trx
  except:
    run_process = True
  return run_process

def _check_if_tables_are_most_updated_schema(client):
  reprocess = sql_exec_only("exec etl2_base_table_check_schema '{cl}'".format(cl=client))
  try:
    reprocess = reprocess['reprocess'][0]==1
  except:
    reprocess = True
  return reprocess

def set_run_etl2(client):
  # Condition 1: Data is outdated
  cond1 = _check_if_new_data_available(client)
  logger.warning('New Data: '+str(cond1))
  # Condition 2: Schema is outdated
  cond2 = _check_if_tables_are_most_updated_schema(client)
  logger.warning('Schema Outdated: '+str(cond2))
  return max(cond1,cond2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Source Codes
# MAGIC

# COMMAND ----------

# DBTITLE 1,def sc_custom_columns
# def sc_custom_columns(client, sc_df):
#   if client == 'CARE':


# COMMAND ----------

# DBTITLE 1,def sc_load_and_process_csv
"""
client = 'RADY' 
df = load_transactions(client) 
max_gift=df.gift_date.max() 
min_gift=df.gift_date.min()
"""
def sc_load_and_process_csv(client, max_gift, min_gift):
  schema = get_schema(client)

  ################################################
  # Read source code from bronze Delta table (or file fallback)
  ################################################
  sc = load_sc(client)
  if sc is None:
    raise Exception(f"Failed to load SourceCode for client {client}")
  sc = apply_client_source_code_transform(sc, client)

  ################################################
  # Format sourcecode
  ################################################

  ### WORKAROUNDs for inconsistencies
  sc.columns = sc.columns.str.replace(' ', '')

  # Check if all values in 'MailDate' are null
  if sc['MailDate'].isnull().all():
    sc['MailDate'] = ''
  
  # !!! Future: extract campaign_code+ from source_code where we have data holes
  
  # Formats
  sc.CampaignCode = sc.CampaignCode.astype(str)

  # Combine campaign code columns if there are multiple !!! this is a workaround for RADYs sourcecode csv
  if 'CampaignCode' in sc.columns and 'Campaign Code' in sc.columns:
    sc['CampaignCode']=sc['CampaignCode'].fillna(sc['Campaign Code'])
    sc.drop('Campaign Code', axis=1, inplace=True)

  if 'SourceCode' in sc.columns and 'Source Code' in sc.columns:
    sc['SourceCode']=sc['SourceCode'].fillna(sc['Source Code'])
    sc.drop('Source Code', axis=1, inplace=True)

  # add filters that apply only to the dimension (source code) table
  sc = add_dimension_filters(sc, client)

  # Convert MailDate to datetime
  try:
      # Attempt the first method (Initial method writte,)
      sc['MailDate'] = pd.to_datetime(sc['MailDate'].str[:10])  
  except Exception as e: # If this fails, try the alternate method (Applies to NJH, updated 2/11/25)
      print(f"Error encountered: {e}. Falling back to alternate method.")
      # Ensure MailDate is a string and strip spaces
      sc['MailDate'] = sc['MailDate'].astype(str).str.strip()
      # Remove time component if present (keeps only date part)
      sc['MailDate'] = sc['MailDate'].str.split(" ").str[0]
      # Try parsing MM/DD/YYYY first
      maildate_mdY = pd.to_datetime(sc['MailDate'], format="%m/%d/%Y", errors='coerce')
      # Try parsing YYYY-MM-DD next
      maildate_Ymd = pd.to_datetime(sc['MailDate'], format="%Y-%m-%d", errors='coerce')
      # Merge results, prioritizing known formats
      sc['MailDate'] = maildate_mdY.fillna(maildate_Ymd)
      # Convert all valid dates to MM/DD/YYYY format
      sc.loc[sc['MailDate'].notna(), 'MailDate'] = sc['MailDate'].dt.strftime("%m/%d/%Y")

  #Add mail_date_original column before missing are filled and inferences made (Added 3/20/25)
  sc['mail_date_original'] = sc['MailDate']
  #Add fy_mail_date_original column before missing are filled and inferences made (Added 9/9/25)
  sc['fy_mail_date_original'] = fiscal_from_column(sc, 'MailDate', schema['firstMonthFiscalYear'])


  #New Cols
  sc['fy'] = fiscal_from_column(sc, 'MailDate', schema['firstMonthFiscalYear'])
  sc['DaysSinceMailDate'] = (max_gift - sc['MailDate']).dt.days

  #Keep only sourcecodes with mail date (Added 2/24/25 to mirror sc logic in ETL1)
  fy_valid_mask = sc['fy'] > 1900
  sc = sc.loc[fy_valid_mask]

  # Backup Campaign Name
  sc['_CN'] = 'FY' + \
    (sc['fy'].astype(str).replace('<NA>','XX').str[:4].str[-2:]).fillna('XX') + \
    '_' + sc.MailDate.dt.month_name().fillna('') + \
    '_' + sc.PackageName.fillna('')
  sc.CampaignName = sc.CampaignName.fillna(sc._CN)
  sc = sc.drop('_CN', axis=1)

  # --- Misc setup -------------------------------------------------------
  sc["Client"] = client  # intentionally title case

  print("data_processed_at_og_sc started")
  # Ensure data_processed_at exists and is datetime 
  if "data_processed_at" not in sc.columns:
      sc["data_processed_at"] = pd.NaT
  sc["data_processed_at"] = pd.to_datetime(sc["data_processed_at"], errors="coerce")
  #  Fill nulls with the max non-null timestamp or current time 
  if sc["data_processed_at"].notna().any():
      max_ts = sc["data_processed_at"].max()
  else:
      max_ts = pd.to_datetime(ts_now(), errors="coerce")
  sc.loc[sc["data_processed_at"].isna(), "data_processed_at"] = max_ts
  # Create trimmed original timestamp column 
  sc["data_processed_at_og_sc"] = sc["data_processed_at"].astype(str).str[:19]
  print("data_processed_at_og_sc complete")
  # Overwrite data_processed_at with current timestamp 
  sc["data_processed_at"] = ts_now()

  # Add load_source_codes as source 
  sc["source"] = "load_source_codes"
  # Update source if Source Code starts with 'BUDGET_SC' 
  if "SourceCode" in sc.columns:
      mask_budg_sc = sc["SourceCode"].astype(str).str.startswith("BUDGET_SC", na=False)
      sc.loc[mask_budg_sc, "source"] = "budget_missing_sc"

  ### load
  # create dataframe
  sc_standard = pd.DataFrame(data=None,columns=sc_dims)
  sc_standard = sc_standard.astype(sc_def)

  # OPTION 1: THE EXPLICITLY INPUT SOURCECODES
  insert_df = sc.copy()
  insert_df = database_headers(insert_df)
  
  # # ORIGINAL 2/17/25
  # insert_df['client_campaign'] = insert_df['client'].str.upper()+'-'+insert_df['campaign_code'].fillna(insert_df['campaign_name']).fillna(insert_df['source_code']).fillna('')
  
  # #UPDATED 2/18/25 to add 'campaign_name' to client_campaign field with campaign_code. Source_code fallback if both are empty
  # # Create base campaign field without source_code fallback
  # insert_df['client_campaign'] = insert_df['client'].str.upper() + '-' + \
  #     insert_df['campaign_code'].fillna('') + \
  #     insert_df['campaign_name'].where(insert_df['campaign_name'].notna(), '').radd('-').fillna('')
  # # If client_campaign ends with '-' (meaning both campaign_code and campaign_name were missing), replace with source_code
  # insert_df['client_campaign'] = insert_df['client_campaign'].str.rstrip('-')  # Remove any trailing hyphen
  # insert_df.loc[insert_df['client_campaign'] == insert_df['client'].str.upper(), 'client_campaign'] += '-' + insert_df['source_code']

  #UPDATED 2/20/25 to add FY to end of client_campaign if available
  insert_df['client_campaign'] = insert_df['client'].str.upper() + '-' + \
      insert_df['campaign_code'].fillna('') + \
      insert_df['campaign_name'].where(insert_df['campaign_name'].notna(), '').radd('-').fillna('')
  # Remove any trailing hyphen (if both campaign_code and campaign_name were missing)
  insert_df['client_campaign'] = insert_df['client_campaign'].str.rstrip('-')
  # If client_campaign is just the client name, add the source_code
  insert_df.loc[insert_df['client_campaign'] == insert_df['client'].str.upper(), 'client_campaign'] += '-' + insert_df['source_code']
  # Append -FY{fy} if fy is available
  insert_df['client_campaign'] += insert_df['fy'].apply(lambda x: f"-MD_FY{x}" if pd.notna(x) else "")

  ##-------------------------------------------------------------------------------------------------------------
  # #Add "Whitemail: " + client_campign row for every unique client_campaign value
  # # 1) Aggregate by client_campaign using the SAME rules
  # agg_map_whitemail = {
  #     "fy": "max",
  #     "mail_date": "max",
  #     "fy_mail_date_original": "max",
  #     "mail_date_original": "max",
  #     "client": "first",
  #     "campaign_code": "first",
  #     "campaign_name": "first",
  #     "client_campaign": "first",
  # }
  # whitemail_df = (
  #     insert_df.groupby("client_campaign", as_index=False)
  #             .agg(agg_map_whitemail)
  # )
  # # 2) Create the Whitemail source_code
  # whitemail_df["source_code"] = "Whitemail: " + whitemail_df["client_campaign"]
  # # 3) Make sure schema matches insert_df (add any missing cols as None and order columns)
  # for col in insert_df.columns:
  #     if col not in whitemail_df.columns:
  #         whitemail_df[col] = None
  # whitemail_df = whitemail_df[insert_df.columns]
  # #Avoid duplicates if some Whitemail rows already exist
  # mask = ~insert_df["source_code"].eq("Whitemail: " + insert_df["client_campaign"])
  # insert_df = insert_df[mask]
  # # 4) Put Whitemail rows ON TOP of existing insert_df
  # insert_df = pd.concat([whitemail_df, insert_df], ignore_index=True)

  ##--------------------------------------------------------------------------------------------------------------------




  insert_df['source_code_key'], _ = pd.factorize(insert_df['source_code'])

  overlap_cols = list(set(insert_df.columns) & set(sc_dims))
  sc_standard = pd.concat([sc_standard,insert_df[overlap_cols]])

  #Add curated_sc_key
  sc_standard['sc_key_curated'] = (sc_standard['client'].str.upper().fillna('') + '_sc_' + sc_standard['source_code_key'].astype(str).fillna(''))

  # OPTION 3: UNSOURCED FALLBACKS -- update this so the years match with those in gift history, not just sc history. AFHU has 3 yrs of scs but 60 years of gifts
  from datetime import date
  years = pd.DataFrame(range(min_gift.year-1,max_gift.year+1), columns=['fy'])
  years['fy']=years['fy'].astype(str)
  blank = pd.DataFrame(data=['XXXX'],columns=['fy'])
  years = pd.concat([years,blank])

  years['client']=client
  years['source_code']='UNSOURCED-FY'+years['fy']
  years['campaign_code']='UNSOURCED-FY'+years['fy']
  years['campaign_name']='Unsourced FY'+years['fy']
  years['client_campaign']=years['client'].str.upper()+'-UNSOURCED-FY'+years['fy']
  years['source_code_key'], _ = pd.factorize(years['client_campaign'])
  years['mail_date']=years[years['fy']!='XXXX'].apply(lambda x: date(int(x['fy']),schema['firstMonthFiscalYear'],1), axis=1)
  years['fy']=years['fy'].replace('XXXX', None) # set back
  years['source']='unsourced fallbacks'
  #Add curated_sc_key
  years['sc_key_curated'] = (years['client'].str.upper().fillna('') + '_unsourced_' + years['source_code_key'].astype(str).fillna(''))

  overlap=list(set(years.columns) & set(sc_dims))
  sc_standard = pd.concat([sc_standard,years[overlap]])

  # import (overwrite replaces data - no DROP/TRUNCATE needed)
  table_name = client.lower()+'_source_code'
  create_table_from_template('template_source_code', table_name)
  sc_standard['data_processed_at']=ts_now()
  sql_import(sc_standard, table_name, overwrite_or_append='overwrite')
  etl2_status_entry(client,'Source Code Processing: Loaded')
  
  return sc

# COMMAND ----------

# DBTITLE 1,def sc_find_missing

# Goal: Add the attempts to map campaign name into the source code table instead of putting into camp perf
"""
client = 'HKI'
df = load_transactions(client)
sc= sc_load_and_process_csv(client, df.gift_date.max(), df.gift_date.min())
"""
import numpy as np
import pandas as pd

def sc_find_missing(df, sc):
  ### Find campaigns that aren't in SourceCode.csv already
  cols = df.columns[~df.columns.str.upper().str.contains(client.upper())]
  trx = database_headers(df) # ignore FH filters
  sc2 = database_headers(sc) #

  comb = pd.merge(trx, sc2, on='source_code', how='left', suffixes=['','_sc'], indicator=True)
  missing = comb[comb['_merge'] != 'both'].copy()


  # fix nulls
  missing['source_code'] = missing['source_code'].replace(['', 'NONE'], np.nan) # added NONE 2/14/25
  missing = missing[missing['source_code'] != '']
  missing['source'] = 'inferred from trx'
  missing['data_processed_at'] = ts_now()

  # estimate mail date
  missing['tmp'] = missing['source_code'].fillna(missing['campaign_name']) # Changed Back 2/20/25

  # Add gift fy to tmp for grouping - temp + _ + fy['gift_date'] (Added 3/20/25)
  missing['tmp'] = missing['tmp'] + "_" + fiscal_from_column(missing, 'gift_date', schema['firstMonthFiscalYear']).astype(str)
  # Group by tmp to get min mail_date
  missing['mail_date'] = missing.groupby('tmp')['gift_date'].transform('min')
  # Add fy for missing dates grouped
  missing['fy'] = fiscal_from_column(missing, 'mail_date', schema['firstMonthFiscalYear'])

  # UPDATED TO ADD FY TO END 2/20/25
  missing['client_campaign'] = missing.apply(
    lambda row: (
      f"{row['client'].upper()}-{row['campaign_code']}-{row['campaign_name']}" if pd.notna(row.get('campaign_code')) and pd.notna(row.get('campaign_name')) else
      f"{row['client'].upper()}-{row['campaign_code']}" if pd.notna(row.get('campaign_code')) else
      f"{row['client'].upper()}-CN {row['campaign_name']}" if pd.notna(row.get('campaign_name')) else
      f"{row['client'].upper()}-{row['source_code']}" if pd.notna(row.get('source_code')) else
      row['client'].upper()
    ) + (f"-MD_FY{row['fy']}" if pd.notna(row.get('fy')) else ""),
    axis=1
  )

  # narrow missing to cols in sc
  to_use = list(set(missing.columns) & set(sc_dims))
  missing = missing[to_use].drop_duplicates().reset_index(drop=True)

  # Identify duplicate columns
  dupe_cols = missing.columns[missing.columns.duplicated()]
  # If any duplicate columns, print a warning and drop them
  if len(dupe_cols) > 0:
    print(f"⚠️ Warning: Dropping duplicate columns: {list(dupe_cols)} for sc_find_missing. Keeping first column in dataframe.")
    missing = missing.loc[:, ~missing.columns.duplicated()]
  else:
    print("✅ No duplicate columns found in sc_find_missing.")

  # ----------------------------
  # Backfill campaign_group in `missing` from `sc2` using campaign_code
  # (only where missing.campaign_group is blank)
  # ----------------------------
  # Normalize blanks -> NA
  if 'campaign_group' not in missing.columns:
    missing['campaign_group'] = None
  missing['campaign_group'] = missing['campaign_group'].replace(['', ' ', 'NONE', 'None'], np.nan)
  missing['campaign_code']  = missing['campaign_code'].replace(['', ' ', 'NONE', 'None'], np.nan)

  if 'campaign_group' not in sc2.columns:
    sc2['campaign_group'] = None
  sc2['campaign_group'] = sc2['campaign_group'].replace(['', ' ', 'NONE', 'None'], np.nan)
  sc2['campaign_code']  = sc2['campaign_code'].replace(['', ' ', 'NONE', 'None'], np.nan)
  # Build lookup: campaign_code -> campaign_group (dedupe safely)
  sc2_lookup = (
      sc2.loc[sc2['campaign_code'].notna() & sc2['campaign_group'].notna(), ['campaign_code', 'campaign_group']]
        .drop_duplicates()
  )
  # If duplicates still exist (multiple groups for same code), take the most frequent
  sc2_lookup = (
      sc2_lookup.groupby('campaign_code')['campaign_group']
                .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0])
                .reset_index()
  )
  # Map onto missing
  missing['campaign_group'] = missing['campaign_group'].fillna(
      missing['campaign_code'].map(sc2_lookup.set_index('campaign_code')['campaign_group'])
  )
  # Optional debug
  print(f"🔁 Backfilled campaign_group for {missing['campaign_group'].notna().sum():,} / {len(missing):,} rows (non-null)")
  #-------------------------------------------------------------------------------------------------------------------------------------

  # add identifiers (Moved here in OOO 2/26/25 to remove duplicates before assigning source_code_key)
  missing['source_code_key'] = [str(i) for i in range(1, len(missing) + 1)]

  # load missing source codes separately
  table_name = client.lower() + '_source_code_missing'
  sql_import(missing, table_name)

  missing = apply_client_source_code_missing_transform(missing, client)

  #Add curated_sc_key
  missing['sc_key_curated'] = (missing['client'].str.upper().fillna('') + '_trx_' + missing['source_code_key'].astype(str).fillna(''))

  # OPTION 2: INFERRED FROM TRX
  script = "delete from {tbl} where source = 'inferred from trx'".format(tbl=client.lower() + '_source_code')
  sql_exec_only(script)
  sql_import(missing, client.lower() + '_source_code', overwrite_or_append='append')

  etl2_status_entry(client, 'Source Code Processing: {num} sources in tx data not in SourceCode.csv'.format(num=len(missing)))



# COMMAND ----------

# DBTITLE 1,def qa_source_codes
# qa 1: populated with non blanks
def _qa_sc_unexpected_blanks(client):
  table_name = client.lower()+'_source_code'
  query = "select count(distinct source_code) unexpectedly_empty_scs from {tbl} where source_code <> '' and source <> 'inferred from trx'".format(tbl=table_name)
  results = sql(query)['unexpectedly_empty_scs'].iloc[0]
  add_to_error_table(client, 
                     'Source Codes populated', 
                     'Source Code', 
                     '{nmb} source codes processed'.format(nmb=results), 
                     results == 0, 
                     'error')

# qa 2: placeholders inserted
def _qa_sc_placeholders(client):
  table_name = client.lower()+'_source_code'
  query = "select count(distinct source_code) placeholders_inserted from {tbl} where source = 'unsourced fallbacks'".format(tbl=table_name)
  results = sql(query)['placeholders_inserted'].iloc[0]
  add_to_error_table(client, 
                     'Placeholders populated', 
                     'Source Code', 
                     '{nmb} placeholders for unsourced gifts inserted'.format(nmb=results), 
                     results == 0, 
                     'error')

# TBD: no missing client_campaign values

# TBD: All gift years have unsourced placeholders


def qa_source_codes(client):
  # Run
  _qa_sc_unexpected_blanks(client)
  _qa_sc_placeholders(client)

  # tally em up
  warnings, errors = query_qa_errors(client,['Source Code'])
  etl2_status_entry(client,'Source Code Processing: QAd (Warnings: {warn}, Errors: {err})'.format(warn=warnings, err=errors))

  # Publish
  if apply_client_force_publish_source_code_despite_errors(client):
    errors = 0
  if errors == 0:
    print("No errors found. Publishing to Curated")
    publish_to_curated(client.lower()+'_source_code',False)
    etl2_status_entry(client,'Source Code Processing: Published to Curated')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Budget File

# COMMAND ----------

# DBTITLE 1,def budget_load_and_process_csv
"""
client = 'TCI'
"""

def budget_load_and_process_csv(client):
    schema = get_schema(client)

    ################################################
    # Read Budget from bronze Delta table (or file fallback)
    ################################################
    df = load_budget(client)
    if df is None:
        raise Exception(f"Failed to load Budget for client {client}")
    df.columns = df.columns.str.strip()

    # Standardize column names
    df.rename(columns={'Data_Processed_At': 'data_processed_at'}, inplace=True)

    ## Generate budget key column
    # df = generate_budget_key_column(
    #     df,
    #     new_col_name='Budg_SC_Unique_Code',
    #     required_cols=['Client', 'CampaignCode'],
    #     optional_cols=['Program (SC)'])

    ################################################
    # Define SQL-aligned dtypes
    ################################################
    expected_dtypes = {
        'Client': 'string',
        'Budg_Type': 'string',
        'Budg_FY': 'string',
        'Budg_FYQtr': 'string',
        'Budg_FYMonth': 'string',
        'Budg_CampaignCode': 'string',
        'Budg_CampaignName': 'string',
        'Budg_CampaignGroup': 'string',
        'Budg_Channel': 'string',
        'Budg_Program': 'string',
        'Budg_MailDate': 'datetime64[ns]',
        'Budg_Quantity': 'float64',
        'Budg_Gifts': 'float64',
        'Budg_Revenue': 'float64',
        'Budg_Cost': 'float64',
        'Budg_YE_Gifts': 'float64',
        'Budg_YE_Revenue': 'float64',
        'Budg_Client_CampCode': 'object',
        'data_processed_at': 'datetime64[ns]'
    }

    ################################################
    # Enforce datatypes
    ################################################
    for col, dtype in expected_dtypes.items():
        if col in df.columns:
            if "datetime" in dtype:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif dtype == 'float64':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
            else:
                df[col] = df[col].astype('string').str.strip()
        else:
            print(f"⚠️ Warning: Column '{col}' not found in Budget.csv")

    ################################################
    # Replace NaNs with None for SQL inserts
    ################################################
    df = df.where(pd.notnull(df), None)

    ################################################
    # Return cleaned DataFrame
    ################################################
    print(f"✅ Budget DataFrame ready with {len(df)} rows and {len(df.columns)} columns.")
    return df

# COMMAND ----------

# DBTITLE 1,def process_budget_table
""" 
import math
import time
from datetime import datetime

client = 'TCI'
budget_df = budget_load_and_process_csv('client')

"""
def process_budget_table(budget_df, client):
    logger.warning('****** Budget Table Load Started ******')

    # Setup (dbo path - publish_to_curated in publish_bi_tables)
    table_name = f"{client.lower()}_budget"
    create_table_from_template('template_budget', table_name)
    etl2_status_entry(client, 'BI Table Processing: Budget Table Started')

    # Prepare DataFrame
    budget_df['data_processed_at'] = ts_now()
    budget_df['data_processed_at'] = pd.to_datetime(budget_df['data_processed_at'], errors='coerce')

    # Align column names to match SQL naming
    budget_df_db = database_headers(budget_df)

    # --- Keep only columns defined in table_schema['BudgetTable'] ---
    valid_cols = table_schema['BudgetTable']
    budget_df_db = budget_df_db[[c for c in budget_df_db.columns if c in valid_cols]]

    # Optional: quick warning for visibility
    dropped_cols = [c for c in budget_df_db.columns if c not in valid_cols]
    if dropped_cols:
        logger.warning(f"Dropped {len(dropped_cols)} columns not in table_schema['BudgetTable']: {dropped_cols}")

    # Insert logic (Delta: sql_import with chunked overwrite/append)
    import math
    import time
    chunksize = 10_000
    total_rows = len(budget_df_db)
    logger.warning(f"Inserting {total_rows:,} rows into {table_name}")

    start_time = time.time()
    for i, start in enumerate(range(0, total_rows, chunksize), start=1):
        end = start + chunksize
        chunk = budget_df_db.iloc[start:end]
        mode = 'overwrite' if i == 1 else 'append'
        sql_import(chunk, table_name, overwrite_or_append=mode)
    logger.warning(f"****** Budget Load Complete in {time.time() - start_time:.2f}s ******")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Transactions File
# MAGIC This section pulls together the various logic siloed today by different report code. The output of this section is a standardized Transaction resultset that has each clients' specific logic applied. Output saved to **transactions.parquet** in **Master** client folder.

# COMMAND ----------

# DBTITLE 1,def process_trx_apply_suppressions -> df
def process_trx_apply_suppressions(client): #client = 'AFHU'
  # load data.parquet
  df = load_parquet(client)

  # format donor id
  #df['DonorID']

  # suppressions
  schema = get_schema(client)
  suppressions = schema['Suppressions']
  for k in suppressions.keys():
    logger.warning(f"df shape before suppressions for {k}: {df.shape}")
    df = apply_func(df, suppressions, k)
    logger.warning(f"df shape after suppressions for {k}: {df.shape}")

  # if multiple gift ids (AFHU)
  if ('GiftID' in df.columns) & ('Gift ID' in df.columns):
    df['GiftID'] = np.where((df['GiftID'].isna()) | (df['GiftID']=='nan')
            , df['Gift ID']
            , df['GiftID']
            )
    df.drop(columns='Gift ID', inplace=True)
  
  # General Additions
  df['GiftFY'] = fiscal_from_column(df, 'GiftDate', schema['firstMonthFiscalYear']).astype(int)
#   df['GiftFY'] = (
#     pd.Series(fiscal_from_column(df, 'GiftDate', schema['firstMonthFiscalYear']))
#     .fillna(0)
#     .astype(int)
# )


  return df

# COMMAND ----------

# DBTITLE 1,def process_trx_add_fh_filters: Add Columns (Filters) -> df3
# client = 'TSE' df = df2
import numpy as np
def process_trx_add_fh_filters(df, client):
  try:
    # Get first gift date
    df = get_fh_FirstGiftDate(df) #df.columns
    df['gift_id_bkup']=df['GiftID']

    # Add FH filters (generic + client specific) #client = 'WFP'
    schema = get_schema(client)
    filters = get_fh_filters(schema['FileHealth']) # these require first gift date first

    # Add filters that all clients should have
    generics = ['DonorGroup', 'GiftLevel', 'GiftMonth', 'GiftHistory', 'GiftAmountFlag', 'JoinLevel']
    filters.extend([x for x in generics if x not in filters])

    # Remove some filters that will now be handled elsewhere
    removes = ['GiftFiscal', 'JoinFiscal', 'JoinFiscalYear', 'JoinFY', 'GiftFY']
    filters = [x for x in filters if x not in removes]

    for f in filters:
      #print(f)
      df = apply_filters(df, f)
    
    return df
  
  except Exception as e:
    logger.error("process_trx_add_fh_filters: %s", traceback.format_exc())
    notification_exception_raised(client, code_location='process_trx_add_fh_filters', error_message=repr(e))
    raise(e)

# COMMAND ----------

# DBTITLE 1,def process_trx_add_cp_filters -> df5
# df = df4 
'''
dfc.columns[dfc.columns.str.lower().duplicated()]
'''
def process_trx_add_cp_filters(df, client):
  ### load source code # df.columns
  # sourcecode.csv
  sc = sc_load_and_process_csv(client,max_gift=df['GiftDate'].max(),min_gift=df['GiftDate'].min())

  # create white mail source codes
  schema = get_schema(client)
  if 'SynthSC' in schema.keys():
    df = add_synth_sc(df, schema)

  # label unsourced gifts -- MK: These don't appear to get used
  #pattern = schema['SourceCodeRegex']['SourceCode']
  #cond = check_col(df.SourceCode, pattern)
  
  # save appeal to campaign and appeal to mail date mapping
  cols = ['CampaignCode', 'CampaignName'] #, 'mail_date']
  _sc = sc.drop_duplicates('CampaignCode')[cols]
  appealsToCampaigns = dict(zip(_sc['CampaignCode'], _sc['CampaignName']))
  #appealstoMailDates = dict(zip(_sc['campaign_code'], _sc['MailDate'])) # !!! Unsure if used. Holding for removal.
  
  # Manual col renames to prevent downstream duplications
  ren = {
    'Campaign Name':'CampaignName',
    'Source Code':'SourceCode',
    'Campaign Code':'CampaignCode'
  }
  df.rename(columns=ren, inplace=True)
  #Add CampaignCode with blanks if not in columns
  if "CampaignCode" not in df.columns:
      df["CampaignCode"] = "" 
  #Rename CampaignCode from df df_CampaignCode to differentiate from SC    
  df = df.rename(columns={'CampaignCode': 'df_CampaignCode'})

  ### join tx and sc data
  dfc = pd.merge(df, sc, left_on='SourceCode', right_on='SourceCode', how='outer', suffixes=['','_sc']) # this used to be df[cols]
  
  # Use SC ListCode if Trx ListCode is na 
  cols = [c for c in dfc.columns if c in ['ListCode', 'ListCode_sc']]
  
  if len(cols) > 1:
    dfc['ListCode'] = np.where(dfc['ListCode'].isna(),
      dfc['ListCode_sc'],
      dfc['ListCode']
    )
    dfc.drop(columns=['ListCode_sc'],inplace=True)
  
  #############################################
  # Attempt to map in a campaign name where it may not be included in the source code docs
  #############################################
  # if client == 'ASJ' and 'df_CampaignCode' not in dfc.columns:
  #   dfc['df_CampaignCode'] = ''

  dfc.CampaignCode = np.where(dfc.CampaignCode.isna(), dfc.df_CampaignCode, dfc.CampaignCode)

  dfc['TempCampName'] = dfc.CampaignCode.map(appealsToCampaigns) 
  dfc.CampaignName = np.where(dfc.CampaignName.isna(), dfc.TempCampName, dfc.CampaignName) 
  dfc = dfc.drop(['TempCampName', 'df_CampaignCode'], axis = 1)

  # create map of gift ids to campaign names (df was specifically used instead of dfc in the etl1 code)
  if 'CampaignName' in df.columns:
    idsToCampaigns = dict(zip(df.GiftID, df.CampaignName))
  else:
    idsToCampaigns = {}

  dfc['TempCampName'] = dfc.GiftID.map(idsToCampaigns)
  dfc.CampaignName = np.where(dfc.CampaignName.isna(), dfc.TempCampName, dfc.CampaignName)
  dfc = dfc.drop(['TempCampName'], axis = 1)
  # dfc.columns

  # Apply client specific filter restrictions on the dataset as needed
  dfc = add_dataset_filters(dfc, client) # source code validation (CHOA is only adding a GiftFiscal columns, not filtering though.)

  # Rename
  m = {
    'ListCPP': '_ListCPP',
    'Quantity': 'RawQuantity'
    }
  dfc = dfc.rename(columns=m)
  dfc["ListCode"] = dfc["ListCode"].astype(str).str.replace(r"\.0$", "", regex=True)

  # #Add blank sc_key_curated columns
  # dfc['sc_key_curated'] = ''
  
  #############################################
  # Append SCs from trx that aren't in the csv
  #############################################
  sc_find_missing(dfc,sc) 

  #############################################
  # QA: Source Codes
  #############################################
  
  qa_source_codes(client)

  return dfc

# COMMAND ----------

# DBTITLE 1,def trx_backfill_sc_key_curated
from typing import Sequence
import pandas as pd

def _clean_str(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
         .astype(str)
         .str.strip()
    )

def trx_backfill_sc_key_curated(df: pd.DataFrame, sc_curated: pd.DataFrame) -> pd.DataFrame:

    print(f"\n🔁 Starting sc_key_curated backfill")
    print(f"   Gift rows: {len(df):,}")
    print(f"   SC rows:   {len(sc_curated):,}")

    out = df.copy().reset_index(drop=True)

    out["sc_key_curated"] = pd.NA
    out["sc_key_match_method"] = pd.NA

    def _empty_series(idx):
        return pd.Series([""] * len(idx), index=idx, dtype="object")

    # --------------------
    # Normalize columns
    # --------------------
    print("🧹 Normalizing join columns...")

    out["_client"]      = _clean_str(out["client"])
    out["_source_code"] = _clean_str(out["source_code"]) if "source_code" in out.columns else _empty_series(out.index)
    out["_camp_name"]   = _clean_str(out["campaign_name"]) if "campaign_name" in out.columns else _empty_series(out.index)
    out["_camp_code"]   = _clean_str(out["campaign_code"]) if "campaign_code" in out.columns else _empty_series(out.index)

    sc = sc_curated.copy()
    sc["_client"]      = _clean_str(sc["client"])
    sc["_source_code"] = _clean_str(sc["source_code"]) if "source_code" in sc.columns else _empty_series(sc.index)
    sc["_camp_name"]   = _clean_str(sc["campaign_name"]) if "campaign_name" in sc.columns else _empty_series(sc.index)
    sc["_camp_code"]   = _clean_str(sc["campaign_code"]) if "campaign_code" in sc.columns else _empty_series(sc.index)

    def _make_lookup(sc_df: pd.DataFrame, key_cols: Sequence[str]) -> pd.DataFrame:
        sc2 = sc_df.loc[
            sc_df["sc_key_curated"].notna(),
            list(key_cols) + ["sc_key_curated"]
        ].copy()
        return sc2.drop_duplicates(subset=list(key_cols), keep="first")

    cand = pd.DataFrame(index=out.index)

    # --------------------
    # Step 1
    # --------------------
    print("🔍 Step 1 — client + source_code")

    lk1 = _make_lookup(sc, ["_client", "_source_code"])
    cand["m1"] = (
        out[["_client", "_source_code"]]
        .merge(lk1, on=["_client", "_source_code"], how="left")["sc_key_curated"]
        .to_numpy()
    )

    print(f"   Matches: {pd.notna(cand['m1']).sum():,}")

    # --------------------
    # Step 2
    # --------------------
    print("🔍 Step 2 — client + source_code + campaign_name + campaign_code")

    lk2 = _make_lookup(sc, ["_client", "_source_code", "_camp_name", "_camp_code"])
    cand["m2"] = (
        out[["_client", "_source_code", "_camp_name", "_camp_code"]]
        .merge(lk2, on=["_client", "_source_code", "_camp_name", "_camp_code"], how="left")["sc_key_curated"]
        .to_numpy()
    )

    print(f"   Matches: {pd.notna(cand['m2']).sum():,}")

    # --------------------
    # Step 3
    # --------------------
    print("🔍 Step 3 — client + source_code + campaign_name")

    lk3 = _make_lookup(sc, ["_client", "_source_code", "_camp_name"])
    cand["m3"] = (
        out[["_client", "_source_code", "_camp_name"]]
        .merge(lk3, on=["_client", "_source_code", "_camp_name"], how="left")["sc_key_curated"]
        .to_numpy()
    )

    print(f"   Matches: {pd.notna(cand['m3']).sum():,}")

    # --------------------
    # Step 4
    # --------------------
    print("🔍 Step 4 — FY fallback (UNSOURCED-FY)")

    sc_uns = sc.loc[sc["_camp_code"].str.startswith("UNSOURCED-FY", na=False)].copy()

    print(f"   UNSOURCED rows: {len(sc_uns):,}")

    out["_gift_fy_norm"] = _clean_str(out["gift_fy"]) if "gift_fy" in out.columns else _empty_series(out.index)
    sc_uns["_fy_norm"]   = _clean_str(sc_uns["fy"]) if "fy" in sc_uns.columns else _empty_series(sc_uns.index)

    lk4 = _make_lookup(sc_uns, ["_client", "_fy_norm"])
    cand["m4"] = (
        out[["_client", "_gift_fy_norm"]]
        .rename(columns={"_gift_fy_norm": "_fy_norm"})
        .merge(lk4, on=["_client", "_fy_norm"], how="left")["sc_key_curated"]
        .to_numpy()
    )

    print(f"   Matches: {pd.notna(cand['m4']).sum():,}")

    # --------------------
    # Priority combine
    # --------------------
    print("📌 Applying priority logic (Step2 → Step3 → Step1 → Step4)")

    out["sc_key_curated"] = (
        pd.Series(cand["m2"])
        .combine_first(pd.Series(cand["m3"]))
        .combine_first(pd.Series(cand["m1"]))
        .combine_first(pd.Series(cand["m4"]))
    )

    filled = pd.notna(out["sc_key_curated"]).sum()
    total  = len(out)

    print(f"✅ Final filled: {filled:,} / {total:,}")
    print(f"❗ Remaining nulls: {total - filled:,}")

    # cleanup
    out = out.drop(columns=[c for c in out.columns if c.startswith("_")])
    out = out.drop(columns=["sc_key_match_method"])

    print("🎉 Backfill complete\n")

    return out




# COMMAND ----------

# DBTITLE 1,def process_trx_prep_data ->df6
# df = df5  client = 'CHOA'
def process_trx_prep_data(df, client, column_renames):
  df = database_headers(df)
  df.columns = remove_client_prefix(df.columns,client)

  # handle duplicated columns
  dupes = df.columns[df.columns.duplicated()]
  for col in dupes:
    #print('Fix dupe: '+col)
    df[col+'_new'] = df[[col]].bfill(axis=1).iloc[:, 0]
    df.drop(columns=[col],inplace=True)
    df.rename(columns={col+'_new':col},inplace=True)
  
  # handle various forms of null columns
  for col in df.columns:
    #print(col)
    df[col] = df[col].replace(r"\bnan\b",np.nan,regex=True)
    df[col] = df[col].replace(r"\bNone\b",np.nan,regex=True)
    df[col] = df[col].replace({None: np.nan})
    # df[col] = df[col].replace('', np.nan)
    df[col] = df[col].apply(lambda x: np.nan if isinstance(x, str) and x == '' else x)
  # df.['GiftChannel'].drop_duplicates()

  # add client 
  df['client']=client

  # fix donor_id format
  df["donor_id"] = (
      df["donor_id"]
        .astype(str)
        .str.replace(".0", "", regex=False)   # <-- key change
        .replace({"nan": np.nan})
  )
  # fix dates
  if 'first_gift_date' not in df.columns:
    df['first_gift_date']=pd.NaT
  
  date_cols = ['gift_date', 'first_gift_date']
  for dt in date_cols:
    #print(dt)
    try:
      df[dt]=pd.to_datetime(df[dt])
    except Exception as e:
      logger.error("process_trx_prep_data: %s", traceback.format_exc())

  # add date processed (this should only be necessary until all clients are processed post code update for this field)
  if 'data_processed_at' not in df.columns:
    df['data_processed_at']=np.datetime64("NaT")
  
  # Column renames for consistency
  for k,v in column_renames.items():
    if v in df.columns and k in df.columns:
      msg = 'Both '+k+' and '+v+' are present in the data. This is a fatal error that must be reconciled.'
      print(msg)
      notification_exception_raised(client, code_location='process_trx_prep_data', error_message=msg)
      raise Exception(msg)
    elif k in df.columns:
      print('Update: '+k+' to '+v)
      df.rename(columns={k:v}, inplace=True)
    else:
      print('Skip: '+k)
  
  # deduplicate by gift id (if exists), otherwise add gift id
  if 'gift_id' in df.columns:
    df = df.drop_duplicates('gift_id')
  else:
    df = df.drop_duplicates()
    df['gift_id'] = [i for i in range(df.shape[0])]

  # add donor id if not in there (placeholder)
  if 'donor_id' not in df.columns:
    logger.warning('Create definition of a donor')
  
  # remove empty rows
  df = df[(df['donor_id'].notna()) & (df['gift_date'].notna()) & (df['gift_amount'].notna())]
  
  # Add empty cols as necessary
  all_cols = list({id(x):x for x in gift_dims+donor_dims}.values()) #sc_dims
  for item in all_cols:
    if item not in df.columns:
      logger.warning('Add missing col: '+item)
      df[item] = None
  
  # Narrow to cols needed from table schema definitions
  # df = df[all_cols]

  # Replace NaN with NaT for dates
  dic = {**table_schema['DonorTable'],**table_schema['GiftTable'],**table_schema['SourceCodeTable']}
  dates = [k for k, v in dic.items() if 'date' in v]
  for col in dates:
    logger.warning(col)
    if col in df.columns:
        df[col] = df[col].replace(np.NaN, pd.NaT)
    else:
        logger.warning(f"Column '{col}' not in dataframe. Adding it as NaT.")
        df[col] = pd.NaT


  # Add Unique Integer Keys
  df['donor_key'] = pd.factorize(df['donor_id'])[0]
  df['gift_key'] = pd.factorize(df['gift_id'])[0]

  # Gift Month & CY
  df['gift_month'] = df['gift_date'].dt.month.astype('Int64')
  df['gift_cy'] = df['gift_date'].dt.year.astype('Int64')

  #Add sc_key_curated column to gift table
  df['sc_key_curated'] = ''
  sc_curated = sql(f"select * from curated.{client.lower()}_source_code")
  df = trx_backfill_sc_key_curated(df, sc_curated)

  return df
  logger.warning('******Data Prepped')


# COMMAND ----------

# DBTITLE 1,def qa_transactions
### To add new checks, simply create a function and add it to the qa_transactions function below.

def _qa_transaction_totals(df):
  if df.count()['gift_date'] <= 100:
    fail = 1
  else:
    fail = 0
  
  check_level = 'error'
  detail = 'Record count: '+str(df.count()['gift_date'])
  add_to_error_table(client,'Transactions.parquet volume', 'Transactions.parquet', detail, fail, check_level)
  return fail, 0


def _qa_keys(df):
  # Donor Keys
  missing_donor_id = df['donor_id'].isna().sum()
  add_to_error_table(client, 'IDs Present', 'Transactions.parquet', str(missing_donor_id)+' records missing donor_id', missing_donor_id>0, 'warning')

  # Gift Keys
  missing_gift_id = df['gift_id'].isna().sum()
  add_to_error_table(client, 'IDs Present', 'Transactions.parquet', str(missing_gift_id)+' records missing gift_id', missing_gift_id>0, 'warning')

######################################################
# Wrapper
######################################################
def qa_transactions(df):
  client = df['client'].iloc[0]
  _qa_transaction_totals(df)
  _qa_keys(df)
  warnings, errors = query_qa_errors(client, ['Transactions.parquet'])

  return warnings, errors

# COMMAND ----------

# DBTITLE 1,*def process_transactions
def process_transactions(client): #client = 'NJH'
  print('process_transactions running')
  schema = get_schema(client)

  print("Building df")
  # load & apply suppressions # client = 'CARE'
  df = process_trx_apply_suppressions(client) # 
  etl2_status_entry(client,'Transaction Processing: Loaded & Suppressed')
  logger.warning('*** Loaded & Suppressed')

  # Column Mappers
  maps = {
    'Gift ID':'GiftID'
  }
  df.rename(columns=maps,inplace=True)
  
  # deduplicate by gift id (if exists), otherwise add gift id
  if 'GiftID' in df.columns:
    # correct nulls
    df['GiftID'] = df['GiftID'].replace(r"\bnan\b",np.nan,regex=True)
    df['GiftID'] = df['GiftID'].replace(r"\bNone\b",np.nan,regex=True)
    df['GiftID'] = df['GiftID'].replace({None: np.nan})
    print('df Built')
    print('Building df2')
    if df['GiftID'].isna().sum()==0: # all records have a giftid
      df2 = df.drop_duplicates('GiftID')
    else: # if at least some giftids are null, fill in blanks with index value
      df2 = df.drop_duplicates().reset_index(drop=True)
      df2['GiftID'] = np.where(df2['GiftID'].isna()
                      , ['_'+str(x) for x in df2.index.values]
                      , df2['GiftID'].astype(str))
  else: #if gift id doesn't exist, create from index
    df2 = df.drop_duplicates().reset_index(drop=True)
    df2['GiftID'] = ['_'+str(x) for x in df2.index.values]
  etl2_status_entry(client,'Transaction Processing: Gift IDs Handled')
  logger.warning('*** Gift IDs Handled')

  # exclude $0 gifts but keep negatives- business decision
  df2 = df2[df2['GiftAmount'].astype(float).fillna(0) != 0]


  # rename cp cols
  if 'CPMapper' in schema.keys():
    df2.rename(columns=schema['CPMapper'], inplace=True)
    etl2_status_entry(client,'Transaction Processing: Rename cp cols based on CPMapper')
    logger.warning('*** Rename cp cols based on CPMapper')
  print('df2 Built')
  # add FH columns
  print('Building df3')
  if 'FileHealth' in schema:
    df3 = process_trx_add_fh_filters(df2, client)
    etl2_status_entry(client,'Transaction Processing: FH Filters Added')
    logger.warning('*** FH Filters Added (process_trx_add_fh_filters)')
  else:
    df3=df2
  print('df3 Built')

  # clean source code column (if exists)
  print('Building df4')
  df4 = update_client_codes(df3, client) 
  etl2_status_entry(client,'Transaction Processing: Source Codes Cleaned')
  logger.warning('*** Source Codes Processed (update_client_codes)') 
  print('df4 Built')

  # camp perf actions
  print('Building df5')
  if not apply_client_skip_cp_filters(client):
    df5 = process_trx_add_cp_filters(df4, client)
    etl2_status_entry(client,'Transaction Processing: CP Filters Added')
    logger.warning('*** CP Filters Added (process_trx_add_cp_filters)')
    
  else:
    df5=df4
  print('df5 Built')

  print('Building df6')
  # clean and prep
  df6 = process_trx_prep_data(df5, client, column_renames) # df6 = df
  etl2_status_entry(client,'Transaction Processing: Structure Standardized & Prepared for Use')
  logger.warning('*** Structure Standardized & Prepared for Use (process_trx_prep_data)')
  print("df6 Built")
  # add QA check
  print('qa_transactions df6 started')
  if not apply_client_skip_trx_qa(client):
    warnings, errors = qa_transactions(df6)
  else:
    warnings, errors = 0, 0
  print('qa_transactions df6 complete')

  # store in Delta (silver transactions table)
  if errors == 0:
    df6['gift_id'] = df6['gift_id'].astype(str)
    table_name = client.lower() + '_transactions'
    sql_import(database_headers(df6), table_name, overwrite_or_append='overwrite')
    etl2_status_entry(client, 'Transaction Processing: Transactions stored in Delta')
    logger.warning('*** Transactions stored in Delta')
  else:
    etl2_status_entry(client,'Transaction Processing: FAILED, '+str(errors)+' errors')
    e = 'Transaction Processing Failed QA. See etl2_qa_results for details'
    notification_exception_raised(client, code_location='process_transactions', error_message=e)
    raise Exception(e)
  
  # Update trx mx data contained date
  update_max_date(client, df6['gift_date'].max(), 'trx_parquet')

  return df6

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process BI Tables

# COMMAND ----------

# DBTITLE 1,def universal gift filter logic
def universal_gift_logic(df,client):
  #Universal Gift table column add #1
  #Add fuse_tracked_campaign column
  # Load list of unique source codes
  sc = load_sc(client)
  # Check if `SourceCode` in `df` exists in `sc['SourceCode']` and add new column
  df["fuse_tracked_campaign"] = df["campaign_code"].isin(sc["CampaignCode"])
  logger.warning("fuse_tracked_campaign column added")

  #Universal Gift table column add #2 - Add donor gift number column
  df['donor_gift_number'] = (
    df.sort_values(['donor_id', 'gift_date', 'gift_amount'], ascending=[True, True, False])
      .groupby('donor_id', sort=False)
      .cumcount() + 1
).reindex(df.index)
#   df['donor_gift_number'] = ( # ranks gifts by number by date (and then gift amount if >2 gifts per date)
#     df.sort_values(['donor_id', 'gift_date', 'gift_amount'], ascending=[True, True, False])#.reset_index(drop=True) # added reset index after error: "cannot reindex on an axis with duplicate labels"
#       .groupby('donor_id')
#       .cumcount() + 1
#       #.sort_index()  # optional, restores to original df's row order
# )
  return df

# COMMAND ----------

# DBTITLE 1,def qa_gift
# ### To add new checks, simply create a function and add it to the qa_gift function below.

# ######################################################
# # Compare various figures between ETL1 and ETL2. These will eventually sunset. 
# ######################################################
# def _qa_gift_comp_to_stg(gift):
#   client = gift['client'].iloc[0]
#   schema = get_schema(client) #client = 'U4U'

#   # load stg
#   stg = load_stg(client)
#   stg = stg[stg['GiftAmount'].astype(float).fillna(0)>0]
  
#   stg_min = stg['GiftDate'].min()
#   stg_max = stg['GiftDate'].max()
  
#   # load cp
#   if 'CampaignPerformance_wFilters' in schema['ReportMenu'].keys():
#     cp = load_cp(client)
#     cp_min = cp['GiftDate'].min()
#     cp_max = cp['GiftDate'].max()
#     mn = max(stg_min,cp_min)
#     mx = min(stg_max,cp_max)
#   else:
#     mn = stg_min
#     mx = stg_max

#   # set comp times
#   stg_comp = stg[(stg['GiftDate']>=mn) & (stg['GiftDate']<=mx)]
#   gift_comp = gift[(gift['gift_date']>=mn) & (gift['gift_date']<=mx)]

#   # Delete checks for this Client+Dataset combo
#   script = "delete from etl2_qa_results where client='{cl}' and dataset='Gift'".format(cl=client)
#   sql_exec_only(script)

#   # # Gift = FH for DonorIDs
#   # fh_donor_cnt = stg_comp['DonorID'].nunique() 
#   # g_donor_cnt = gift_comp['donor_id'].nunique()
#   # check = 'Donor ID counts match Staged for FH'
#   # fail = fh_donor_cnt != g_donor_cnt
#   # error_description = np.where(fail,
#   #                           'FH Donor Count: '+str(fh_donor_cnt)+', Gift Donor Count: '+str(g_donor_cnt),
#   #                           ''
#   # )
#   # check_level = np.where(g_donor_cnt/fh_donor_cnt < .80,
#   #                        'error',
#   #                        'warning')
#   # add_to_error_table(client, check, 'Gift', error_description, fail, check_level)

#   # Gift = FH for GiftAmount
#   fh_ga = stg_comp['GiftAmount'].sum().round(0)
#   g_ga = gift_comp['gift_amount'].sum().round(0)
#   check = 'Gift Amount compared to Staged for FH'
#   fail = fh_ga > g_ga
#   error_description = np.where(fail,
#                             'FH Gift Amount: '+str(fh_ga)+', Gift Gift Amount: '+str(g_ga),
#                             ''
#   )
#   check_level = 'warning'
#   add_to_error_table(client, check, 'Gift', error_description, fail, check_level)

#   # Gift = CP for GiftAmount
#   if 'CampaignPerformance_wFilters' in schema['ReportMenu'].keys():
#     cp_comp = cp[(cp['GiftDate']>=mn) & (cp['GiftDate']<=mx)]
#     cp_ga = cp_comp['GiftAmount'].sum().round(0)
#     g_ga = gift_comp['gift_amount'].sum().round(0)
#     check = 'Gift Amount compared to CP'
#     fail = cp_ga > g_ga
#     error_description = np.where(fail,
#                               'CP Gift Amount: '+str(cp_ga)+', Gift Gift Amount: '+str(g_ga),
#                               ''
#     )
#     check_level = 'warning'
#     add_to_error_table(client, check, 'Gift', error_description, fail, check_level)

# ######################################################
# # Count how many gifts have a blank source code
# ######################################################
# def _qa_gift_scs(gift):
#   client = gift['client'].iloc[0]
#   cnt = gift['source_code'].drop_duplicates().notna().sum()
#   add_to_error_table(client, 'Source Codes Processed', 'Gift', str(cnt)+' Source Codes present', cnt == 0, 'warning')
#   return 0, (cnt == 0).sum()

# ######################################################
# # Wrapper
# ######################################################
# def qa_gift(gift, client):
#   schema = get_schema(client)
#   _qa_gift_comp_to_stg(gift)

#   if 'SourceCodeProcessing' in schema['ReportMenu'].keys():
#     _qa_gift_scs(gift)

#   warnings, errors = query_qa_errors(client,['Gift'])
#   return warnings, errors

# COMMAND ----------

# DBTITLE 1,def qa_gift
### To add new checks, simply create a function and add it to the qa_gift function below.

######################################################
# Compare various figures between Gifts and Data.pqt
######################################################
def _qa_gift_comp_to_stg(gift):
  client = gift['client'].iloc[0]
  schema = get_schema(client) #client = 'U4U'

  # load staging data (previously Staged CampPerf, data, now Data.parquet)
  stg = load_parquet(client)
  stg = stg[stg['GiftAmount'].astype(float).fillna(0)>0]
  
  stg_min = stg['GiftDate'].min()
  stg_max = stg['GiftDate'].max()
  
  # load cp
  # if 'CampaignPerformance_wFilters' in schema['ReportMenu'].keys():
  #   cp = load_cp(client)
  #   cp_min = cp['GiftDate'].min()
  #   cp_max = cp['GiftDate'].max()
  #   mn = max(stg_min,cp_min)
  #   mx = min(stg_max,cp_max)
  # else:
  mn = stg_min
  mx = stg_max

  # set comp times
  stg_comp = stg[(stg['GiftDate']>=mn) & (stg['GiftDate']<=mx)]
  gift_comp = gift[(gift['gift_date']>=mn) & (gift['gift_date']<=mx)]

  # Delete checks for this Client+Dataset combo
  tbl = get_metadata_table_path('etl2_qa_results')
  sql_exec_only(f"DELETE FROM {tbl} WHERE client='{client}' AND dataset='Gift'")

  # # Gift = FH for DonorIDs
  # fh_donor_cnt = stg_comp['DonorID'].nunique() 
  # g_donor_cnt = gift_comp['donor_id'].nunique()
  # check = 'Donor ID counts match Staged for FH'
  # fail = fh_donor_cnt != g_donor_cnt
  # error_description = np.where(fail,
  #                           'FH Donor Count: '+str(fh_donor_cnt)+', Gift Donor Count: '+str(g_donor_cnt),
  #                           ''
  # )
  # check_level = np.where(g_donor_cnt/fh_donor_cnt < .80,
  #                        'error',
  #                        'warning')
  # add_to_error_table(client, check, 'Gift', error_description, fail, check_level)

  # Gift = FH for GiftAmount
  fh_ga = stg_comp['GiftAmount'].sum().round(0)
  g_ga = gift_comp['gift_amount'].sum().round(0)
  check = 'Gift Amount compared to Staged for FH'
  fail = fh_ga > g_ga
  error_description = np.where(fail,
                            'FH Gift Amount: '+str(fh_ga)+', Gift Gift Amount: '+str(g_ga),
                            ''
  )
  check_level = 'warning'
  add_to_error_table(client, check, 'Gift', error_description, fail, check_level)

  # Gift = CP for GiftAmount
  # if 'CampaignPerformance_wFilters' in schema['ReportMenu'].keys():
  stg_comp = stg_comp[(stg_comp['GiftDate']>=mn) & (stg_comp['GiftDate']<=mx)]
  stg_ga = stg_comp['GiftAmount'].sum().round(0)
  g_ga = gift_comp['gift_amount'].sum().round(0)
  check = 'Gift Amount compared to CP'
  fail = stg_ga > g_ga
  error_description = np.where(fail,
                            'CP Gift Amount: '+str(stg_ga)+', Gift Gift Amount: '+str(g_ga),
                            ''
  )
  check_level = 'warning'
  add_to_error_table(client, check, 'Gift', error_description, fail, check_level)

######################################################
# Count how many gifts have a blank source code
######################################################
def _qa_gift_scs(gift):
  client = gift['client'].iloc[0]
  cnt = gift['source_code'].drop_duplicates().notna().sum()
  add_to_error_table(client, 'Source Codes Processed', 'Gift', str(cnt)+' Source Codes present', cnt == 0, 'warning')
  return 0, (cnt == 0).sum()

######################################################
# Wrapper
######################################################
def qa_gift(gift, client):
  schema = get_schema(client)
  _qa_gift_comp_to_stg(gift)

  if 'SourceCodeProcessing' in schema['ReportMenu'].keys():
    _qa_gift_scs(gift)

  warnings, errors = query_qa_errors(client,['Gift'])
  return warnings, errors

# COMMAND ----------

# DBTITLE 1,def process_gift_table
####################################################################
# Client Specific Gift Table pd.set_option("max_rows", 25)
####################################################################
# df = load_transactions(client) #OR process_transactions(client)
def process_gift_table(df, client):
  logger.warning('******Gifts Started')
  #df = load_transactions('FWW')
  
  # create dataframe
  gift = pd.DataFrame(data=None,columns=gift_dims)
  gift = gift.astype(gift_def)

  # remove columns from dims if they don't exist in this particular client's dataset
  tmp = df[gift_dims]#.reset_index(drop=True)
  gift = pd.concat([gift,tmp])

  #Add universal gift columns
  gift = universal_gift_logic(gift,client)
  logger.warning('universal_gift_logic applied')

  #Add processed at column
  gift['data_processed_at']=ts_now()


  # Ensure table exists (no DROP - Delta overwrite handles data replacement)
  table_name = client.lower()+'_gift'
  create_table_from_template('template_gift', table_name)

  # don't set key if there are missing ids
  if (gift['gift_id'].isna()).sum()>0:
    drop_pk(table_name, 'gift_key')

  apply_client_after_gift_before_qa(gift, client)

  # # insert into SQL table (Replaced 8/22/25)
  # logger.warning('Inserting Gifts to SQL table')
  # etl2_status_entry(client,'BI Table Processing: Gifts Processed')
  # engine, params = connection(exec_method='sqlalchemy')
  # formatted = database_headers(gift)
  # formatted.to_sql(table_name, engine, if_exists='append', index=False, chunksize=100000) #chunksize updated from 1,000 to 100,000 to try to increase speed (2/27/25)
  # logger.warning('Gift Insertion to SQL table Complete')
  # etl2_status_entry(client,'BI Table Processing: Gift Table Loaded')

  #-----------------------------------------------------------------------------------------------------------
  # Insert Gifts to SQL Table (Delta: sql_import with chunked overwrite/append)
  import math
  logger.warning(f'[{datetime.now()}] Inserting Gifts to SQL table')
  start_time = time.time()

  formatted = database_headers(gift)

  chunksize = 2500
  total_rows = len(formatted)
  num_chunks = max(1, math.ceil(total_rows / chunksize))
  summary_every = 100  # <-- change this if you want a different summary cadence

  logger.warning(f"Total rows: {total_rows:,}, Chunks: {num_chunks}, Summary cadence: every {summary_every} chunk(s)")

  # rolling window stats
  win_times = []     # seconds per chunk in the current window
  win_rows = []      # rows inserted per chunk in the current window

  for i, start in enumerate(range(0, total_rows, chunksize), start=1):
      end = start + chunksize
      chunk = formatted.iloc[start:end]

      chunk_start = time.time()
      try:
          mode = 'overwrite' if i == 1 else 'append'
          sql_import(chunk, table_name, overwrite_or_append=mode)
      except Exception as e:
          logger.exception(f"Chunk {i}/{num_chunks} failed after {time.time() - chunk_start:.2f}s")
          raise

      # collect per-chunk stats but don't print yet
      chunk_elapsed = time.time() - chunk_start
      win_times.append(chunk_elapsed)
      win_rows.append(len(chunk))

      # when we reach the cadence (or the very last chunk), print a compact summary
      if (i % summary_every == 0) or (i == num_chunks):
          # window stats
          w_chunks = len(win_times)
          w_time_total = sum(win_times)
          w_rows_total = sum(win_rows)
          w_avg_sec = w_time_total / max(w_chunks, 1)
          w_min, w_sec = divmod(int(w_avg_sec), 60)
          w_rps = w_rows_total / max(w_time_total, 1e-9)

          # cumulative stats & ETA
          overall_elapsed = time.time() - start_time
          o_min, o_sec = divmod(int(overall_elapsed), 60)
          inserted_rows = min(i * chunksize, total_rows)
          overall_rps = inserted_rows / max(overall_elapsed, 1e-9)

          remaining_chunks = num_chunks - i
          # Use the *observed* average so far for ETA
          avg_chunk_sec_so_far = overall_elapsed / max(i, 1)
          eta_seconds = int(remaining_chunks * avg_chunk_sec_so_far)
          e_min, e_sec = divmod(eta_seconds, 60)

          # window boundaries for readability (e.g., 1-10, 11-20, ...)
          win_start_idx = i - w_chunks + 1
          win_end_idx = i

          logger.warning(
              f"[{datetime.now()}] Progress {win_start_idx}-{win_end_idx}/{num_chunks} chunks | "
              f"Window avg {w_min}m {w_sec}s per chunk | "
              f"Window throughput ~{w_rps:,.0f} rows/s | "
              f"Cumulative {o_min}m {o_sec}s | "
              f"Overall throughput ~{overall_rps:,.0f} rows/s | "
              f"ETA ~{e_min}m {e_sec}s"
          )

          # reset window
          win_times.clear()
          win_rows.clear()

  end_time = time.time()
  elapsed_seconds = end_time - start_time
  minutes, seconds = divmod(int(elapsed_seconds), 60)

  logger.warning(
      f'[{datetime.now()}] Gift Insertion to SQL table Complete '
      f'(took {minutes}m {seconds}s)'
  )
  etl2_status_entry(client, 'BI Table Processing: Gift Table Loaded')
  # ------------------------------------------------------------------------------------------------------------------

  # QA
  logger.warning("Gift QA Run Started")
  if not apply_client_skip_gift_qa(client):
    qa_gift(gift, client)
  logger.warning("Gift QA Run Complete")

  logger.warning('******Gifts Processed')
  return gift



# COMMAND ----------

# DBTITLE 1,def _donor_calc_columns
# trx=load_transactions('CHOA')

#donor = 
"""
  # create dataframe
  donor = pd.DataFrame(data=None,columns=donor_dims)
  donor = donor.astype(donor_def)
  # tmp donors
  tmp_donor = trx[donor_dims]
  # Make arrays into strings
  def convert_arrays_to_str(col):
    if isinstance(col.iloc[0], (list, np.ndarray)):
        logger.warning('Converting array to str')
        return col.apply(lambda x: str(x))  # Convert array-like elements to strings
    return col
  tmp_donor = tmp_donor.apply(convert_arrays_to_str)
  '''
  tmp_donor[tmp_donor.duplicated(subset='donor_id', keep=False)]
  '''
  # CSL
  tmp_donor = _donor_csl(client, tmp_donor)
  # Insert deduplicated copy
  donor = pd.concat([donor,tmp_donor.drop_duplicates().reset_index(drop=True)])"""

def _donor_calc_columns(trx, donor):
  #Sort transction file to account for first gifts on same date (largest gift_amount picked if same date) #added 8/6/25
  trx_sorted = trx.sort_values(['donor_key', 'gift_date', 'gift_amount'], ascending=[True, True, False])
  #Create df of first gifts
  ###firsts = trx_sorted.loc[trx_sorted.groupby('donor_key').gift_date.idxmin()] #Updated with below 8/6/25
  firsts = trx_sorted.drop_duplicates(subset='donor_key', keep='first')


  # Find the original column name that should be used to populate the "join_" version
  orig_cols = [x.replace('join_','').replace('first_','') for x in join_cols]

  # get the values for each field from the first record
  update_df = firsts[['donor_key']+orig_cols]

  # create the cy, fiscal, and month fields off of gift_date
  fiscal_month_start = get_schema(client)['firstMonthFiscalYear']
  update_df['cy'] = (update_df['gift_date'].dt.year).astype('Int64')
  update_df['fy'] = (fiscal_from_column(update_df,'gift_date',fiscal_month_start))

  # prepend "join_" 
  keys = list(set([col for col in update_df.columns]) - set(['donor_key','gift_date']))
  values = ['join_'+col for col in keys]
  dictionary = dict(zip(keys, values))
  dictionary['gift_date']='first_gift_date'
  update_df.rename(columns=dictionary,inplace=True)
  
  # update the working table using these firsts
  donor.set_index('donor_key', inplace=True)
  update_df.set_index('donor_key', inplace=True)
  donor.update(update_df, join='left', overwrite=True)
  donor.reset_index(inplace=True)

  # fix dtypes
  donor['join_fy']=donor['join_fy'].astype('Int64')
  donor['join_month']=donor['join_month'].astype('Int64')

  # add core donor definition (2+ years) 
  donor_lifespan = trx.groupby('donor_key')['gift_date'].agg({'min','max','count'}).reset_index()
  donor_lifespan['lifespan']=(((donor_lifespan['max'] - donor_lifespan['min']).dt.days)+1)/365
  donor_lifespan['core_donor']=donor_lifespan['lifespan']>=2
  donor_lifespan.drop(columns=['min'],inplace=True)
  donor_lifespan.rename(columns={'count':'gifts','max':'last_gift_date'},inplace=True)
  #Make last_gift_date just date
  donor_lifespan['last_gift_date'] = donor_lifespan['last_gift_date'].dt.date

  
  # add gift statistic columns #Added 5/9/25
  donor_gift_summary = trx.groupby('donor_key')['gift_amount'].agg({'min','max','mean'}).reset_index() #Added 5/9/25
  donor_gift_summary.rename(columns={'min':'min_gift_amount','max':'max_gift_amount','mean':'donor_average_gift'},inplace=True) #Added 5/9/25 

  #add major gift columns #Added 5/12/25
  major_gift_definition = 10000 #Updated 5/12
  donor_major_gift_summary = (
      trx[trx['gift_amount'] >= major_gift_definition]
      .groupby('donor_key')['gift_date']
      .agg(['min', 'max'])
      .reset_index()
      .rename(columns={'min': 'min_major_gift_date', 'max': 'max_major_gift_date'})
  )
  # Convert to plain dates
  donor_major_gift_summary['min_major_gift_date'] = donor_major_gift_summary['min_major_gift_date'].dt.date
  donor_major_gift_summary['max_major_gift_date'] = donor_major_gift_summary['max_major_gift_date'].dt.date


  # update the working table using these firsts
  donor.set_index('donor_key', inplace=True)
  donor_lifespan.set_index('donor_key', inplace=True)
  donor_gift_summary.set_index('donor_key', inplace=True) #Added 5/9/25
  donor_major_gift_summary.set_index('donor_key', inplace=True) #Added 5/12/25
  
  #Join calc column tables
  donor.update(donor_lifespan, join='left', overwrite=True)
  donor.update(donor_gift_summary, join='left', overwrite=True) #Added 5/9/25
  donor.update(donor_major_gift_summary, join='left', overwrite=True) #Added 5/12/25
  donor['major_gift_definition'] = major_gift_definition #Added 5/12/25
  donor.reset_index(inplace=True)

  #Add geographic donor columns (state_last_gift, zip_code_last_gift)
  #Add state_last_gift columns
  donor['state_last_gift'] = donor['donor_key'].map(
    trx.sort_values(['donor_key', 'gift_date', 'gift_amount'])
       .drop_duplicates('donor_key', keep='last')
       .set_index('donor_key')['state'])
  print("state_last_gift added to donor table")

  donor['zip_code_last_gift'] = donor['donor_key'].map(
    trx.sort_values(['donor_key', 'gift_date', 'gift_amount'])
       .drop_duplicates('donor_key', keep='last')
       .set_index('donor_key')['zip_code'])
  print("zip_code_last_gift added to donor table")

  donor = apply_client_donor_post_process(donor, trx, client)

  return donor


# COMMAND ----------

# DBTITLE 1,def _donor_csl
'''
client = 'NJH'
donor = tmp_donor
'''
def _donor_csl(client, donor):
  try:
    return apply_client_donor_csl(donor, client)
  except Exception as e:
    fn_name = "donor_csl"
    add_to_error_table(client, 
                   'Donor CSL Failed', 
                   'Donor', 
                   '{fn_name} Exception: {msg}'.format(fn_name=fn_name, msg=repr(e)), 
                   1, 
                   'warning')
    logger.error("Donor CSL Failed: %s", traceback.format_exc())
  return donor

# COMMAND ----------

# DBTITLE 1,def process_donor_table
# set client first, then load trx
'''
client = 'NJH'
trx = load_transactions(client)
'''
#########################
# donors 
#########################
import math
import time
from datetime import datetime

def process_donor_table(trx, client):
    logger.warning('******Donors Started')

    ########## Create Base Donor Table (deduplicated from trx)

    # create dataframe
    donor = pd.DataFrame(data=None, columns=donor_dims)
    donor = donor.astype(donor_def)

    # tmp donors
    tmp_donor = trx[donor_dims]

    # Make arrays into strings (guard for empty and non-list types)
    def convert_arrays_to_str(col):
        if len(col) > 0 and isinstance(col.iloc[0], (list, np.ndarray)):
            logger.warning('Converting array to str')
            return col.apply(lambda x: str(x))
        return col

    tmp_donor = tmp_donor.apply(convert_arrays_to_str)

    # CSL
    tmp_donor = _donor_csl(client, tmp_donor)

    # Insert deduplicated copy
    donor = pd.concat([donor, tmp_donor.drop_duplicates().reset_index(drop=True)])

    # Add Calc Cols
    donor = _donor_calc_columns(trx, donor)

    # add ts
    donor['data_processed_at'] = ts_now()

    # don't set key if there are missing donors and no duplicates
    missing = (donor['donor_key'].isna()).sum() > 0
    add_to_error_table(
        client,
        'Missing donor_ids',
        'Donor',
        f"{(donor['donor_key'].isna()).sum()} records with missing donors",
        missing,
        'warning'
    )

    dupes = (donor['donor_key'].duplicated()).sum() > 0
    add_to_error_table(
        client,
        'Duplicated donor_ids',
        'Donor',
        f"{dupes} donor_ids with conflicting/mismatched donor info",
        dupes,
        'error'
    )

    # load the client specific version
    table_name = f"{client.lower()}_donor"
    create_table_from_template('template_donor', table_name)

    if max(missing, dupes):
        drop_pk(table_name, 'donor_key')

    etl2_status_entry(client, 'BI Table Processing: Donors Processed')

    # -----------------------------------------------------------------------------------------------------------
    # Insert Donors to SQL Table (Delta: sql_import with chunked overwrite/append)
    logger.warning(f'[{datetime.now()}] Inserting Donors to SQL table')
    start_time = time.time()

    formatted = database_headers(donor)

    chunksize = 10_000
    total_rows = len(formatted)
    num_chunks = max(1, math.ceil(total_rows / chunksize))
    summary_every = 10  # <-- change cadence here

    logger.warning(
        f"Total donor rows: {total_rows:,}, Chunks: {num_chunks}, Summary cadence: every {summary_every} chunk(s)"
    )

    # rolling window stats
    win_times = []   # seconds per chunk in current window
    win_rows  = []   # rows inserted per chunk in current window

    for i, start in enumerate(range(0, total_rows, chunksize), start=1):
        end = start + chunksize
        chunk = formatted.iloc[start:end]

        chunk_start = time.time()
        try:
            mode = 'overwrite' if i == 1 else 'append'
            sql_import(chunk, table_name, overwrite_or_append=mode)
        except Exception:
            logger.exception(f"Donor chunk {i}/{num_chunks} failed after {time.time() - chunk_start:.2f}s")
            raise

        # collect stats
        chunk_elapsed = time.time() - chunk_start
        win_times.append(chunk_elapsed)
        win_rows.append(len(chunk))

        # summary every N chunks or at the very end
        if (i % summary_every == 0) or (i == num_chunks):
            w_chunks = len(win_times)
            w_time_total = sum(win_times)
            w_rows_total = sum(win_rows)
            w_avg_sec = w_time_total / max(w_chunks, 1)
            w_min, w_sec = divmod(int(w_avg_sec), 60)
            w_rps = w_rows_total / max(w_time_total, 1e-9)

            overall_elapsed = time.time() - start_time
            o_min, o_sec = divmod(int(overall_elapsed), 60)

            inserted_rows = min(i * chunksize, total_rows)
            overall_rps = inserted_rows / max(overall_elapsed, 1e-9)

            remaining_chunks = num_chunks - i
            avg_chunk_sec_so_far = overall_elapsed / max(i, 1)
            eta_seconds = int(remaining_chunks * avg_chunk_sec_so_far)
            e_min, e_sec = divmod(eta_seconds, 60)

            win_start_idx = i - w_chunks + 1
            win_end_idx = i

            logger.warning(
                f"[{datetime.now()}] Donor progress {win_start_idx}-{win_end_idx}/{num_chunks} chunks | "
                f"Window avg {w_min}m {w_sec}s per chunk | "
                f"Window throughput ~{w_rps:,.0f} rows/s | "
                f"Cumulative {o_min}m {o_sec}s | "
                f"Overall throughput ~{overall_rps:,.0f} rows/s | "
                f"ETA ~{e_min}m {e_sec}s"
            )

            win_times.clear()
            win_rows.clear()

    end_time = time.time()
    minutes, seconds = divmod(int(end_time - start_time), 60)

    logger.warning(
        f'[{datetime.now()}] Donor Insertion to SQL table Complete (took {minutes}m {seconds}s)'
    )
    etl2_status_entry(client, 'BI Table Processing: Donor Table Loaded')
    # -----------------------------------------------------------------------------------------------------------

    # qa
    logger.warning("Donor QA Run Started")
    donors_mismatch = trx['donor_key'].nunique() != donor['donor_key'].nunique()
    add_to_error_table(
        client,
        'Donor_key counts match Gift',
        'Donor',
        'Trx: ' + str(trx['donor_key'].nunique()) + ' vs Donor: ' + str(donor['donor_key'].nunique()),
        donors_mismatch,
        'error'
    )
    logger.warning("Donor QA Run Complete")

    logger.warning('******Donors Processed')
    return donor
  

# COMMAND ----------

# DBTITLE 1,*def process_bi_tables

def process_bi_tables(dfx, budget_df, client):
  print('process_bi_tables running')
  print('process_gift_table started')
  process_gift_table(dfx, client)
  print('process_gift_table complete')
  process_donor_table(dfx, client)
  print('process_donor_table complete')
  process_budget_table(budget_df, client)
  print('process_budget_table complete')
  print('query_qa_errors started')
  warnings, errors = query_qa_errors(client, ['Gift', 'Donor', 'Source Code'])
  print('query_qa_errors complete')

  if errors == 0:
      etl2_status_entry(client, 'Gift, Donor & SC Processing: Complete, ' + str(errors) + ' errors')
      logger.warning('Publishing to curated')
      publish_bi_tables(client)  # client = 'TCI'
      logger.warning('Published to curated')
      print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] Master tables: skipped (implement later)')
      # update_master(client)  # TODO: implement Spark SQL equivalent of etl2_master_table_update
    
  else:
    e = 'Transactions did not pass QA'
    etl2_status_entry(client,'Gift, Donor & SC Processing: FAILED, '+str(errors)+' errors')
    notification_exception_raised(client, code_location='process_bi_tables', error_message=e)
    raise Exception(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish

# COMMAND ----------

# DBTITLE 1,def update_max_gift_date
def update_max_gift_date(client):
  mgd_tbl = get_metadata_table_path('max_gift_dates')
  gift_tbl = get_curated_table_path(f"{client.lower()}_gift")
  try:
    sql_exec_only(f"""
      MERGE INTO {mgd_tbl} AS m
      USING (SELECT client, max(gift_date) AS mx FROM {gift_tbl} GROUP BY client) AS g
      ON m.client = g.client
      WHEN MATCHED THEN UPDATE SET m.curated = g.mx
      WHEN NOT MATCHED THEN INSERT (client, curated) VALUES (g.client, g.mx)
    """)
  except Exception:
    sql_exec_only(f"""
      INSERT INTO {mgd_tbl} (client, curated)
      SELECT client, max(gift_date) FROM {gift_tbl} GROUP BY client
    """)

def update_max_date(client, date, col_name):
  tbl = get_metadata_table_path('max_gift_dates')
  cl = str(client).strip() if client else ''
  try:
    sql_exec_only(f"UPDATE {tbl} SET {col_name} = '{date}' WHERE client = '{cl}'")
  except Exception:
    sql_exec_only(f"INSERT INTO {tbl} (client, {col_name}) VALUES ('{cl}', '{date}')")

# COMMAND ----------

# DBTITLE 1,def publish_bi_tables
def publish_bi_tables(client):
  # Gift
  publish_to_curated("{cl}_gift".format(cl=client.lower()))
  etl2_status_entry(client,'BI Table Processing: Gift published to curated')
  update_max_gift_date(client)
  
  # Donor
  publish_to_curated("{cl}_donor".format(cl=client.lower()))
  etl2_status_entry(client,'BI Table Processing: Donor published to curated')

  # Budget
  publish_to_curated("{cl}_budget".format(cl=client.lower()))
  etl2_status_entry(client,'BI Table Processing: Budget published to curated')

  apply_client_after_publish_bi_tables(client)

# COMMAND ----------

# DBTITLE 1,def update_master
# client = 'AFHU'

# def check_master_status(client):
#   script = '''
#   select count(*) outdated from max_gift_dates mgd 
#   where client = '{cl}' and coalesce(master,'1900-01-01')<curated
#   '''.format(cl=client)
#   needs_refresh = sql(script)['outdated'][0]>0
#   return needs_refresh

# from sqlalchemy import text
# from sqlalchemy.exc import SQLAlchemyError

# def update_master(client):
#   # Workaround sqlalchemy's default transaction behavior since we explicitly commit/rollback in the stored procedure itself
#   engine = connect_sqlalchemy(appname = 'update master',autocommit=True)
#   script = "exec dbo.etl2_master_table_update '{cl}'".format(cl=client)

#   try:
#     # Execute the stored procedure without wrapping it in a transaction (default sqlalchemy behavior)
#     with engine.connect() as conn:  
#       result = conn.execute(text(script))  
#     logger.warning('Master updated')

#     # Set the status entry
#     etl2_status_entry(client,'Master Table Processing: Master tables rebuilt for client')

#     print('Master Source Code, Gift, and Donor tables updated')

#   except SQLAlchemyError as e:
#     #print("SQL Error:", str(e.orig))
#     logger.error("Master Table Processing: %s", str(e.orig))
#     notification_exception_raised(client, code_location='update_master', error_message=str(e.orig))

#   except Exception as e:
#     logger.error("Master Table Processing: %s", traceback.format_exc())
#     notification_exception_raised(client, code_location='update_master', error_message=repr(e))


import traceback

# -----------------------------------
# Check if master tables need refresh
# -----------------------------------

def check_master_status(client):
    script = f"""
    select count(*) outdated
    from max_gift_dates mgd
    where client = '{client}'
      and coalesce(master, '1900-01-01') < curated
    """

    df = sql(script)

    # pandas-safe for newer versions (DBR 16.4+)
    needs_refresh = df["outdated"].iloc[0] > 0
    return needs_refresh


# -----------------------------------
# Run master table rebuild procedure
# -----------------------------------

def update_master(client):
    """TODO: implement Spark SQL equivalent of etl2_master_table_update. Azure SQL stored proc not used in Databricks/Delta."""
    logger.warning("update_master: skipped (TODO: Spark SQL equivalent of etl2_master_table_update)")







# COMMAND ----------

# MAGIC %md
# MAGIC ## Notify

# COMMAND ----------

# DBTITLE 1,def notification_exception_raised
# error_message = 'test error message'
def notification_exception_raised(client, code_location, error_message):
  # Add to ETL2 Monitoring
  etl2_status_entry(client,'[RAISED ERROR] {loc}: {msg}'.format(loc=code_location, msg=error_message))

  # Send Email
  body = """
  <h1>Base Table Gen Error: {cl}</h1>
  <p>{loc}: {e}</p>
  <br>
  <p>Quick Links</p>
  <ul>
    <li><a href="https://adb-1969403299592450.10.azuredatabricks.net/editor/notebooks/3327395016494432?o=1969403299592450#command/3498571594386095">Base Table Generation Code</a></li>
    <li><a href="https://portal.azure.com/#@fusefundraising.com/resource/subscriptions/af3498c3-b201-46bc-b478-f6d117ee11b4/resourceGroups/MSD_BI/providers/Microsoft.Storage/storageAccounts/msdstore/storagebrowser">Logs (Right click in the file row whitespace -> View/edit)</a></li>
  </ul>
  """.format(cl=client, loc=code_location, e=error_message)

  # Apply style from shared css
  body = inline_formatter(body)

  # Send email
  to, cc = data_team_recipients()
  send_email_from_html(to, "{cl} Base Table Gen Error".format(cl=client), body)

# COMMAND ----------

# DBTITLE 1,Review Function List
import inspect

# Replace my_function with the actual function name
print(inspect.getsource(create_sql_table))

# COMMAND ----------

# DBTITLE 1,Print Individual Function
import inspect

function_name = create_sql_table

# Replace my_function with the actual function name
print(inspect.getsource(function_name))

# COMMAND ----------

# MAGIC %md
# MAGIC # **Where Stuff Actually Runs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prep & Set Variables

# COMMAND ----------

# DBTITLE 1,**Set Client (from config; widget fallback when called from ETL2)
import ast
import pandas as pd
import warnings
import numpy as np
import sys

warnings.filterwarnings("ignore")

# Clients from config; fallback to widget when called from ETL2, else default
clients = get_clients_from_config()
if not clients:
  try:
    clients = ast.literal_eval(dbutils.widgets.get('client'))
  except Exception:
    clients = ['FS']
# Ensure list
if isinstance(clients, str):
  clients = [clients]

# First client used for schema setup / logging path (multi-client run logs to first)
client = clients[0]

# Set up logging
import logging
import traceback
log_file = '/dbfs/mnt/msd-datalake/logs/{client}_base_table_processing.log'.format(client=client)

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

etl2_status_entry(client,'Table generation started')

# COMMAND ----------

# DBTITLE 1,**Load Fuse Table Schemas (from repo schema/Fuse/table_schemas.json)

table_schema = get_table_schemas()

# Column Renames
column_renames = table_schema['ColumnRenames']

# Gift Table Schemas
gift_def = table_schema['GiftTable']
gift_dims = list(gift_def.keys())
create_sql_table('template_gift', get_table_schemas()['GiftTable'], primary_key='gift_key')

# Source Code
sc_def = table_schema['SourceCodeTable']
sc_dims = list(sc_def.keys())
create_sql_table('template_source_code', get_table_schemas()['SourceCodeTable'])

# Donor Table Schemas
donor_def = table_schema['DonorTable']
donor_dims = list(donor_def.keys())
create_sql_table('template_donor', get_table_schemas()['DonorTable'], primary_key='donor_key')

# Budget Table Schemas
budget_def = table_schema['BudgetTable']
budget_dims = list(budget_def.keys())
create_sql_table('template_budget', get_table_schemas()['BudgetTable'], primary_key='budg_sc_unique_code') 

# Set case sensitive columns - SQL Server COLLATE not supported in Spark/Delta. Skip for Databricks.

# Join Cols
join_cols = table_schema['JoinColumns']
etl2_status_entry(client,'Table schemas defined')

####################################
# Required Cols for each Client
####################################

for _client in clients:
  schema = get_schema(_client)
  cols = list(set(schema.get('FileHealth', {}).keys()) - {'EndofRptPeriod', 'calcDate'})

  # translate to db colnames
  tmp_df = pd.DataFrame(columns=cols)
  tmp_df = database_headers(tmp_df)
  tmp_df.columns = remove_client_prefix(tmp_df.columns, _client)
  tmp_df.rename(columns=column_renames, inplace=True)

  # format as table
  cols = pd.DataFrame(data=tmp_df.columns, columns=['column_name'])
  cols['client'] = _client

  # load to sql (delete from table, insert)
  tbl = get_metadata_table_path('etl2_column_qa')
  sql_exec_only(f"DELETE FROM {tbl} WHERE client = '{_client}'")
  sql_import(cols, 'etl2_column_qa', 'append')

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing

# COMMAND ----------

# DBTITLE 0,**Process
def main(client):
  """Always run full process. Workflow triggers on bronze update."""
  try:
    logger.warning('*** Start Main')
    schema = get_schema(client)

    # Step 1: Process Transaction & SC Tables
    dfx = process_transactions(client)
    logger.warning('*** process_transactions Complete')
    budget_df = budget_load_and_process_csv(client)
    logger.warning('*** budget_load_and_process_csv Complete')

    # Step 2: Process BI Tables
    process_bi_tables(dfx, budget_df, client)
    logger.warning('*** process_bi_tables Complete')

    logger.warning('*** Process Complete')
  
  # 1st: If there's a more detailed error (like sql syntax), report the more detailed version
  except ValueError as ve:
    logger.error("ValueError occurred: %s", ve)
    notification_exception_raised(client, code_location='main', error_message=repr(ve))
  
  # Otherwise, fallback to the general exception
  except Exception as e:
    logger.error("main: %s", traceback.format_exc())
    notification_exception_raised(client, code_location='main', error_message=repr(e))

for c in clients:
  main(c)
logging.shutdown()

# COMMAND ----------

# MAGIC %md 
# MAGIC #Testing

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

