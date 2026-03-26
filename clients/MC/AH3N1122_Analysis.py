# Databricks notebook source
'''
EDA
'''

# COMMAND ----------

import numpy as np
import os
import pandas as pd

import warnings
warnings.filterwarnings("ignore")


# COMMAND ----------

# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# MAGIC %run ../python_modules/utilities

# COMMAND ----------

# establish file storage directories
filemap = Filemap('MC')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# collect the promo files
dfs = []
_path = os.path.join(filemap.RAW, 'PromoFiles')
for promo in os.listdir(_path):
  print('file: ', promo)
  _df = pd.read_csv(os.path.join(_path, promo), encoding='ISO-8859-1')
  dfs.append(_df)
dfs = pd.concat(dfs)
print(dfs.shape)
dfs.head(2)

# COMMAND ----------

# Mercy Corps sends us promo files - we'd like to see the donors who received the sourcecode AH3N1122 
# and then match them up to any gifts made between 11/4/2022-12/31/2022. 
# We'd like to see codes and revenue associated with each gift made from those donors during that time. 

code = 'AH3N1122'
col = 'Assigned Appeal ID'
mask = dfs[col] == code
donors = dfs.loc[mask, 'Constituent ID'].unique()
print(len(donors))

mask = dfs['Constituent ID'].astype(int).isin(donors)
dfs = dfs.loc[mask]
print(dfs.shape)

set([type(x) for x in donors])

# COMMAND ----------

key_map = {
    "Gf_Import_ID" : "GiftID",
    "Gf_Date" : "GiftDate",
    "Gf_Amount" : "GiftAmount",
    "Gf_Type" : "GiftType",
    "Campaign ID" : "CampaignID",
    "Gf_Campaign" : "CampaignID",
    "Gf_Fund" : "FundID",
    "Gf_Appeal" : "CampaignCode",
    "Gf_Package" : "PackageID",
    "Gf_Pay_method" : "PayMethod",
    "Gf_Constit_Code" : "ConstituentCode",
    "Gf_CnBio_Key_Indicator" : "IndividualOrOrgFlag",
    "Gf_CnBio_ID" : "DonorID",
    "Gf_Apls_1_01_Ap_Appeal_category" : "AppealCategory",
    "Appeal_Subcategory" : "AppealSubCategory",
    "Appeal_Tertiary_Category" : "AppealTertiaryCategory",
    "Gf_SfCrdt_Amount" : "SoftCreditAmount",
    "Gf_SfCrdt_Constituent_ID" : "SoftCreditDonorID",
    "Marketing_Source_Code" : "SourceCode",
    "Segment1" : "SegmentCode",
    "Gift_Subtype" : "GiftSubtype",
    "Campaign Description" : "Program"
  }

# COMMAND ----------

f = 'MC Transactions 07.01.2022 thru 01.02.2023.csv'
df = pd.read_csv(os.path.join(filemap.RAW, f)).rename(columns=key_map)
print(df.shape)
df.GiftDate = pd.to_datetime(df.GiftDate)
mask = (df.GiftDate >= '2022-11-04') &  (df.GiftDate <= '2022-12-31')
df = df.loc[mask]
print(df.shape)

# COMMAND ----------


# df = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
# mask = (df.GiftDate >= '2022-11-04') &  (df.GiftDate <= '2022-12-31')
# df = df.loc[mask]

mask = df.DonorID.astype(int).isin(donors)
df = df.loc[mask]
df.shape

# COMMAND ----------

df.isna().sum() / df.shape[0]

# COMMAND ----------



# COMMAND ----------

# _ = [print(x) for x in sorted(dfs['Assigned Appeal Marketing Source Code'].dropna().unique())]
d = {
  'Assigned Appeal Marketing Source Code': 'OldSourceCode',
  'Assigned Appeal ID': 'CampaignCode',
  'Constituent ID': 'DonorID',
  'Assigned Appeal Marketing Segment': 'SegmentCode',
  'Assigned Package ID': 'PackageCode'
  }
dfs = dfs.rename(columns=d)
dfs.head().T

# COMMAND ----------

# dfs.OldSourceCode = dfs.CampaignCode+dfs.SegmentCode+dfs.PackageCode
# dfs.isna().sum()


# COMMAND ----------

df.isna().sum()

# COMMAND ----------

cols = ['DonorID', 'CampaignCode', 'OldSourceCode']
print(df.shape)
df.DonorID = df.DonorID.astype(int)
df = pd.merge(df, dfs[cols].drop_duplicates(), on=cols[:2], how='left')
print(df.shape)

# COMMAND ----------

df.isna().sum()

# COMMAND ----------

mask = df.CampaignCode.str.len() == 20
df.loc[mask]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df.to_csv(os.path.join(filemap.CURATED, 'MC_%s_Donors.csv' %code), index=False)

# COMMAND ----------

sel_donors = df.DonorID.astype(int).unique()
len(sel_donors)

# COMMAND ----------

len(set(sel_donors).intersection(set(sel_donors)^set(donors)))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md #Added task

# COMMAND ----------

# Hi @Joseph Katzenstein - Would it be possible to get the exact same report from you except this time, 
# we'd like to see the donors who received the sourcecode AH1N1121 & package code A4 (Midlevel) 
# and then match them up to any gifts made between 11/4/2021-12/31/2021. 

# COMMAND ----------

# collect the promo files
dfs = []
_path = os.path.join(filemap.RAW, 'PromoFiles')
for promo in os.listdir(_path):
  print('file: ', promo)
  _df = pd.read_csv(os.path.join(_path, promo), encoding='ISO-8859-1')
  dfs.append(_df)
dfs = pd.concat(dfs)
print(dfs.shape)
dfs.head(2)

# COMMAND ----------



code = 'AH1N1121'
c1 = 'Assigned Appeal ID'
c2 = 'Assigned Package ID'
c3 = 'Assigned Appeal Marketing Source Code'
mask = ((dfs[c1] == code) | (dfs[c3].str.startswith(code))) & (dfs[c2] == 'A4')
dfs = dfs.loc[mask]
donors = dfs['Constituent ID'].unique()
print(len(donors))
print(dfs.shape)

# mask = dfs['Constituent ID'].astype(int).isin(donors)
# dfs = dfs.loc[mask]
# print(dfs.shape)

set([type(x) for x in donors])

# COMMAND ----------

df = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
# mask = (df.GiftDate >= '2021-11-04') &  (df.GiftDate <= '2021-12-31')
# df = df.loc[mask]
# print(df.shape)

mask = df.DonorID.astype(int).isin(donors)
df = df.loc[mask]
print(df.shape)
mask = (df.GiftDate >= '2021-11-04') &  (df.GiftDate <= '2021-12-31')
df = df.loc[mask]
print(df.shape)

# COMMAND ----------

dfs

# COMMAND ----------

df.to_csv(os.path.join(filemap.CURATED, 'MC_%s_Donors.csv' %code), index=False)

# COMMAND ----------

