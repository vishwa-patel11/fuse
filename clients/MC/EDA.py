# Databricks notebook source
'''
EDA
'''

# COMMAND ----------

import numpy as np
import os
import pandas as pd

import matplotlib.pyplot as plt

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

df = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
print(df.shape)

# COMMAND ----------

codes = ['QQ1N1023', 'QQ2N1023']
mask = (df.CampaignCode.isin(codes))# & (df.PackageID == 'QE') #(df.SourceCode.isna())
df = df.loc[mask]
df

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

# rename the columns
m = {
  'Constituent ID': 'DonorID',
  'Marketing Finder Number': 'Finder_Number',
  'Assigned Appeal Date': 'MailDate',
  'Assigned Appeal ID': 'CampaignCode',
  'Assigned Package ID': 'PackageCode',
  'Assigned Appeal Marketing Segment': 'ListCode',
  'Assigned Appeal Marketing Source Code': 'SourceCode',
  'Assigned Package Description': '_PackageName'
}
dfs = dfs.rename(columns=m)
dfs.MailDate = pd.to_datetime(dfs.MailDate)

# take just the relevant campaigns
m1 = dfs.CampaignCode.str.startswith(('AG', 'AH', 'AN', 'AW', 'QQ'))
m2 = dfs.MailDate.dt.year > 2021
mask = (m1) & (m2)
dfs = dfs.loc[mask]

print(dfs.shape)
dfs.head(2)

# COMMAND ----------

cols = ['DonorID', 'CampaignCode']#, 'Finder_Number']
df.DonorID = df.DonorID.astype(int)
print(df.DonorID.dtypes)
print(dfs.DonorID.dtypes)

df_donors = set(df.DonorID.unique())
mask = dfs.CampaignCode.isin(codes)
dfs_donors = set(dfs.loc[mask].DonorID.unique())

comp = df_donors ^ dfs_donors
print(len(comp))
print(len(df_donors.intersection(comp)))
print(len(dfs_donors.intersection(comp)))

# COMMAND ----------


mask = df.DonorID.isin(df_donors.intersection(comp))
df.loc[mask, 'GiftAmount'].sum()
# df.DonorID = df.DonorID.astype(int)
# df = pd.merge(df, dfs, on=cols, how='inner')



# COMMAND ----------

df.GiftAmount.sum()

# COMMAND ----------

df.isna().sum()

# COMMAND ----------

mask = dfs.CampaignCode == code
dfs.loc[mask].isna().sum()

# COMMAND ----------

