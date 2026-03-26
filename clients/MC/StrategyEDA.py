# Databricks notebook source
import pandas as pd
import numpy as np
import os
import json

import seaborn as sns
import matplotlib.pyplot as plt

from datetime import datetime

# COMMAND ----------

# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# MAGIC %run ../python_modules/utilities

# COMMAND ----------

filemap=Filemap('MC')

# COMMAND ----------

os.listdir(filemap.STAGED)

# COMMAND ----------

df = pd.read_csv(os.path.join(filemap.STAGED, 'StagedForFH_wFilters.csv'))
print(df.shape)
df.head().T

# COMMAND ----------

df['GiftID'] = [x for x in range(len(df))]
df.GiftDate = pd.to_datetime(df.GiftDate)
df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', 7)
d_agg = {
  'GiftAmount': 'sum',
  'GiftID': 'nunique',
  'DonorID': 'nunique'
}
d_names = {
  'GiftAmount': 'Revenue',
  'GiftID': 'Gifts',
  'DonorID': 'Donors'
}
_df = df.groupby('GiftFiscal').agg(d_agg).rename(columns=d_names)
_df.to_csv(os.path.join(filemap.CURATED, 'MC_RevenueGiftsDonors_AllTime.csv'))
_df

# COMMAND ----------

mask = df.GiftDate.dt.month.isin([7,8,9,10])
d_agg = {
  'GiftAmount': 'sum',
  'GiftID': 'nunique',
  'DonorID': 'nunique'
}
d_names = {
  'GiftAmount': 'Revenue',
  'GiftID': 'Gifts',
  'DonorID': 'Donors'
}
_df = df.loc[mask].groupby('GiftFiscal').agg(d_agg).rename(columns=d_names)
_df.to_csv(os.path.join(filemap.CURATED, 'MC_RevenueGiftsDonors_AllTime_FYTDthroughOctober.csv'))
_df

# COMMAND ----------

df.columns

# COMMAND ----------

codes = ['DAF', 'GPMG']
mask = df.CampaignCode.isin(codes)
df.loc[mask].groupby([df.GiftDate.dt.year, 'CampaignCode']).GiftAmount.sum()

# COMMAND ----------

with open(os.path.join(filemap.SCHEMA, 'schema.json'), 'r') as f:
  schema = json.load(f)
schema['Suppressions']

# COMMAND ----------

def dropna(df, col, values, inc):
  df['mask'] = [str(x) in values for x in df[col].values.tolist()]
  df = df.dropna(subset=['mask'])
  return df[df['mask']]

# COMMAND ----------

df = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
print(df.shape)
df.head().T

# COMMAND ----------

df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', 7)
df.GiftFiscal.value_counts()

# COMMAND ----------

sup = schema['Suppressions']
col = list(sup.keys())[0]
col

# COMMAND ----------

values = list(sup.values())[0]['Values']
values

# COMMAND ----------

print(df.shape)
df = dropna(df, col, values, True)
print(df.shape)


# COMMAND ----------

codes = ['DAF', 'GPMG']
mask = df.CampaignCode.isin(codes)
df.loc[mask].groupby(['GiftFiscal', 'CampaignCode']).GiftAmount.sum()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df = df.sort_values(by=['DonorID', 'GiftDate'])
df['Recency'] = df.GiftDate.diff().dt.days
mask = df.DonorID != df.shift().DonorID
df.loc[mask, 'Recency'] = 0
df.head(20).T

# COMMAND ----------

# # Recency
# _df = df.groupby('DonorID')['GiftDate'].max().to_frame(name='MostRecentGiftDate')
# df = pd.merge(df, _df, on='DonorID', how='left')
# df['Recency'] = (datetime.now() - df.MostRecentGiftDate).dt.days
# df.Recency.describe()

# COMMAND ----------

# Frequency
_df = df.groupby('DonorID')['GiftID'].nunique().to_frame(name='Frequency')
df = pd.merge(df, _df, on='DonorID', how='left')
df.Frequency.describe()

# COMMAND ----------

# Monetary
_df = df.groupby('DonorID')['GiftAmount'].max().to_frame(name='Monetary')
df = pd.merge(df, _df, on='DonorID', how='left')
df.Monetary.describe()

# COMMAND ----------



# COMMAND ----------

mask = df.GiftFiscal == 2023
_df = df.loc[mask].drop_duplicates('DonorID')


# COMMAND ----------

mask = _df.Frequency < 300
_df.loc[mask].Frequency.hist(bins=50)

# COMMAND ----------

mask = _df.Frequency < 50
_df.loc[mask].Frequency.hist(bins=25)

# COMMAND ----------

_df.Recency.hist(bins=50)

# COMMAND ----------

mask = _df.Monetary < 2e3
_df.loc[mask].Monetary.hist(bins=30)

# COMMAND ----------

mask = _df.Monetary < 600
_df.loc[mask].Monetary.hist(bins=50)

# COMMAND ----------

# Total FY23 Giving
grouped = _df.groupby('DonorID')['GiftAmount'].sum().to_frame(name='TotalGiving')
_df = pd.merge(_df, grouped, on='DonorID', how='left')
_df.TotalGiving.describe()

# COMMAND ----------

mask = _df.TotalGiving < 2000
_df.loc[mask].TotalGiving.hist(bins=30)


# COMMAND ----------

mask = _df.TotalGiving < 500
_df.loc[mask].TotalGiving.hist(bins=30)

# COMMAND ----------

COLOR = 'YlGnBu'
mask = (_df.Recency < 1000) & (_df.Frequency < 10)# & (_df.TotalGiving < 300)
grouped = _df.loc[mask].groupby(['Recency', 'Frequency'])
avg_total = grouped.TotalGiving.sum()
avg_total = avg_total.reset_index().pivot(index='Recency', columns='Frequency', values='TotalGiving')
sns.heatmap(avg_total,cmap=COLOR)
plt.show()

# COMMAND ----------

avg_total

# COMMAND ----------

mask = (_df.Recency < 1000) & (_df.Monetary < 500) & (_df.TotalGiving < 300)
grouped = _df.loc[mask].groupby(['Recency', 'Monetary'])
avg_total = grouped.TotalGiving.mean()
avg_total = avg_total.reset_index().pivot(index='Recency', columns='Monetary', values='TotalGiving')
sns.heatmap(avg_total, cmap='RdBu')
plt.show()

# COMMAND ----------

mask = (_df.Monetary < 300) & (_df.TotalGiving < 300)
grouped = _df.loc[mask].groupby(['Recency', 'Monetary'])
avg_total = grouped.TotalGiving.mean()
avg_total = avg_total.reset_index().pivot(index='Recency', columns='Monetary', values='TotalGiving')
sns.heatmap(avg_total, cmap=COLOR)
plt.show()

# COMMAND ----------

mask = (_df.Frequency < 50) & (_df.Monetary < 500) & (_df.TotalGiving < 1000)
grouped = _df.loc[mask].groupby(['Frequency', 'Monetary'])
avg_total = grouped.TotalGiving.mean()
avg_total = avg_total.reset_index().pivot(index='Frequency', columns='Monetary', values='TotalGiving')
sns.heatmap(avg_total, cmap=COLOR)
plt.show()

# COMMAND ----------

df.head().T

# COMMAND ----------

df.AppealSubCategory.unique()

# COMMAND ----------

