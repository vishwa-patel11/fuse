# Databricks notebook source
import numpy as np
import pandas as pd
import os

# COMMAND ----------

# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# MAGIC %run ../python_modules/utilities

# COMMAND ----------

filemap = Filemap('MC')

# COMMAND ----------

df = pd.read_csv(os.path.join(filemap.CURATED, 'MC_CampPerf_Data3.csv'))
print(df.shape)
df.head().T

# COMMAND ----------

df.GiftDate = pd.to_datetime(df.GiftDate)
df.GiftDate.describe()

# COMMAND ----------

df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', 7)
df.GiftFiscal.describe()

# COMMAND ----------

mask = df.GiftFiscal == 2023
df.loc[mask]['SourceCode'].str[6:8].unique()

# COMMAND ----------

mask = df.SourceCode.str.contains('23')
df.loc[mask].GiftDate.describe()

# COMMAND ----------

mask = (df.GiftDate >= '2022-07-01') & (df.GiftDate < '2023-01-01')
df.loc[mask].SourceCode.str[6:8].unique()

# COMMAND ----------

mask = df.SourceCode.str.contains('23')
df.loc[mask].SourceCode.str[6:8].unique()


# COMMAND ----------

mask = df.GiftFiscal == 2023
df = df.loc[mask]
df.shape

# COMMAND ----------

df.GiftDate.describe()

# COMMAND ----------

mask = df.MonetaryCode.str.len() > 1
_df = df.loc[mask].groupby('MonetaryCode')['GiftAmount'].sum().sort_values(ascending=False).to_frame(name='Revenue')
_df

# COMMAND ----------

mask = df.MonetaryCode.str.len() > 1
_df = df.loc[mask].groupby('MonetaryCode')['GiftAmount'].sum().to_frame(name='Revenue')
_df.plot(kind='barh')

# COMMAND ----------

mask = df.RecencyCode.str.len() > 1
_df = df.loc[mask].groupby('RecencyCode')['GiftAmount'].sum().sort_values(ascending=False).to_frame(name='Revenue')
_df

# COMMAND ----------

mask = df.RecencyCode.str.len() > 1
cols = ['RecencyCode', 'GiftAmount']
_df = df.loc[mask].groupby('RecencyCode')['GiftAmount'].sum().to_frame(name='Revenue')
_df.plot(kind='barh')

# COMMAND ----------

mask = _df.Revenue < 1e6
_df.loc[mask].plot(kind='barh')

# COMMAND ----------

df.GiftDate = pd.to_datetime(df.GiftDate)
df.GiftDate.describe()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
print(df.shape)
df.head().T

# COMMAND ----------

mask = (df.GiftDate >= '2022-07-01') & (df.GiftDate < '2023-01-01') & (df.CampaignCode.str[6:8] == '23')
df.loc[mask].isna().sum() / df.loc[mask].shape[0]

# COMMAND ----------

df.loc[mask].CampaignCode.unique()

# COMMAND ----------

mask = (df.GiftDate >= '2022-07-01') & (df.GiftDate < '2023-01-01') & (df.CampaignCode != 'WEW00723')
sorted(df.loc[mask].CampaignCode.str[6:8].unique())

# COMMAND ----------

mask = (df.GiftDate >= '2022-07-01') & (df.GiftDate < '2023-01-01')
df.loc[mask].GiftAmount.sum()

# COMMAND ----------

