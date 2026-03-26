# Databricks notebook source
import numpy as np
import pandas as pd
import os

from pyspark.sql.functions import isnan
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# MAGIC %run ../python_modules/utilities

# COMMAND ----------

filemap = Filemap('MC')

# COMMAND ----------

def get_unique_from_spark_col(df, col):
  return df.select(F.collect_set(col).alias(col)).first()[col]

# COMMAND ----------

os.listdir(os.path.join(filemap.RAW, 'PromoFiles'))

# COMMAND ----------

# get CONS_GIVING donors

df = pd.read_csv(os.path.join(filemap.RAW, 'PromoFiles', 'Offline promos FY23 to date.csv'))
print(df.shape)
df.head().T 

# COMMAND ----------

donors = df['Constituent ID'].unique()
print(len(donors))
del df

# COMMAND ----------

df = pd.read_csv(os.path.join(filemap.STAGED, 'StagedForFH_wFilters.csv'))
print(df.shape)
df.head().T

# COMMAND ----------

df.GiftDate = pd.to_datetime(df.GiftDate)
df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', 7)
df.GiftFiscal.describe()

# COMMAND ----------

mask = df.GiftFiscal == 2023
df = df.loc[mask]
df.GiftFiscal.unique()

# COMMAND ----------

df['DM_Audience'] = df.DonorID.isin(donors)
df.DM_Audience.value_counts()

# COMMAND ----------

sorted(df.AppealCategory.unique())

# COMMAND ----------

not_dm = [x for x in df.AppealCategory.unique() if 'Direct Mail' not in x]
not_dm

# COMMAND ----------

mask = (df.DM_Audience) & (df.AppealCategory.isin(not_dm))
_df = df.loc[df.DM_Audience].groupby('AppealCategory')['GiftAmount'].sum().to_frame(name='Revenue')
_df = _df.sort_values(by='Revenue', ascending=False)
_df.to_csv(os.path.join(filemap.CURATED, 'MC_FY23_DMAudienceRevenueBreakdown.csv'))
_df

# COMMAND ----------

mask = (df.DM_Audience) & (df.AppealCategory.isin(not_dm))
_df = df.loc[df.MC_UkraineJoin].groupby(['AppealCategory', 'MC_AppealSubCategory'])['GiftAmount'].sum().to_frame(name='Revenue')
# _df = df.loc[df.MC_UkraineJoin].groupby(['MC_AppealSubCategory'])['GiftAmount'].sum().to_frame(name='Revenue')
_df = _df.sort_values(by='Revenue', ascending=False)
_df.to_csv(os.path.join(filemap.CURATED, 'MC_FY23_UkraineJoinRevenueBreakdown.csv'))
_df

# COMMAND ----------

sorted(df.MC_AppealSubCategory.unique())

# COMMAND ----------

