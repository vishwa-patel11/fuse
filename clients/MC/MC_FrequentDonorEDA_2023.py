# Databricks notebook source
'''
EDA
'''

# COMMAND ----------

import numpy as np
import os
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

import warnings
warnings.filterwarnings("ignore")


# COMMAND ----------

# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# MAGIC %run ../python_modules/utilities

# COMMAND ----------

# establish file storage directories
filemap = Filemap('MC')
os.listdir(filemap.STAGED)

# COMMAND ----------

f = 'StagedForFH_wFilters.csv'
df = pd.read_csv(os.path.join(filemap.STAGED, f))
print(df.shape)

df = df.loc[~df.MC_Donor984126]
print(df.shape)


print(df.shape)

df.head().T

# COMMAND ----------

print(df.shape)
df = df.drop_duplicates()
print(df.shape)
df['GiftID'] = [i for i in range(len(df))]
print(df.shape)
print(df.GiftID.max())

# COMMAND ----------

df.GiftDate = pd.to_datetime(df.GiftDate)
df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', 7)

mask = df.GiftFiscal == 2023
df = df.loc[mask]
df.GiftDate.describe(datetime_is_numeric=True)

# COMMAND ----------

d = {'GiftAmount': 'sum', 'GiftID': 'nunique'}
_df = df.groupby('DonorID').agg(d)

d = {'GiftAmount': 'TotalRevenue', 'GiftID': 'NumGifts'}
_df = _df.rename(columns=d).reset_index()

df = pd.merge(df, _df, on='DonorID', how='left')
print(df.shape)
df.head().T

# COMMAND ----------

cols = ['DonorID', 'GiftDate']
df = df.sort_values(cols)
df['DateDelta'] = df.GiftDate.diff().dt.days

mask = df.DonorID != df.DonorID.shift(1)
df.loc[mask, 'DateDelta'] = 0

mask = df.NumGifts > 1
df.loc[mask].tail(10)

# COMMAND ----------

df.sort_values(['TotalRevenue', 'NumGifts'], ascending=False)
mask = df.DonorID.astype(str) == '158481'
df.loc[mask, 'GiftDate'].describe(datetime_is_numeric=True)

# COMMAND ----------

df.loc[~mask].sort_values(['TotalRevenue', 'NumGifts'], ascending=False)

# COMMAND ----------

df.NumGifts.describe()

# COMMAND ----------

cols = ['NumGifts', 'TotalRevenue']
mask = df.NumGifts < 100
df.loc[mask].sort_values('NumGifts').drop_duplicates('DonorID').plot(x='NumGifts', y='TotalRevenue', kind='scatter')
plt.show()

# COMMAND ----------

_df = df.groupby('DonorID').DateDelta.mean().to_frame(name='AvgDateDelta')
print(df.shape)
df = pd.merge(df, _df, on='DonorID', how='left')
print(df.shape)

# COMMAND ----------

df.NumGifts = np.round(df.NumGifts / 2) * 2
df.AvgDateDelta = np.round(df.AvgDateDelta / 2) * 2

# COMMAND ----------

import pandas as pd
import seaborn as sns

# Create a pivot 
# mask = df.AvgDateDelta < 370 
mask = (df.TotalRevenue < 1e4) & (df.AvgDateDelta < 370) #& (df.LastGiftDate >= '2022-07-01')
pivot_table = df.loc[mask].drop_duplicates('DonorID').pivot_table(index='NumGifts', columns='AvgDateDelta', values='TotalRevenue')
# pivot_table = df.drop_duplicates('DonorID').pivot_table(index='NumGifts', columns='AvgDateDelta', values='TotalRevenue')




# COMMAND ----------

# Create a heatmap using seaborn
plt.figure(figsize=(10, 6))
sns.heatmap(pivot_table, cmap='YlGnBu')#, annot=True, fmt='.1f', linewidths=.5)
plt.title('MC - Total Revenue by Average Gift Interval and Number of Gifts - FY23')
plt.show()

# COMMAND ----------

print(df.DonorID.nunique())
print(df.GiftAmount.sum())

# COMMAND ----------

mask = (df.NumGifts >= 10) & (df.NumGifts <= 32) & (df.AvgDateDelta >= 10) & (df.AvgDateDelta <= 30)
revenue = df.loc[mask].GiftAmount.sum()
donors = df.loc[mask].DonorID.nunique()
print('revenue: ', revenue)
print('donors: ', donors)
print(revenue/donors)
print(revenue / df.GiftAmount.sum())
print(donors/df.DonorID.nunique())

# COMMAND ----------

revenue = df.loc[~mask].GiftAmount.sum()
donors = df.loc[~mask].DonorID.nunique()
print('revenue: ', revenue)
print('donors: ', donors)
print(revenue/donors)
print(revenue / df.GiftAmount.sum())
print(donors/df.DonorID.nunique())

# COMMAND ----------

6.7/22.7

# COMMAND ----------

mask = df.DonorID == 1067990
df.loc[mask]

# COMMAND ----------



# COMMAND ----------

