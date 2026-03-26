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

df['WeekOfYear'] = df.GiftDate.dt.isocalendar().week
df.WeekOfYear.describe()

# COMMAND ----------

df.tail()

# COMMAND ----------

df['is2022'] = df.GiftDate.dt.year == 2022
df.is2022.value_counts()

# COMMAND ----------

_df = df.groupby('DonorID').GiftDate.min().to_frame(name='FirstGiftDate')
df = pd.merge(df, _df, on='DonorID', how='left')
mask = df.GiftDate == df.FirstGiftDate
df = df.loc[mask].drop_duplicates('DonorID')
print(df.shape)

# COMMAND ----------

mask = (df.WeekOfYear.isin([i for i in range(9,18)])) & (df.GiftDate.dt.year == 2022)
ukraine_donors = df.loc[mask, 'DonorID'].unique()
len(ukraine_donors)

# COMMAND ----------

df.WeekOfYear.hist(bins=100)

# COMMAND ----------

df.groupby('is2022').WeekOfYear.hist(bins=100)

# COMMAND ----------

df.groupby('is2022').WeekOfYear.plot(kind='kde')
plt.xlim([0, 52])

# COMMAND ----------

df['GiftYear'] = df.GiftDate.dt.year
cols = ['GiftYear', 'WeekOfYear', 'is2022']
df= df[cols]
df.head()

# COMMAND ----------

_df = df.groupby('WeekOfYear').size().to_frame(name='NumPerWeek')
df = pd.merge(df, _df, on='WeekOfYear', how='left')
df.head()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df.GiftYear.describe()

# COMMAND ----------

df['AvgPerWeek'] = (df.NumPerWeek / (df.GiftYear.max() - df.GiftYear.min())) // 1
df.head()

# COMMAND ----------

_df = df.loc[df.is2022].groupby('WeekOfYear').size().to_frame(name='NumPerWeek2022')
df = pd.merge(df, _df, on='WeekOfYear', how='left')
df.head()

# COMMAND ----------

mask = df.WeekOfYear == 1
df.loc[mask]

# COMMAND ----------

df = df.drop_duplicates('WeekOfYear')
df.shape

# COMMAND ----------

plt.rcParams['figure.figsize'] = [18,10]
df.sort_values('WeekOfYear').set_index('WeekOfYear')[['AvgPerWeek', 'NumPerWeek2022']].plot(kind='bar')
plt.title('Mercy Corps - Joins per Week of Year, 22 Year Average vs. 2022')
plt.xlabel('Week Of Year')
plt.ylabel('New Joins')
plt.show()

# COMMAND ----------



# COMMAND ----------

df = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
print(df.shape)
_df = df.groupby('DonorID').GiftID.nunique().to_frame(name='NumGifts')
df = pd.merge(df, _df, on='DonorID', how='left')
print(df.shape)
mask = df.NumGifts > 1
df = df.loc[mask]
print(df.shape)

# COMMAND ----------

_df = df.groupby('DonorID').GiftDate.min().to_frame(name='FirstGiftDate')
df = pd.merge(df, _df, on='DonorID', how='left')
print(df.shape)

mask = df.GiftDate != df.FirstGiftDate
_df = df.loc[mask].groupby('DonorID').GiftDate.min().to_frame(name='Second_GiftDate')
df = pd.merge(df, _df, on='DonorID', how='left')
print(df.shape)

# COMMAND ----------

df['UkraineJoin'] = df.DonorID.isin(ukraine_donors)
df['UkraineJoin'] = (df.FirstGiftDate >= '2022-02-24') & (df.FirstGiftDate <= '2022-05-31')

# COMMAND ----------

def _dm_or_digital(df):
  feature = 'Channel'
  df[feature] = 'Other'

  ids = ['143', '145', '146']
  m1 = df.CampaignID.astype(str).isin(ids)
  m2 = (df.CampaignID.astype(str) == '160') & (df.AppealCategory == 'Direct Mail')  
  mask = (m1) | (m2)
  df[feature] = np.where(mask, 'DM', df[feature])

  ids = [
    '132', '133', '134',
    '135', '136', '137',
    '138', '139', '140',
    '142'
  ]
  mask = df.CampaignID.astype(str).isin(ids)
  df[feature] = np.where(mask, 'Digital', df[feature])

  return df

df = _dm_or_digital(df)
df.Channel.value_counts()

# COMMAND ----------

df['TimeToSecondGift'] = (df.Second_GiftDate - df.FirstGiftDate).dt.days
mask = df.TimeToSecondGift < 731
df = df.loc[mask].drop_duplicates('DonorID')

# COMMAND ----------


fig, ax = plt.subplots()
ax.hist(df['TimeToSecondGift'], bins=100, color='blue', alpha=.4, label='22 Year Data')
ax.legend(loc='upper left')
ax.set_ylabel('Count - 22 Year Data')
ax.set_xlabel('Days to Second Gift')

ax2 = ax.twinx()
ax2.hist(df.loc[df.UkraineJoin, 'TimeToSecondGift'], bins=100, color='orange', alpha=.8, label='Ukraine Period')
ax2.legend(loc='upper right')
ax2.set_ylabel('Count - Ukraine Joins')

plt.title('Days to Second Gift, 22 Years Data vs. Ukraine Period Joins')
plt.show()

# COMMAND ----------

fig, ax = plt.subplots()
mask = df.TimeToSecondGift < 731
df = df.loc[mask].drop_duplicates('DonorID')
mask = (df.UkraineJoin) & (df.Channel == 'DM')
ax.hist(df.loc[mask, 'TimeToSecondGift'], bins=100, color='blue', alpha=.4, label='DM')
ax.legend(loc='upper left')
ax.set_ylabel('Count - DM')
ax.set_xlabel('Days to Second Gift')

ax2 = ax.twinx()
mask = (df.UkraineJoin) & (df.Channel == 'Digital')
ax2.hist(df.loc[mask, 'TimeToSecondGift'], bins=100, color='orange', alpha=.8, label='Digital')
ax2.legend(loc='upper right')
ax2.set_ylabel('Count - Digital')

plt.title('Days to Second Gift of Ukraine Period Joins - DM vs. Digital')
plt.show()


# COMMAND ----------


df['JoinPeriod'] = None
mask = (df.FirstGiftDate >= '2020-02-24') & (df.FirstGiftDate <= '2020-05-31')
df.loc[mask, 'JoinPeriod'] = '2020Join'
mask = (df.FirstGiftDate >= '2021-02-24') & (df.FirstGiftDate <= '2021-05-31')
df.loc[mask, 'JoinPeriod'] = '2021Join'
df.loc[df.UkraineJoin, 'JoinPeriod'] = 'Ukraine'
mask = (df.FirstGiftDate >= '2023-02-24') & (df.FirstGiftDate <= '2023-05-31')
df.loc[mask, 'JoinPeriod'] = '2023Join'
df.JoinPeriod.value_counts()

       

# COMMAND ----------

grouped = df.dropna(subset=['JoinPeriod']).groupby('JoinPeriod')

fig, ax = plt.subplots()

for name, group in grouped:
    ax.hist(group['TimeToSecondGift'], bins=100, label=name, alpha=0.5)

ax.set_xlabel('Days To Second Gift')
ax.set_ylabel('Count')
ax.legend()
plt.title('Days To Second Gift by Join Period')
plt.show()

# COMMAND ----------

fig, ax = plt.subplots()

mask = (df.JoinPeriod == '2020Join') & (df.Channel == 'DM')
ax.hist(df.loc[mask, 'TimeToSecondGift'], bins=100, color='blue', alpha=.4, label='DM')
ax.legend(loc='upper left')
ax.set_ylabel('Count - DM')
ax.set_xlabel('Days to Second Gift')

ax2 = ax.twinx()
mask = (df.JoinPeriod == '2020Join') & (df.Channel == 'Digital')
ax2.hist(df.loc[mask, 'TimeToSecondGift'], bins=100, color='orange', alpha=.8, label='Digital')
ax2.legend(loc='upper right')
ax2.set_ylabel('Count - Digital')

plt.title('Days to Second Gift of 2020 Joins - DM vs. Digital')
plt.show()

# COMMAND ----------

fig, ax = plt.subplots()

mask = (df.JoinPeriod == '2021Join') & (df.Channel == 'DM')
ax.hist(df.loc[mask, 'TimeToSecondGift'], bins=100, color='blue', alpha=.4, label='DM')
ax.legend(loc='upper left')
ax.set_ylabel('Count - DM')
ax.set_xlabel('Days to Second Gift')

ax2 = ax.twinx()
mask = (df.JoinPeriod == '2021Join') & (df.Channel == 'Digital')
ax2.hist(df.loc[mask, 'TimeToSecondGift'], bins=100, color='orange', alpha=.8, label='Digital')
ax2.legend(loc='upper right')
ax2.set_ylabel('Count - Digital')

plt.title('Days to Second Gift of 2021 Joins - DM vs. Digital')
plt.show()

# COMMAND ----------

fig, ax = plt.subplots()

mask = (df.JoinPeriod == 'Ukraine') & (df.Channel == 'DM')
ax.hist(df.loc[mask, 'TimeToSecondGift'], bins=100, color='blue', alpha=.4, label='DM')
ax.legend(loc='upper left')
ax.set_ylabel('Count - DM')
ax.set_xlabel('Days to Second Gift')

ax2 = ax.twinx()
mask = (df.JoinPeriod == 'Ukraine') & (df.Channel == 'Digital')
ax2.hist(df.loc[mask, 'TimeToSecondGift'], bins=100, color='orange', alpha=.8, label='Digital')
ax2.legend(loc='upper right')
ax2.set_ylabel('Count - Digital')

plt.title('Days to Second Gift of Ukraine Joins - DM vs. Digital')
plt.show()

# COMMAND ----------

fig, ax = plt.subplots()

mask = (df.JoinPeriod == '2023Join') & (df.Channel == 'DM')
ax.hist(df.loc[mask, 'TimeToSecondGift'], bins=100, color='blue', alpha=.4, label='DM')
ax.legend(loc='upper left')
ax.set_ylabel('Count - DM')
ax.set_xlabel('Days to Second Gift')

ax2 = ax.twinx()
mask = (df.JoinPeriod == '2023Join') & (df.Channel == 'Digital')
ax2.hist(df.loc[mask, 'TimeToSecondGift'], bins=100, color='orange', alpha=.8, label='Digital')
ax2.legend(loc='upper right')
ax2.set_ylabel('Count - Digital')

plt.title('Days to Second Gift of 2023 Joins - DM vs. Digital')
plt.show()

# COMMAND ----------

