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

df = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
print(df.shape)

# COMMAND ----------

cols = [
  'PayMethod', 'AppealSubCategory',
  'AppealTertiaryCategory',
  'CampaignID', 'GiftSubtype', 'GiftType', 
  'AppealCategory'
  ]

for col in cols:
  print(col)
  print(sorted(df[col].fillna(value='None').unique()))
  print('*'*50)

# COMMAND ----------

mask = df.GiftType == 'Recurring Gift Pay Cash'
sust_donors = df.loc[mask, 'DonorID'].unique()
df['GaveSustainerGift'] = df.DonorID.isin(sust_donors)
df.GaveSustainerGift.value_counts()

# COMMAND ----------

'''
Six Join Group groupings:
•	Direct Mail Ukraine gave First Gift is to Direct Mail, and is between February 24 – May 31, 2022
•	Direct Mail FY21 gave First Gift is to Direct Mail in February 24 – May 31, 2021
•	Direct Mail FY20 gave First Gift is to Direct Mail in February 24 – May 31, 2020
•	Digital Ukraine gave First Gift is to Digital in February 24 – May 31, 2022
•	Digital FY21 gave First Gift is to Digital in February 24 – May 31, 2021
•	Digital FY20 gave First Gift is to Digital in February 24 – May 31, 2020
'''


# COMMAND ----------

'''
@Joseph Katzenstein upon discussing this with Lillie & Courtney, I am providing some additional context as well as making a formal request for a data file. The sustainer stuff in the queue takes priority, but if we can get the following data request by October 20th that would be great.

The goal of this analysis is to compare giving behavior of donors who joined around an emergency to that of donors who joined during non-emergency, and we want to be able to distinguish donors who joined with either a gift coded to Direct Mail (Campaign ID = 143, 145, 146, 158 & 160 when Appeal Category = Direct Mail), or a gift coded to Digital (Campaign ID = 132-140 & 142).

Please create a file of all transactions received to-date from these donors.  File should contain the following fields for each row:
·         Donor ID
·         Gift ID
·         Gift Amount
·         Gift Date
·         Join Group (one of the 6 listed above)
·         Join Gift where “True” when gift is the 1st by a unique donor, otherwise “False”
·         Join Amount
·         Gave 2nd Gift where “True” if donor made any additional gift, otherwise “False”
·         2nd Gift where “True” when a gift is 2nd by a unique donor, or otherwise “False”
·         2nd Gift Amount
·         Time to 2nd Gift where value is days to 2nd gift, or field is NULL for single gift givers
·         Gave After 1 Year where “True” if donor has any gift 365+ days after join gift
·         Gift Within 267 Days where “True” if gift is received within first 267 days after join.
·         Gift Within 268-461 where “True” if gifts received in days 268 to 461 after join. (This will allow us to see impact of Ukraine on 2021 joins specifically)
·         Total Gifts which is count of donor’s total gifts made
·         Campaign ID
·         Appeal Category
'''

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

_df = df.groupby('DonorID')['GiftDate'].min().to_frame(name='FirstGiftDate')
print(df.shape)
df = pd.merge(df, _df, on='DonorID', how='left')
print(df.shape)

mask = (df.GiftDate == df.FirstGiftDate)
_df = df.loc[mask].drop_duplicates('DonorID')
_df['JoinChannel'] = _df['Channel']
cols = ['DonorID', 'JoinChannel']
df = pd.merge(df, _df[cols], on=cols[0], how='left')
print(df.shape)
df.JoinChannel.value_counts()

# COMMAND ----------

mask = (df.GiftDate == df.FirstGiftDate)
_df = df.loc[mask].drop_duplicates('DonorID')
_df['GaveFirstSustainerGift'] = _df.GiftType == 'Recurring Gift Pay Cash'
cols = ['DonorID', 'GaveFirstSustainerGift']
print(df.shape)
df = pd.merge(df, _df[cols], on=cols[0], how='left')
print(df.shape)
df.GaveFirstSustainerGift = df.GaveFirstSustainerGift.fillna(value=False)
df.GaveFirstSustainerGift.value_counts()

# COMMAND ----------

mask = df.GiftType == 'Recurring Gift Pay Cash'
_df = df.loc[mask].sort_values('GiftDate').drop_duplicates('DonorID')
_df['FirstSustainerGiftAppealCategory'] = _df.AppealCategory
cols = ['DonorID', 'FirstSustainerGiftAppealCategory']
print(df.shape)
df = pd.merge(df, _df[cols], on=cols[0], how='left')
print(df.shape)

# COMMAND ----------

df.loc[df.GaveFirstSustainerGift].JoinChannel.unique()

# COMMAND ----------

mask = (df.JoinChannel.isin(['DM', 'Digital'])) | (df.GaveFirstSustainerGift)
df = df.loc[mask]
print(df.shape)

# COMMAND ----------

def _join_group(df):
  '''
  Six Join Group groupings:
  •	Direct Mail Ukraine gave First Gift is to Direct Mail, and is between February 24 – May 31, 2022
  •	Direct Mail FY21 gave First Gift is to Direct Mail in February 24 – May 31, 2021
  •	Direct Mail FY20 gave First Gift is to Direct Mail in February 24 – May 31, 2020
  •	Digital Ukraine gave First Gift is to Digital in February 24 – May 31, 2022
  •	Digital FY21 gave First Gift is to Digital in February 24 – May 31, 2021
  •	Digital FY20 gave First Gift is to Digital in February 24 – May 31, 2020
  '''
  feature = 'JoinGroup'
  df[feature] = None
  m1 = (df.JoinChannel == 'DM')
  m2 = (df.JoinChannel == 'Digital')

  m3 = (df.FirstGiftDate >= '2022-02-24') & (df.FirstGiftDate <= '2022-05-31')
  df.loc[m1&m3, feature] = 'DM-Ukraine'
  df.loc[m2&m3, feature] = 'Digital-Ukraine'
  
  m3 = (df.FirstGiftDate >= '2021-02-24') & (df.FirstGiftDate <= '2021-05-31')
  df.loc[m1&m3, feature] = 'DM-FY21'
  df.loc[m2&m3, feature] = 'Digital-FY21'
  
  m3 = (df.FirstGiftDate >= '2020-02-24') & (df.FirstGiftDate <= '2020-05-31')
  df.loc[m1&m3, feature] = 'DM-FY20'
  df.loc[m2&m3, feature] = 'Digital-FY20'

  return df

df = _join_group(df)
print(df.JoinGroup.isna().sum())
df.JoinGroup.value_counts()


# COMMAND ----------

mask = df.JoinGroup.notna()
print(df.shape)
df = df.loc[mask]
print(df.shape)

# COMMAND ----------

# join gift
_df = df.sort_values('GiftID').drop_duplicates(['DonorID', 'GiftDate'])
_df['JoinGift'] = _df.GiftDate == _df.FirstGiftDate
cols = ['GiftID', 'JoinGift']
df = pd.merge(df, _df[cols], on=cols[0], how='left')
df.JoinGift = df.JoinGift.fillna(value=False)

# join amount
_df = df.loc[df.JoinGift].drop_duplicates('DonorID')
_df['JoinAmount'] = _df.GiftAmount
cols = ['DonorID', 'JoinAmount']
df = pd.merge(df, _df[cols], on=cols[0], how='left')
print(df.shape)
df.JoinAmount.describe()
# (62004, 28)

# COMMAND ----------

# Gave 2nd Gift where “True” if donor made any additional gift, otherwise “False”
# 2nd Gift where “True” when a gift is 2nd by a unique donor, or otherwise “False”
# 2nd Gift Amount
# Time to 2nd Gift where value is days to 2nd gift, or field is NULL for single gift givers
_df = df.groupby('DonorID')['GiftID'].nunique().to_frame(name='TotalGifts')
df = pd.merge(df, _df, on='DonorID', how='left')

df['GaveSecondGift'] = df.TotalGifts > 1
df.GaveSecondGift.value_counts()

# COMMAND ----------

mask = df.GiftDate == df.FirstGiftDate
_df = df.loc[~mask].groupby('DonorID')['GiftDate'].min().to_frame(name='SecondGiftDate')
df = pd.merge(df, _df, on='DonorID', how='left')

_df = df.sort_values('GiftID').drop_duplicates(['DonorID', 'GiftDate'])
_df['SecondGift'] = _df.GiftDate == _df.SecondGiftDate
cols = ['GiftID', 'SecondGift']
df = pd.merge(df, _df[cols], on=cols[0], how='left')
df.SecondGift = df.SecondGift.fillna(value=False)
print(df.shape)
df.SecondGift.value_counts()

# COMMAND ----------

# second gift amount
_df = df.loc[df.SecondGift].drop_duplicates('DonorID')
_df['SecondGiftAmount'] = _df.GiftAmount
cols = ['DonorID', 'SecondGiftAmount']
df = pd.merge(df, _df[cols], on=cols[0], how='left')
print(df.shape)
df.SecondGiftAmount.describe()

# COMMAND ----------

df['TimeToSecondGift'] = (df.SecondGiftDate - df.FirstGiftDate).dt.days
df.TimeToSecondGift.describe()

# COMMAND ----------

_df = df.groupby('DonorID')['GiftDate'].max().to_frame(name='LastGiftDate')
print(df.shape)
df = pd.merge(df, _df, on='DonorID', how='left')
print(df.shape)

# COMMAND ----------

df['TotalDaysOnFile'] = (df.LastGiftDate - df.FirstGiftDate).dt.days
df.TotalDaysOnFile.describe()

# COMMAND ----------

df['GaveAfterOneYear'] = df.TotalDaysOnFile > 365
df.GaveAfterOneYear.value_counts()

# COMMAND ----------

df['DaysSinceJoin'] = (df.GiftDate - df.FirstGiftDate).dt.days
df.DaysSinceJoin.describe()

# COMMAND ----------

# Gift Within 267 Days where “True” if gift is received within first 267 days after join.
df['GiftWithin267Days'] = df.DaysSinceJoin <= 267
df.GiftWithin267Days.value_counts()


# COMMAND ----------

# Gift Within 268-461 where “True” if gifts received in days 268 to 461 after join
df['GiftWithin268to461Days'] = (df.DaysSinceJoin > 267) & (df.DaysSinceJoin <= 461)
df.GiftWithin268to461Days.value_counts()

# COMMAND ----------

df.columns

# COMMAND ----------

# ·         Donor ID
# ·         Gift ID
# ·         Gift Amount
# ·         Gift Date
# ·         Join Group (one of the 6 listed above)
# ·         Join Gift where “True” when gift is the 1st by a unique donor, otherwise “False”
# ·         Join Amount
# ·         Gave 2nd Gift where “True” if donor made any additional gift, otherwise “False”
# ·         2nd Gift where “True” when a gift is 2nd by a unique donor, or otherwise “False”
# ·         2nd Gift Amount
# ·         Time to 2nd Gift where value is days to 2nd gift, or field is NULL for single gift givers
# ·         Gave After 1 Year where “True” if donor has any gift 365+ days after join gift
# ·         Gift Within 267 Days where “True” if gift is received within first 267 days after join.
# ·         Gift Within 268-461 where “True” if gifts received in days 268 to 461 after join. (This will allow us to see impact of Ukraine on 2021 joins specifically)
# ·         Total Gifts which is count of donor’s total gifts made
# ·         Campaign ID
# ·         Appeal Category

cols = [
  'DonorID', 'GiftID', 'GiftAmount', 'GiftDate', 'JoinGroup', 'JoinAmount',
  'GaveSecondGift', 'SecondGift', 'SecondGiftAmount', 'TimeToSecondGift',
  'GaveAfterOneYear', 'GiftWithin267Days', 'GiftWithin268to461Days',
  'TotalGifts', 'CampaignID', 'AppealCategory', 'JoinGift',
  'GaveSustainerGift', 'GaveFirstSustainerGift', 'FirstSustainerGiftAppealCategory',
  ]
df[cols].head().T
        

# COMMAND ----------

print(df.shape)
print(df.drop_duplicates('GiftID').shape)

# COMMAND ----------

df.JoinGift.value_counts()


# COMMAND ----------

df = df[cols]
print(df.shape)
df.head().T


# COMMAND ----------

df.to_csv(os.path.join(filemap.CURATED, 'MC_UkraineDonorAnalysis.csv'), index=False)

# COMMAND ----------

mask = (df.DonorID.astype(str) == '978166') & (df.GiftDate == '2020-05-20')
df.loc[mask]

# COMMAND ----------

