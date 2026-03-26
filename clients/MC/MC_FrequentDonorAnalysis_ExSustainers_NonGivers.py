# Databricks notebook source
'''
Frequent Donor Analysis
'''

# COMMAND ----------

# import json
import pandas as pd
import numpy as np

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

# pd.set_option('display.max_rows', None)


# COMMAND ----------

# MAGIC %run ../python_modules/utilities

# COMMAND ----------

# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# establish context
filemap = Filemap('MC')
os.listdir(filemap.STAGED)

# COMMAND ----------

# MAGIC %md #Identify sustainers

# COMMAND ----------

path = os.path.join(filemap.RAW, 'Sustainers')
_df = pd.read_csv(os.path.join(path, 'Mercy Corps Account Info.csv'))
# print(_df.shape)

m = {
  'Constituent_ID': 'DonorID', 
  'PIP_Next_Transaction_Date': 'DueDate',
  'Previous_PIP_End_Date': 'CompletedDate'
  }
_df = _df.rename(columns=m)[list(m.values())]
print(_df.shape)
_df.head()

# COMMAND ----------

# take only donors with either a valid due date, or valid completed date (or both)
mask = (_df.DueDate.notna()) | (_df.CompletedDate.notna())
_df = _df.loc[mask]
_df.shape

# COMMAND ----------

# identify the set of unique donors
donors = _df.DonorID.unique()
len(donors)

# COMMAND ----------

# MAGIC %md #Read the transaction history

# COMMAND ----------

# read the tx history
df = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
df.DonorID = df.DonorID.astype(int)

# set the fiscal date columne
df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', 7)
print(df.shape)

# find the FY23 gifts and revenue
mask = df.GiftFiscal == 2023
print('FY23 Revenue: ', df.loc[mask, 'GiftAmount'].sum())
print('FY23 Donors: ', df.loc[mask, 'DonorID'].nunique())

# take only the non-sustainers
mask = df.DonorID.isin(donors)
df = df.loc[~mask]
print(df.shape)

# remove gifts from FY24
mask = df.GiftFiscal < 2024
df = df.loc[mask]
print(df.shape)


# COMMAND ----------

# sanity check - the sustainers should not be in the df dataset
d1 = set(df.DonorID.unique())
d2 = set(_df.DonorID.unique())
d1.intersection(d2)

# COMMAND ----------

# Recency
_df = df.groupby('DonorID')['GiftDate'].max().to_frame(name='MostRecentGiftDate')
df = pd.merge(df, _df, on='DonorID', how='left')
df['Recency'] = (datetime(2023,7,1) - df.MostRecentGiftDate).dt.days
df.Recency.describe()

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

# remove organizations
mask = df.IndividualOrOrgFlag == 'O'
print(df.GiftAmount.sum())
print(df.loc[mask].GiftAmount.sum())
print(df.loc[~mask].GiftAmount.sum())
print(df.loc[mask].GiftID.nunique())
print(df.loc[mask].DonorID.nunique())

df = df.loc[~mask]
df.IndividualOrOrgFlag.value_counts()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md #Promo history

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

# rename the columns
m = {
  'Constituent ID': 'DonorID',
  'Assigned Appeal Date': 'MailDate',
  'Assigned Appeal ID': 'CampaignCode'
}
dfs = dfs.rename(columns=m)
dfs.MailDate = pd.to_datetime(dfs.MailDate)

# take just the relevant campaigns
m1 = dfs.CampaignCode.str.startswith(('AG', 'AH', 'AN', 'AW', 'QQ'))
m2 = dfs.MailDate.dt.year > 2021
mask = (m1) & (m2)
dfs = dfs.loc[mask]

# bootstrap the date and take only FY23
dfs['MailFY'] = dfs['Assigned Appeal Description'].str.split(' ').str[0]
mask = dfs.MailFY.isin(['FY23', 'Unsourced'])
dfs = dfs.loc[mask]

# identify the times each donor was mailed in FY23
_df = dfs.groupby('DonorID').size().to_frame(name='TimesMailed')
_df.head()
# print(dfs.shape)
# print(df.shape)
# df = pd.merge(df, _df, on='DonorID', how='inner')
# print(df.shape)

# print(dfs.shape)
# dfs.head(2)

# COMMAND ----------

# find the FY23 gifts
mask = df.GiftFiscal == 2023

# the set of donors that gave in 2023, and the compliment is those who did not
donors2023 = df.loc[mask, 'DonorID'].unique()
print(df.loc[~mask, 'DonorID'].nunique())
len(donors2023)

# COMMAND ----------

# reset donors to be all those who were mailed in FY23
donors = _df.reset_index().DonorID.unique()
# len(donors)

# non_givers are those who were mailed but who did not give in FY23
non_givers = set(donors).intersection(set(donors)^set(donors2023))
len(non_givers)
# 21424

# COMMAND ----------

# identify the total_mailed 
total_mailed = _df.reset_index().DonorID.nunique()
print(total_mailed)

# identify the ratio of those who did not give to those who were mailed
len(non_givers) / _df.reset_index().DonorID.nunique()

# COMMAND ----------

df.GiftFiscal.describe()

# COMMAND ----------


# sanity check - how many of our cohort actually gave in FY23
m1 = (df.Recency > 1e3)
m2 = (df.Frequency == 1)
m3 = (df.Monetary < 1e2)
m4 = (df.GiftFiscal == 2023)
mask = m1 & m2 & m3 & m4
df.loc[mask, 'DonorID'].nunique() / df.loc[m4, 'DonorID'].nunique()



# COMMAND ----------

# sanity check - the non_givers should not have gifts past FY22
mask = df.DonorID.isin(non_givers)
df.loc[mask, 'GiftDate'].describe(datetime_is_numeric=True)

# COMMAND ----------

# now that that is confirmed, we can mask the dataset
mask = df.DonorID.isin(non_givers)
df = df.loc[mask]
df.shape

# COMMAND ----------

df.DonorID.nunique()

# COMMAND ----------

# merge in the total times mailed column
print(df.shape)
df = pd.merge(df, _df, on='DonorID', how='left')
print(df.shape)

# COMMAND ----------

p = [x/20 for x in range(1,20)] + [.96, .97, .98, .99, .995, .999, 1]
df.drop_duplicates('DonorID').Recency.describe(percentiles=p)

# COMMAND ----------

# # d = {25: '20%', 10: '40%'}
# mask = df.Recency < 366
# df.loc[mask].drop_duplicates('DonorID').Recency.hist(bins=100)
df.drop_duplicates('DonorID').Recency.hist(bins=100)
plt.xlabel('Days')
plt.ylabel('Donor Count')
plt.title('Days back from 6/30/2023 for non-responders')
plt.show()

# COMMAND ----------

p = [x/20 for x in range(1,20)] + [.96, .97, .98, .99, .995, .999, 1]
df.drop_duplicates('DonorID').Frequency.describe(percentiles=p)

# COMMAND ----------

mask = df.Frequency < 50
df.loc[mask].drop_duplicates('DonorID').Frequency.hist(bins=100)
# df.drop_duplicates('DonorID').Frequency.hist(bins=100)
plt.xlabel('Gifts')
plt.ylabel('Donor Count')
plt.title('Number of total gifts per donor for the non-responders')
plt.show()

# COMMAND ----------

p = [x/20 for x in range(1,20)] + [.96, .97, .98, .99, .995, .999, 1]
df.drop_duplicates('DonorID').Monetary.describe(percentiles=p)

# COMMAND ----------

mask = df.Monetary <= 1e3
df.loc[mask].drop_duplicates('DonorID').Monetary.hist(bins=100)
# df.drop_duplicates('DonorID').Monetary.hist(bins=100)
plt.xlabel('HPC')
plt.ylabel('Donor Count')
plt.title('Highest Previous Gift per donor for the non-responders')
plt.show()

# COMMAND ----------

mask = df.Monetary <= 1e2
df.loc[mask].drop_duplicates('DonorID').Monetary.hist(bins=100)
plt.xlabel('HPC')
plt.ylabel('Donor Count')
# plt.title('Highest Previous Gift per donor for the %s cohort' %d[threshold])
plt.show()

# COMMAND ----------

p = [x/20 for x in range(1,20)] + [.96, .97, .98, .99, .995, .999, 1]
df.drop_duplicates('DonorID').TimesMailed.describe(percentiles=p)

# COMMAND ----------

df.drop_duplicates('DonorID').TimesMailed.hist(bins=100)
plt.xlabel('Times Mailed')
plt.ylabel('Donor Count')
# plt.title('Number of times mailed for FY23 campaigns for the %s cohort' %d[threshold])
plt.show()

# COMMAND ----------

m1 = (df.Recency > 1e3)
m2 = (df.Frequency == 1)
m3 = (df.Monetary < 1e2)
mask = m1 & m2 & m3
df.loc[mask, 'DonorID'].nunique()

# COMMAND ----------

df.loc[mask, 'DonorID'].nunique() / df.DonorID.nunique()

# COMMAND ----------

df.loc[mask, 'DonorID'].nunique()/total_mailed

# COMMAND ----------

df.DonorID.nunique()

# COMMAND ----------

df.loc[mask, 'GiftDate'].describe(datetime_is_numeric=True, percentiles=p)

# COMMAND ----------

df.loc[mask, 'GiftDate'].hist(bins=100)
plt.xlabel('Gift Date')
plt.ylabel('Gift Count')
plt.title('Gift distribution for the non-responders mailed in FY23')
plt.show()


# COMMAND ----------

_df = df.loc[mask]
grouped = _df.groupby('DonorID').GiftDate.min().to_frame(name='FirstGiftDate')
print(_df.shape)
_df = pd.merge(_df, grouped, on='DonorID', how='left')
print(_df.shape)

# COMMAND ----------

_df.drop_duplicates('DonorID').FirstGiftDate.hist(bins=100)
plt.xlabel('Gift Date (Calendar Year)')
plt.ylabel('Gift Count')
plt.title('JoinGift distribution for the lowest 21% of non-responders mailed in FY23')
plt.show()

# COMMAND ----------

