# Databricks notebook source
'''
Sustainer Analysis
'''

# COMMAND ----------

# import json
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

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

# # read master file
path = os.path.join(filemap.MASTER, 'Data.parquet')
df = pd.read_parquet(path).drop_duplicates()

# format and filter columns
df.columns = [x.replace(' ', '') for x in df.columns]
# cols = ['DonorID', 'GiftDate', 'GiftType', 'GiftAmount', 'AppealCategory', 'PayMethod']
# df = df[cols]

# sort values and give each row a unique id
df = df.sort_values(['DonorID', 'GiftDate'])
df['GiftID'] = [x for x in range(df.shape[0])]

print(df.shape)
df.head().T
# (736792, 6)

# COMMAND ----------

df

# COMMAND ----------

df.PayMethod.value_counts()

# COMMAND ----------



# COMMAND ----------

# add sustainer gift flag 
feature = 'SustGiftFlag'
condition = df.GiftType == 'Recurring Gift Pay Cash'
df[feature] = np.where(condition, 'Sustainer', '1x')

# filter for only sustainer donors
feature = 'SustDonors'
mask = df['SustGiftFlag'] == 'Sustainer'
sustainers = df[mask]['DonorID'].unique()
mask = df['DonorID'].isin(sustainers)
df = df.loc[mask]


print(df.shape)
df.head()

# COMMAND ----------

# derive relevant dates
# first ever gift date
feature = 'FirstGiftDate'
_df = df.groupby('DonorID')['GiftDate'].min().to_frame(name=feature)
df = pd.merge(df, _df, on='DonorID', how='left')

# first sustainer gift date
feature = 'SustFirstGiftDate'
mask = df['SustGiftFlag'] == 'Sustainer'
_df = df[mask].groupby('DonorID')['GiftDate'].min().to_frame(name=feature)
df = pd.merge(df, _df, on='DonorID', how='left')


#first sustainer gift payment method
mask = df.SustFirstGiftDate == df.GiftDate #add to top, work with df
_df = df.loc[mask].drop_duplicates('DonorID')
_df['FirstSustainerPaymentMethod'] = _df['PayMethod']
cols = ['DonorID','FirstSustainerPaymentMethod']
df = pd.merge(df,_df[cols],on='DonorID',how='left')


# filter out 1x gifts before the first sustainer join gift
mask = df['GiftDate'] >= df['SustFirstGiftDate']
df = df[mask]

# derive Gift FY
schema = get_schema_details(filemap.SCHEMA)
fym = schema['firstMonthFiscalYear']
df['GiftFY'] = fiscal_from_column(df, 'GiftDate', fym)

print(df.shape)
df.head()

# COMMAND ----------

# Sustainer Type
feature = 'SustainerType'
_df = df.drop_duplicates('DonorID')

condition = _df['FirstGiftDate'] == _df['SustFirstGiftDate']
_df[feature] = np.where(condition, 'New', 'Converted')

cols = ['DonorID', feature]
df = pd.merge(df, _df[cols], on='DonorID', how='left')

print(df.shape)
df[feature].value_counts()

# COMMAND ----------

# get the ids of all sustainer gifts by donor
feature = 'SustGiftIDs'
mask = df['SustGiftFlag'] == 'Sustainer'
_df = df[mask].groupby('DonorID')['GiftID'].unique().to_frame(name=feature)

_df[feature] = [sorted(x) for x in _df[feature].values.tolist()]
df = pd.merge(df, _df, on='DonorID', how='left')

print(df.shape)
df.head()

# COMMAND ----------

# get the most recent sustainer gift by donor

# save GiftID: GiftDate mapping for use after join
idsToDates = dict(zip(df['GiftID'], df['GiftDate']))
idsToDates[-1] = None

# get the most recent sustainer gift preceeding any donor gift
# if none exist, record donor id as -1 (undefined)
feature = 'MostRecentSustGiftID'
cols = ['GiftID', 'SustGiftIDs']
df[feature] = [max(i if i <= x[0] else -1 for i in x[1] ) for x in df[cols].values.tolist()]

# map GiftIDs -> GiftDates
feature = 'MostRecentSustGiftDate'
df[feature] = df['MostRecentSustGiftID'].map(idsToDates)

# drop helper columns
df = df.drop(['SustGiftIDs', 'MostRecentSustGiftID'], axis=1)

print(df.shape)
df[feature].describe()

# COMMAND ----------

# Determine the number of days from any gifts to the most recent sustainer gift

# sort by donor and date to facilitate subtraction
df = df.sort_values(['DonorID', 'GiftDate'])

# calculate days and cast as integer
feature = 'DateDelta'
df[feature] = (df['GiftDate'] - df['MostRecentSustGiftDate']).dt.days
df[feature] = df[feature].astype('Int32')

# find the difference to help identify reacquired
df[feature+'_diff'] = df[feature].diff()

# calculate sust gift delta days and cast as integer
feature = 'SustGiftDateDelta'
df[feature] = df['MostRecentSustGiftDate'].diff().dt.days

print(df.shape)
df[feature].describe()

# COMMAND ----------

# assign the sustainer membership status (active or lapsed)

THRESHOLD = 183
feature = 'SustainerStatus'
df[feature] = None

# when the number of days since the last sustainer gift is <= 183
condition = (~df['DateDelta'].isna()) & (df['DateDelta'] <= THRESHOLD)
df[feature] = np.where(condition, 'Active', df[feature])

# when the number of days since the last sustainer gift is > 183
condition = (~df['DateDelta'].isna()) & (df['DateDelta'] > THRESHOLD)
df[feature] = np.where(condition, 'Lapsed', df[feature])

print(df.shape)
df[feature].describe()

# COMMAND ----------

# find the most recent sustainer join date
# i.e. if the sustainer is reactivated
feature = 'MostRecentSustJoinDate'

# When the DataDelta goes from greater than 183 -> 0, the diff becomes less than -183
condition = (df['DateDelta_diff'] < -THRESHOLD) & (~df['DateDelta_diff'].isna())
df[feature] = np.where(condition, df['MostRecentSustGiftDate'], None)

# When the SustGftDataDelta is greater than 183 days
condition = (df['SustGiftDateDelta'] > THRESHOLD) & (~df['DateDelta_diff'].isna())
df[feature] = np.where(condition, df['MostRecentSustGiftDate'], None)

# sort for good measure before forward filling
df = df.sort_values(['DonorID', 'GiftDate'])

# COMMAND ----------

df[feature] = pd.to_datetime(df[feature])

# COMMAND ----------

# find the breaks between donors and assign first sust gift date
mask = df['DonorID'] != df['DonorID'].shift(1)
##df[feature][mask] = df['SustFirstGiftDate']
df.loc[mask, feature] = df['SustFirstGiftDate']
df.dtypes

# COMMAND ----------



# # find the breaks between donors and assign first sust gift date
# mask = df['DonorID'] != df['DonorID'].shift(1)
# df[feature][mask] = df['SustFirstGiftDate']

# forward fill nas
df[feature] = pd.to_datetime(df[feature]).fillna(method='pad')

# drop helper column
df = df.drop('DateDelta_diff', axis=1)

# get MostRecentSustJoinDate FY
df['SustJoinFY'] = fiscal_from_column(df, 'MostRecentSustJoinDate', fym)

print(df.shape)
df[feature].describe()

# COMMAND ----------

df.head()

# COMMAND ----------

# identify the acquisition source of the most recent membership join
feature = 'SustJoinSource'

# take the records associated with the most recent membership join
mask = df['GiftDate'] == df['MostRecentSustJoinDate']
_df = df[mask]

# assign Appeal ID to Sustainer Join Source
_df[feature] = _df['AppealCategory']
# condition = _df[feature].str.upper().str.contains('ONLINE').fillna(value=False)
# _df[feature] = np.where(condition, 'ONLINE', 'DIRECT MAIL')

# merge back to the dataframe on donor id and most recent membership join date
cols = ['DonorID', 'MostRecentSustJoinDate', feature]
df = pd.merge(df, _df[cols], on=cols[:2], how='left').drop_duplicates('GiftID')

print(df.shape)
df[feature].describe()

# COMMAND ----------

d = {'DonorID': 'nunique', 'GiftAmount': 'sum'}
cols = ['SustJoinFY', 'SustainerStatus']
mask = df.SustJoinFY > 2019
df.loc[mask].groupby(cols).agg(d)
# df.head().T

# COMMAND ----------

d = {'DonorID': 'nunique', 'GiftAmount': 'sum'}
cols = ['GiftFY', 'SustainerStatus']
mask = df.GiftFY > 2019
df.loc[mask].groupby(cols).agg(d).rename(columns={'DonorID': 'Donors', 'GiftAmount': 'Revenue'})
# df.head().T

# COMMAND ----------

# # separate dataframes into active and lapsed

# # create mask
# mask = df['SustainerStatus'] == 'Lapsed'

# # # take non-active
# # df_lapsed = df[~mask]

# # reassign df as active only
# df = df[~mask]

# print(df.shape)

# COMMAND ----------

# Get days on file for non-Lapsed donors
feature = 'DaysOnFile'

# groupby columns
cols = ['DonorID', 'MostRecentSustJoinDate']

# groupby and get max and min dates for each sustainer period by donor
grouped = df.groupby(cols).agg({'GiftDate': ['max', 'min']})

# flatten the column multi-index
grouped.columns = grouped.columns.get_level_values(1)

# calculate the number of days
grouped[feature] = (grouped['max'] - grouped['min']).dt.days

# reformat the grouped dataframe
grouped = grouped.reset_index().drop(['max', 'min'], axis=1)

# merge back to the sustainer dataframe
df = pd.merge(df, grouped, on=cols, how='left')

print(df.shape)
df.head()

# COMMAND ----------

# MAGIC %md #Start EDA

# COMMAND ----------

df.SustainerStatus.value_counts()

# COMMAND ----------

df.groupby('SustainerStatus').DonorID.nunique()

# COMMAND ----------

mask = df.SustainerStatus == 'Lapsed'
# mask.sum()
lapsed_donors = df.loc[mask, 'DonorID'].unique()
# len(lapsed_donors)
df['LapsedDonors'] = df.DonorID.isin(lapsed_donors)
# df.loc[df.LapsedDonors].DonorID.nunique()
df.loc[df.LapsedDonors, 'SustainerStatus'].value_counts()

# df.loc[df.LapsedDonors].SustGiftDateDelta.hist(bins=100)

# COMMAND ----------

# number of times a donor has joined - may be necessary later for filtering
_df = df.groupby('DonorID').MostRecentSustJoinDate.nunique().to_frame(name='NumJoins')
df = pd.merge(df, _df, on='DonorID', how='left')

# the last sustainer gift date for any of the sustainer's join dates
cols = ['DonorID', 'MostRecentSustJoinDate']
_df = df.groupby(cols).MostRecentSustGiftDate.max().to_frame(name='SustLastGiftDate')
df = pd.merge(df, _df, on=cols, how='left')

# the last gift date for any of the sustainer's join dates
cols = ['DonorID', 'MostRecentSustJoinDate']
_df = df.groupby(cols).GiftDate.max().to_frame(name='LastGiftDate')
df = pd.merge(df, _df, on=cols, how='left')

# COMMAND ----------

# cols = ['DonorID', 'SustainerStatus', 'MostRecentSustGiftDate', 'MostRecentSustJoinDate', 'SustLastGiftDate']
# df = df.sort_values(['DonorID', 'GiftDate'])
# mask =  (df.NumJoins > 1)

# df.loc[mask, cols].head(20)

# COMMAND ----------

# days from each of a sustainers joins to the last sustainer gift of that join period
df['DaysFromJoinToLastGift'] = (df.SustLastGiftDate - df.MostRecentSustJoinDate).dt.days
df.DaysFromJoinToLastGift.describe()

# COMMAND ----------

# days from the data-through date to the last sustainer gifts
# used for identifying lapsed people who are no longer giving, ie. have not lapsed gifts
max_date = df.GiftDate.max()
df['DaysFromDateThrough'] =(max_date - df.MostRecentSustGiftDate).dt.days
df.DaysFromDateThrough.describe()

# COMMAND ----------

# for each of a sustainer's join periods, take only the last record
cols = ['DonorID', 'MostRecentSustJoinDate', 'MostRecentSustGiftDate']
_df = df.sort_values(cols).drop_duplicates(cols[:-1], keep='last')
_df

# COMMAND ----------

# take only the records where the last sustainer gift is greater than 183 days ago
# this 
mask = (_df.DaysFromDateThrough >= 183) 
_df.loc[mask, 'DonorID'].nunique()

# COMMAND ----------

_df['LapsedFY'] = fiscal_from_column(_df, 'MostRecentSustGiftDate', 7)
_df.loc[mask].groupby(_df.LapsedFY).DonorID.nunique()

# COMMAND ----------

mask = (_df.DaysFromJoinToLastGift < 1000)# & (_df.SustJoinFY >= 2020)
_df.loc[mask].DaysFromJoinToLastGift.hist(bins=100)
plt.title('Number of Days Between Sustainer First and Last Gift')
plt.ylabel('Donor count')
plt.xlabel('Number of days')
plt.show()


# COMMAND ----------

types =  ['Credit Card', 'Direct Debit', 'Personal Check']
for t in types:
  mask = (_df.DaysFromJoinToLastGift < 1000) & (_df.PayMethod >= t)
  _df.loc[mask].DaysFromJoinToLastGift.hist(bins=100)
  plt.title('Number of Days Between Sustainer First and Last Gift: Pay Method %s' %t)
  plt.ylabel('Donor count')
  plt.xlabel('Number of days')
  plt.show()


# COMMAND ----------

mask = (_df.DaysFromJoinToLastGift < 1000) & (_df.PayMethod.isin(types))
_df.loc[mask].groupby('PayMethod').DaysFromJoinToLastGift.hist(bins=100, alpha=.5, legend=True)
plt.title('Number of Days Between Sustainer First and Last Gift')
plt.ylabel('Donor count')
plt.xlabel('Number of days')
plt.legend()
plt.show()

# COMMAND ----------


mask = (df.LastGiftDate > df.SustLastGiftDate)
_df = df.loc[mask].sort_values(['DonorID', 'GiftDate'])
_df.shape

# COMMAND ----------

_df.columns

# COMMAND ----------

_df = _df.drop(['DaysFromJoinToLastGift', 'DaysFromDateThrough'], axis=1)
_df.shape

# COMMAND ----------

_df.tail(20)

# COMMAND ----------

_df.to_csv(os.path.join(filemap.CURATED, 'MC_SustainersGiftsAfterLapsing.csv'), index=False)

# COMMAND ----------

mask = (_df.DaysFromJoinToLastGift < 1000)# & (_df.SustJoinFY >= 2020)
_df.loc[mask].groupby('PayMethod').DaysFromJoinToLastGift.hist(bins=100, legend=True, alpha=.5)
plt.title('Number of Days Between Join Date and Finale Sustainer Gift Date')
plt.ylabel('Donor count')
plt.xlabel('Number of days')
plt.legend()
plt.show()


# COMMAND ----------

mask = (_df.DaysFromJoinToLastGift < 1000) & (_df.SustJoinFY >= 2020)
_df.loc[mask].groupby('SustJoinFY').DaysFromJoinToLastGift.hist(bins=100, legend=True, alpha=.2)
import matplotlib.pyplot as plt
plt.legend()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

mask = (df.LapsedDonors) & (df.SustGiftDateDelta >= 0)  & (df.SustGiftDateDelta < 100)
# mask = (df.SustGiftDateDelta > 0)  & (df.SustGiftDateDelta < 100)
df.loc[mask].SustGiftDateDelta.hist(bins=100)

# COMMAND ----------

df.columns

# COMMAND ----------

mask = (df.LapsedDonors) & (df.SustainerStatus == 'Lapsed') #& (df.SustGiftDateDelta >= 25) & (df.SustGiftDateDelta <= 35)
df.loc[mask, 'SustGiftDateDelta'].describe()

# COMMAND ----------

mask = (df.LapsedDonors) & (df.SustGiftDateDelta >= 25) & (df.SustGiftDateDelta <= 35)
df.loc[mask, 'SustainerStatus'].value_counts()

# COMMAND ----------

mask = (df.LapsedDonors) & (df.SustGiftDateDelta >= 25) & (df.SustGiftDateDelta <= 35)
df.loc[mask].to_csv(os.path.join(filemap.CURATED, 'MC_LapsedSustainerEDA.csv'), index=False)
# df.loc[mask].groupby('SustJoinFY').DonorID.nunique()

# COMMAND ----------

mask = (df.LapsedDonors) & (df.SustGiftDateDelta >= 25) & (df.SustGiftDateDelta <= 35)
df.loc[mask, 'DonorID'].nunique()
# df.loc[mask].groupby('SustJoinFY').DonorID.nunique()

# COMMAND ----------

mask = (df.LapsedDonors) & (df.SustGiftDateDelta == 0)
df.loc[mask, 'DonorID'].nunique()
# df.loc[mask].groupby('SustJoinFY').DonorID.nunique()

# COMMAND ----------

print(df.shape)
_df = df.loc[df.LapsedDonors]
_df.shape

# COMMAND ----------

mask = _df.SustainerStatus == 'Active'
#_df = _df.loc[mask].sort_values(['DonorID', 'GiftDate']).drop_duplicates('DonorID', keep='last')
#mask = (_df.SustGiftDateDelta > 0) & (_df.SustGiftDateDelta < 100)
_df.loc[mask].SustGiftDateDelta.hist(bins=100)
plt.ylabel('Donor Count')
plt.xlabel('Days')
plt.title('Days from Sustainer Join to Last Sustainer Gift Before Lapsing')

# COMMAND ----------

_df

# COMMAND ----------

mask = _df.SustFirstGiftDate == _df.GiftDate #add to top, work with df
sustdf = _df.loc[mask]
sustdf

# COMMAND ----------

sustdf

# COMMAND ----------

_df = _df.sort_values('FirstGiftDate')
_df['FirstGiftType'] = _df.groupby('DonorID')['PayMethod'].transform('first')
_df

# COMMAND ----------

mask = _df['DonorID'] == '4386'
_df.loc[mask] #groupbyfirstsustainerpayment method or daydelta.hist 

# COMMAND ----------

_df

# COMMAND ----------

mask = (_df.FirstGiftType == 'Credit Card') & (_df.SustGiftDateDelta > 0) & (_df.SustGiftDateDelta < 100) & (_df.GiftDate >= '2019-01-31')
_df.loc[mask].drop_duplicates(subset=['DonorID','MostRecentSustJoinDate']).SustGiftDateDelta.hist(bins=80, figsize=(15, 12))

# COMMAND ----------

mask = (_df.FirstGiftType == 'Direct Debit') & (_df.SustGiftDateDelta > 0) & (_df.SustGiftDateDelta < 100) & (_df.GiftDate >= '2019-01-31')
_df.loc[mask].drop_duplicates(subset=['DonorID','MostRecentSustJoinDate']).SustGiftDateDelta.hist(bins=80, figsize=(15, 12))

# COMMAND ----------

mask = (_df.FirstGiftType == 'Personal Check') & (_df.SustGiftDateDelta > 0) & (_df.SustGiftDateDelta < 100) & (_df.GiftDate >= '2019-01-31')
_df.loc[mask].drop_duplicates(subset=['DonorID','MostRecentSustJoinDate']).SustGiftDateDelta.hist(bins=80, figsize=(15, 12))

# COMMAND ----------

import matplotlib.pyplot as plt

# Define the masks
mask_cash = (_df.FirstGiftType == 'Credit Card') & (_df.SustGiftDateDelta > 0) & (_df.SustGiftDateDelta < 100) & (_df.GiftDate >= '2019-01-31')
mask_direct_debit = (_df.FirstGiftType == 'Direct Debit') & (_df.SustGiftDateDelta > 0) & (_df.SustGiftDateDelta < 100) & (_df.GiftDate >= '2019-01-31')
mask_personal_check = (_df.FirstGiftType == 'Personal Check') & (_df.SustGiftDateDelta > 0) & (_df.SustGiftDateDelta < 100) & (_df.GiftDate >= '2019-01-31')

# Plot the histograms
plt.figure(figsize=(15, 12))
_df.loc[mask_cash].drop_duplicates(subset=['DonorID','MostRecentSustJoinDate']).SustGiftDateDelta.hist(bins=80, alpha=0.5, label='Credit Card')
_df.loc[mask_direct_debit].drop_duplicates(subset=['DonorID','MostRecentSustJoinDate']).SustGiftDateDelta.hist(bins=80, alpha=0.5, label='Direct Debit')
_df.loc[mask_personal_check].drop_duplicates(subset=['DonorID','MostRecentSustJoinDate']).SustGiftDateDelta.hist(bins=80, alpha=0.5, label='Personal Check')

# Add legend and labels
plt.legend()
plt.xlabel('SustGiftDateDelta')
plt.ylabel('Amount')
plt.title('Histogram of SustGiftDateDelta for Different FirstGiftTypes')
plt.show()


# COMMAND ----------

_df.loc[mask_cash].FirstGiftType.value_counts()

# COMMAND ----------

_df.loc[mask_cash].GiftDate.min()

# COMMAND ----------

_df.loc[mask_cash].DonorID.value_counts()
_df.loc[mask_direct_debit].DonorID.value_counts()
_df.loc[mask_personal_check].DonorID.value_counts()


# COMMAND ----------

_df.loc[mask_personal_check].DonorID.value_counts()[_df.loc[mask_personal_check].DonorID.value_counts() > 1].count()


# COMMAND ----------

_df.loc[mask_personal_check].DonorID.nunique()

# COMMAND ----------

_df.loc[mask_cash].DonorID.value_counts()

# COMMAND ----------

cash = cash.reset_index().rename(columns={'index':'DonorID', 'DonorID':'Number of Duplications'})


# COMMAND ----------

#cash = pd.DataFrame(_df.loc[mask_cash].DonorID.value_counts())
display(cash)

# COMMAND ----------

cash['Number of Duplications'].describe()

# COMMAND ----------

