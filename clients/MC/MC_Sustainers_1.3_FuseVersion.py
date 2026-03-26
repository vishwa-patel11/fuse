# Databricks notebook source
# MAGIC %md # MC Sustainer Analysis

# COMMAND ----------

# MAGIC %md ## Runs and Imports

# COMMAND ----------

# DBTITLE 1,imports
# import json
import pandas as pd
import numpy as np

# pd.set_option('display.max_rows', None)


# COMMAND ----------

# DBTITLE 1,utilities
# MAGIC %run ../python_modules/utilities

# COMMAND ----------

# DBTITLE 1,mount_datalake
# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# DBTITLE 1,establish client and context
# establish context
filemap = Filemap('MC')
os.listdir(filemap.STAGED)

# COMMAND ----------

# MAGIC %md ## Code

# COMMAND ----------

# DBTITLE 1,Read in data, format/filter, sort values
# # read master file
path = os.path.join(filemap.MASTER, 'Data.parquet')
df = pd.read_parquet(path).drop_duplicates()
df['GiftMonth'] = df.GiftDate.dt.month

# format and filter columns
df.columns = [x.replace(' ', '') for x in df.columns]
cols = ['DonorID', 'GiftDate', 'GiftType', 'GiftAmount', 'AppealCategory', 'PayMethod', 'CampaignID', 'GiftMonth']
df = df[cols]

# sort values and give each row a unique id
df = df.sort_values(['DonorID', 'GiftDate'])
df['GiftID'] = [x for x in range(df.shape[0])]

print(df.shape)
df.head().T
# (736792, 6)

# COMMAND ----------

# DBTITLE 1,Add SustGiftFlag
# add sustainer gift flag 
feature = 'SustGiftFlag'
##condition = df.GiftType == 'Recurring Gift Pay Cash' # Updated from 'Recurring Gift Pay Cash' to include values that have 'Recurring Gift Pay-Cash'
condition = df.GiftType.isin(['Recurring Gift Pay Cash', 'Recurring Gift Pay-Cash'])

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

# DBTITLE 1,Derive Relevant Dates
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

# DBTITLE 1,Add SustainerType column
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

# DBTITLE 1,Add SustGiftIDs column
# get the ids of all sustainer gifts by donor
feature = 'SustGiftIDs'
mask = df['SustGiftFlag'] == 'Sustainer'
_df = df[mask].groupby('DonorID')['GiftID'].unique().to_frame(name=feature)

_df[feature] = [sorted(x) for x in _df[feature].values.tolist()]
df = pd.merge(df, _df, on='DonorID', how='left')

print(df.shape)
df.head()

# COMMAND ----------

# DBTITLE 1,Get most recent sustainer gift by donor
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

# DBTITLE 1,Determine the number of days from any gifts to the most recent sustainer gift
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

# DBTITLE 1,assign the sustainer membership status (active or lapsed)
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

# DBTITLE 1,find the most recent sustainer join date
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

# DBTITLE 1,convert to datetime
df[feature] = pd.to_datetime(df[feature])

# COMMAND ----------

# DBTITLE 1,# find the breaks between donors and assign first sust gift date
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

# DBTITLE 1,Assign df as active only
# separate dataframes into active and lapsed

# create mask
mask = df['SustainerStatus'] == 'Lapsed'

# # take non-active
# df_lapsed = df[~mask]

# reassign df as active only
df = df[~mask]

print(df.shape)

# COMMAND ----------

# DBTITLE 1,Get DaysOnFile for non-Lapsed donors
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

# DBTITLE 1,Calculate "Active"
# calculate 'active'

# Days since most recent sustainer join gift
df['GiftDateToJoinDate'] = (df['GiftDate'] - df['MostRecentSustJoinDate']).dt.days

# # Days since first sustainer gift
# basedate = df['GiftDate'].max()
# df['ReportDateToJoinDate'] = df['MostRecentSustJoinDate'].apply(lambda x: (basedate-x).days)

# If a donor gives 275-365 after the most recent sustainer join date, they are active for 1 year
# If a donor gives 549-730 days after the most recent sustainer join date, theyar are active for 2 years
# ...

def get_endpoints(y):
  if y == 1:
    offset = 275
  elif y == 2:
    offset = 184
  else:
    offset = 1
  return ((y-1)*365) + offset, y*365

YEARS = [1, 2, 3, 4, 5]
for year in YEARS:
  start, end = get_endpoints(year)
  df[str(year) + 'active'] = ((df['GiftDateToJoinDate'] > start) & (df['GiftDateToJoinDate'] <= end)).astype(int)
  
print(df.shape)
# df.head()

# COMMAND ----------

# DBTITLE 1,Determine the donors to make up the cohort for each year
# Determine the donors to make up the cohort for each year

def active_donor(df, year):
  return int(df[str(year) + 'active'].any())

for year in YEARS:
  cols = ['DonorID', 'SustJoinFY']
  _ = df.groupby(cols).apply(active_donor, year).to_frame(name=str(year)+'donor')
  if year == 1:
    _df = _
  else:
    _df = pd.merge(_df, _, on=cols, how='left')
  
df = pd.merge(df, _df, on=cols, how='left')
print(df.shape)
df.head()

# COMMAND ----------

# DBTITLE 1,Identify gifts to be included in each LTV year
# Identify gifts to be included in each LTV year

for year in YEARS:
  df[str(year)+'LTVgift'] = (df['GiftDateToJoinDate'] <= year * 365).astype(int)


print(df.shape)
# (29397, 32)

# COMMAND ----------

# DBTITLE 1,Retention function for QC
# Retention function for QC

def get_Ret(df, years):  
  _df = pd.DataFrame()
  
  for y in years:
    mask = df[str(y)+'active'] == 1
    col = str(y) + 'ret'
    _df[col] = [df[mask]['DonorID'].nunique() / df['DonorID'].nunique()]
    
  return _df

# Overall retention by Fiscal Year
years = [i for i in range(2008, 2023)]
mask = df['SustJoinFY'].isin(years)
cols = ['SustJoinFY']
df[mask].groupby(cols).apply(get_Ret, YEARS)

# COMMAND ----------

# MAGIC %md ## Write

# COMMAND ----------

# DBTITLE 1,Write to ADLS2
# write to ADLS2
cols = [x for x in df.columns if 'donor' not in x]
cols = df.columns
df[cols].to_csv(os.path.join(filemap.CURATED, 'MC_SustainerDataset_rev1.csv'), index=False)

# COMMAND ----------

