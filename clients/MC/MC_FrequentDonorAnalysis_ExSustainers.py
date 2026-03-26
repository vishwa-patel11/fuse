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

mask = (_df.DueDate.notna()) | (_df.CompletedDate.notna())
_df = _df.loc[mask]
_df.shape

# COMMAND ----------

donors = _df.DonorID.unique()
len(donors)

# COMMAND ----------

df = pd.read_parquet(os.path.join(filemap.MASTER, 'Data.parquet'))
df.DonorID = df.DonorID.astype(int)
df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', 7)
print(df.shape)

mask = df.GiftFiscal == 2023
print('FY23 Revenue: ', df.loc[mask, 'GiftAmount'].sum())
print('FY23 Donors: ', df.loc[mask, 'DonorID'].nunique())

mask = df.DonorID.isin(donors)
df = df.loc[~mask]
print(df.shape)

mask = df.GiftFiscal < 2024
df = df.loc[mask]
print(df.shape)


# COMMAND ----------

# sanity check
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

mask = df.GiftFiscal == 2023
df = df.loc[mask]
print(df.shape)

# COMMAND ----------

mask = df.IndividualOrOrgFlag == 'O'
print(df.GiftAmount.sum())
print(df.loc[mask].GiftAmount.sum())
print(df.loc[~mask].GiftAmount.sum())
print(df.loc[mask].GiftID.nunique())
print(df.loc[mask].DonorID.nunique())

# COMMAND ----------

df = df.loc[~mask]
df.IndividualOrOrgFlag.value_counts()

# COMMAND ----------

_df = df.groupby('DonorID').GiftAmount.sum().to_frame(name='TotalFY23Revenue')
df = pd.merge(df, _df, on='DonorID', how='left')
print(df.shape)

# COMMAND ----------

df.GiftAmount.sum()

# COMMAND ----------

p = [x/20 for x in range(1,20)] + [.96, .97, .98, .99, .995, .999, 1]
df.drop_duplicates('DonorID').TotalFY23Revenue.describe(percentiles=p)

# COMMAND ----------

mask = (df.TotalFY23Revenue > 2e4)
# mask = (df.TotalFY23Revenue < 1e5)
df.loc[mask].drop_duplicates('DonorID').TotalFY23Revenue.sum()#describe(percentiles=p)
# 4387693.46

# COMMAND ----------

df.loc[mask, 'DonorID'].nunique()

# COMMAND ----------

df = df.loc[~mask]
print(df.shape)
df.drop_duplicates('DonorID').TotalFY23Revenue.describe(percentiles=p)

# COMMAND ----------

# MAGIC %md #Read Promo History

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

print(dfs.shape)
dfs.head(2)

# COMMAND ----------

dfs['MailFY'] = dfs['Assigned Appeal Description'].str.split(' ').str[0]
dfs.MailFY.unique()

# COMMAND ----------

mask = dfs.MailFY.isin(['FY23', 'Unsourced'])
dfs.loc[mask].MailDate.describe(datetime_is_numeric=True)

# COMMAND ----------

dfs = dfs.loc[mask]

# COMMAND ----------

print(df.DonorID.dtypes)
print(dfs.DonorID.dtypes)

# COMMAND ----------

_df = dfs.groupby('DonorID').size().to_frame(name='TimesMailed')
print(dfs.shape)
print(df.shape)
df = pd.merge(df, _df, on='DonorID', how='inner')
print(df.shape)

# COMMAND ----------

_df.TimesMailed.describe()

# COMMAND ----------

d = {'TimesMailed': 'nunique', 'TotalFY23Revenue': 'nunique'}
df.groupby('DonorID').agg(d).describe()

# COMMAND ----------

df['RevenuePerTimesMailed'] = df.TotalFY23Revenue / df.TimesMailed
df.drop_duplicates('DonorID').RevenuePerTimesMailed.describe(percentiles=p)

# COMMAND ----------

mask = df.RevenuePerTimesMailed > 2e3
print('Total Revenue of 99.9 percentile: ', df.loc[mask].drop_duplicates('DonorID').TotalFY23Revenue.sum())
print('Total Donors in 99.9 percentile: ', df.loc[mask, 'DonorID'].nunique())
print('Total FY23 revenue: ', df.drop_duplicates('DonorID').TotalFY23Revenue.sum())
print('Total FY23 donors: ', df.DonorID.nunique())

# COMMAND ----------

# df.loc[mask].drop_duplicates('DonorID').TimesMailed.hist(bins=75)

# COMMAND ----------

# df.loc[~mask].drop_duplicates('DonorID').TimesMailed.hist(bins=75)

# COMMAND ----------

# cols = ['TimesMailed', 'RevenuePerTimesMailed']
# _df = df.loc[mask].drop_duplicates('DonorID').sort_values('TimesMailed')
# _df[cols].plot(x=cols[0], y=cols[-1], kind='scatter')

# COMMAND ----------

# cols = ['TimesMailed', 'RevenuePerTimesMailed']
# _df = df.loc[~mask].drop_duplicates('DonorID').sort_values('TimesMailed')
# _df[cols].plot(x=cols[0], y=cols[-1], kind='scatter')

# COMMAND ----------

# cols = ['TimesMailed', 'TotalFY23Revenue']
# _df = df.loc[mask].drop_duplicates('DonorID').sort_values('TimesMailed')
# _df[cols].plot(x=cols[0], y=cols[-1], kind='scatter')

# COMMAND ----------

# cols = ['TimesMailed', 'TotalFY23Revenue']
# _df = df.loc[~mask].drop_duplicates('DonorID').sort_values('TimesMailed')
# _df[cols].plot(x=cols[0], y=cols[-1], kind='scatter')

# COMMAND ----------

df_outliers = df.loc[mask]
df = df.loc[~mask]
df.drop_duplicates('DonorID').RevenuePerTimesMailed.describe(percentiles=p)

# COMMAND ----------

threshold = 10
mask = df.RevenuePerTimesMailed >= threshold
print('Total remaining Revenue after dropping 99.9% percentile: ', df.drop_duplicates('DonorID').TotalFY23Revenue.sum())
print('Total remaining Donors after dropping 99.9% percentile: ', df.DonorID.nunique())
print('Revenue of remaining top 50 percentile (after dropping .1 percentile outliers): ', df.loc[mask].drop_duplicates('DonorID').TotalFY23Revenue.sum())
print('Donors in remaining top 50 percentile (after dropping .1 percentile outliers): ', df.loc[mask, 'DonorID'].nunique())

# COMMAND ----------

print(7867118.950000001 / 11744920.760000002)
print(14841 / 72571)

# COMMAND ----------

# mask = df.RevenuePerTimesMailed >= 10
# print('Total remaining Revenue after dropping 99.9% percentile: ', df.drop_duplicates('DonorID').TotalFY23Revenue.sum())
# print('Total remaining Donors after dropping 99.9% percentile: ', df.DonorID.nunique())
# print('Revenue of remaining top 50 percentile (after dropping .1 percentile outliers): ', df.loc[mask].drop_duplicates('DonorID').TotalFY23Revenue.sum())
# print('Donors in remaining top 50 percentile (after dropping .1 percentile outliers): ', df.loc[mask, 'DonorID'].nunique())

# COMMAND ----------

28313/72571

# COMMAND ----------

df_saved = df.loc[~mask]
df = df.loc[mask]
print(df.shape)
print(df.DonorID.nunique())
print(df.drop_duplicates('DonorID').TotalFY23Revenue.sum())
print(df.GiftAmount.sum())

# COMMAND ----------

# fig = plt.figure(figsize=(18,12)) # Create matplotlib figure

# _df = df.loc[mask].drop_duplicates('DonorID').sort_values('TimesMailed', ascending=False)

# ax = fig.add_subplot(111) # Create matplotlib axes
# ax2 = ax.twinx() # Create another axes that shares the same x-axis as ax.



# _df.TotalFY23Revenue.plot(kind='bar', color='red', ax=ax)#, width=width, position=1)
# _df.TimesMailed.plot(kind='bar', color='blue', ax=ax2, alpha=.4)#, width=width, position=0)

# ax.set_ylabel('Gross Revenue')
# ax.set_xlabel('Donor ID')
# ax2.set_ylabel('Number Mailed')

# # fmt = '${x:,.0f}'
# # tick = mtick.StrMethodFormatter(fmt)
# # ax.yaxis.set_major_formatter(tick) 

# # ax2.get_yaxis().set_major_formatter(
# #   matplotlib.ticker.FuncFormatter(
# #     lambda x, p: format(int(x), ',')
# #   )
# # )

# plt.title('Gross Revenue (Red) and Times Mailed (Blue)')
# # plt.savefig('c:/users/joey katzenstein/desktop/fww.pdf')
# plt.show()

# COMMAND ----------

d = {25: '20%', 10: '40%'}
df.drop_duplicates('DonorID').Recency.hist(bins=100)
plt.xlabel('Days')
plt.ylabel('Donor Count')
plt.title('Days back from 6/30/2023 for the %s cohort' %d[threshold])
plt.show()

# COMMAND ----------

mask = df.Frequency < 20
df.loc[mask].drop_duplicates('DonorID').Frequency.hist(bins=100)
plt.xlabel('Gifts')
plt.ylabel('Donor Count')
plt.title('Number of total gifts per donor for the %s cohort' %d[threshold])
plt.show()

# COMMAND ----------

mask = df.Monetary <= 1e3
df.loc[mask].drop_duplicates('DonorID').Monetary.hist(bins=100)
plt.xlabel('HPC')
plt.ylabel('Donor Count')
plt.title('Highest Previous Gift per donor for the %s cohort' %d[threshold])
plt.show()

# COMMAND ----------

df.drop_duplicates('DonorID').TimesMailed.hist(bins=100)
plt.xlabel('Times Mailed')
plt.ylabel('Donor Count')
plt.title('Number of times mailed for FY23 campaigns for the %s cohort' %d[threshold])
plt.show()

# COMMAND ----------

cols = [
  'Recency', 'Frequency', 
  'Monetary', 'TotalFY23Revenue', 
  'TimesMailed', 'RevenuePerTimesMailed'
  ]
df[cols].corr()

# COMMAND ----------

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

# Assuming you have a DataFrame named 'df' with the relevant columns
# For example, let's say you have columns 'A', 'B', 'C', 'D', 'E', and 'Target'

# Select the relevant columns
cols = [
  'Recency', 'Frequency', 'Monetary', 
  'TotalFY23Revenue', 'TimesMailed'
  ]
data = df[cols]

# Standardize the data
scaler = StandardScaler()
scaled_data = scaler.fit_transform(data)

# Choose the number of clusters (you can experiment with this)
num_clusters = 10

# Apply K-means clustering
kmeans = KMeans(n_clusters=num_clusters, random_state=42)
df['Cluster'] = kmeans.fit_predict(scaled_data)

# Visualize the clusters
for cluster in range(5,num_clusters):
    cluster_data = df[df['Cluster'] == cluster]
    plt.scatter(cluster_data['RevenuePerTimesMailed'], cluster_data['TimesMailed'], label=f'Cluster {cluster}', alpha=.3)

plt.xlabel('RevenuePerTimesMailed')
plt.ylabel('TimesMailed')
plt.legend()
plt.show()


# COMMAND ----------

mask = df.GiftAmount < 1000
df.loc[mask].GiftAmount.hist(bins=100)

# COMMAND ----------


df.drop_duplicates('DonorID').TotalFY23Revenue.hist(bins=100)

# COMMAND ----------

df.groupby('DonorID').size().hist(bins=100)

# COMMAND ----------

import pandas as pd
import seaborn as sns

# # Create a pivot 
# # mask = df.AvgDateDelta < 370 
# mask = (df.TotalRevenue < 1e4) & (df.AvgDateDelta < 370) #& (df.LastGiftDate >= '2022-07-01')
# pivot_table = df.loc[mask].drop_duplicates('DonorID').pivot_table(index='NumGifts', columns='AvgDateDelta', values='TotalRevenue')
# # pivot_table = df.drop_duplicates('DonorID').pivot_table(index='NumGifts', columns='AvgDateDelta', values='TotalRevenue')

mask = df.RevenuePerTimesMailed < 200
pivot_table = df.loc[mask].drop_duplicates('DonorID').pivot_table(index='TimesMailed', columns='Frequency', values='RevenuePerTimesMailed')


# COMMAND ----------

# Create a heatmap using seaborn
plt.figure(figsize=(10, 6))
sns.heatmap(pivot_table, cmap='YlGnBu')#, annot=True, fmt='.1f', linewidths=.5)
# plt.title('MC - Total Revenue by Average Gift Interval and Number of Gifts - FY23')
plt.show()

# COMMAND ----------

mask = df.RevenuePerTimesMailed < 500
df.loc[mask].drop_duplicates('DonorID').RevenuePerTimesMailed.hist(bins=100)

# COMMAND ----------

df.GiftDate.hist(bins=183)#describe(datetime_is_numeric=True, percentiles=p)

# COMMAND ----------

