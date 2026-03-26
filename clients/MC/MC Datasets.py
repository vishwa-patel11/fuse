# Databricks notebook source
pip install XlsxWriter

# COMMAND ----------

from datetime import datetime, timedelta
import numpy as np
import os
import pandas as pd
import re
import zipfile

from pyspark.sql.functions import isnan
from pyspark.sql.types import *
from pyspark.sql import functions as F

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

# MAGIC %run ../python_modules/mount_datalake

# COMMAND ----------

# MAGIC %run ../python_modules/logger

# COMMAND ----------

# MAGIC %run ../python_modules/utilities

# COMMAND ----------

# establish file storage directories
client = 'MC'
filemap = Filemap(client)

# DONOR = '10389044'

# COMMAND ----------

filename = 'MC_FH_dataset.csv'
path = os.path.join(filemap.CURATED, filename).split('/dbfs')[-1]
DFFH = spark.read.format("csv").option("header","true").load(path)#.select(cols)

# COMMAND ----------

mask = DFFH['Period'] == 'fy'
DFFH = DFFH.filter(mask)
DFFH = DFFH.toPandas()

# COMMAND ----------

DFFH

# COMMAND ----------

DFFH.AppealCategory.unique()

# COMMAND ----------

mask = (DFFH.AppealCategory == 'Direct Mail') | (DFFH.AppealCategory == 'Direct Mail: Online')
df = DFFH.loc[mask]
df
#they want to find what they think is missing money 

# COMMAND ----------

df.AppealCategory.unique()

# COMMAND ----------

df.DonorID.unique()

# COMMAND ----------

pd.options.display.max_seq_items = None


# COMMAND ----------

UniqueDonors = pd.DataFrame(df.DonorID.unique())

# COMMAND ----------

print(UniqueDonors[0].tolist())

# COMMAND ----------

keydonors = UniqueDonors[0].tolist()
keydonors

# COMMAND ----------

filename = 'Data_FiveYears.csv'
path = os.path.join(filemap.MASTER, filename).split('/dbfs')[-1]
DF5Y = spark.read.format("csv").option("header","true").load(path)#.select(cols)

# COMMAND ----------

DF5Y = DF5Y.toPandas()
DF5Y

# COMMAND ----------

mask = (DF5Y.GiftDate >= '2022-07-01') & (DF5Y.GiftDate <= '2023-06-30')
DF5Y = DF5Y.loc[mask]
DF5Y.GiftDate.max()

# COMMAND ----------

# DF5Y['FilterDonors'] = [any([i for i in x if i in keydonors]) for x in DF5Y.DonorID]
# DF5Y

# COMMAND ----------

filtered_DF5Y = DF5Y[DF5Y['DonorID'].isin(keydonors)]

# COMMAND ----------

filtered_DF5Y #responses from audience in question
filtered_DF5Y.groupby('AppealCategory')['GiftAmount'].sum()

# COMMAND ----------

filtered_DF5Y['GiftAmount'] = filtered_DF5Y['GiftAmount'].apply(lambda x: int(float(x)))
filtered_DF5Y.GiftAmount.sum()

# COMMAND ----------

filename = 'Offline promos FY23 to date.csv'
path = os.path.join(filemap.RAW, filename).split('/dbfs')[-1]
PromoData = spark.read.format("csv").option("header","true").load(path)#.select(cols)
PromoData

# COMMAND ----------

PromoData = PromoData.toPandas()
PromoData

# COMMAND ----------

filtered_PromoData = PromoData[PromoData['Constituent ID'].isin(keydonors)]
filtered_PromoData

# COMMAND ----------

PromoData.columns

# COMMAND ----------

KeyPromoDonors = ["1349", "1992", "2029", "3262", "3463", "3963", "3981", "4542", "4817", "6042", "14059", "18778", "19604", "22668", "27140", "32121", "32338", "32454", "44521", "45648", "47245", "48345", "50959", "64534", "67276", "69160", "91370", "111366", "111508", "112351", "131615", "139700", "142944", "143032", "143038", "146790", "147881", "150862", "154343", "156330", "156698", "158086", "158481", "160805", "163201", "165385", "171302", "172782", "172898", "173213", "174185", "182765", "184919", "185943", "191091", "195613", "196558", "197822", "202929", "206688", "210055", "210788", "217839", "218460", "220932", "220988", "228173", "228906", "243911", "244095", "245771", "246226", "246793", "248406", "250169", "262026", "265515", "270071", "273745", "278112", "281911", "286442", "297619", "304801", "305830", "308802", "311817", "315626", "317123", "317720", "324415", "326316", "328929", "333164", "340916", "347910", "347989", "349449", "352680", "362893", "368169", "374036", "380370", "385489", "386584", "388363", "388510", "391981", "393725", "399357", "404013", "404051", "406511", "410641", "412398", "450535", "458176", "459695", "471952", "495624", "500354", "509359", "531334", "547435", "580781", "582925", "584547", "611061", "661119", "667836", "668557", "674909", "677491", "697998", "702033", "715747", "733399", "733805", "743155", "746137", "756269", "767257", "769592", "769982", "773388", "775284", "775655", "778675", "779122", "782908", "783374", "783803", "785628", "786835", "787970", "791228", "791349", "792219", "799007", "802809", "803191", "803824", "813868", "814243", "823463", "824572", "825467", "825759", "826065", "826096", "826191", "826441", "826883", "829708", "832318", "835374", "837450", "843812", "854749", "858888", "859733", "868505", "869115", "869559", "871010", "873316", "874433", "877014", "877244", "884979", "885598", "889067", "890992", "900296", "908901", "916324", "919160", "925618", "926027", "926901", "929071", "930374", "940919", "944844", "962121", "964208", "964399", "965223", "967121", "968929", "969470", "970233", "983682", "984251", "994233", "995618", "1010441", "1011598", "1032289", "1032338", "1033368", "1034442", "1041487", "1041621", "1045676", "1046318", "1046514", "1049077", "1050661", "1051663", "1052024", "1052748", "1053269", "1053583", "1055197", "1056929", "1056944", "1059072", "1059755", "1066441", "1077976"]

# COMMAND ----------

final_filtered_PromoData = PromoData[PromoData['Constituent ID'].isin(KeyPromoDonors)]
final_filtered_PromoData

# COMMAND ----------

cols = ['Constituent ID', 'Assigned Appeal Description']
final_filtered_PromoData.groupby(cols)

# COMMAND ----------

cols = ['Constituent ID', 'Assigned Appeal Description']
grouped_data = final_filtered_PromoData.groupby(cols)

# Create a list to store the grouped data
grouped_data_list = []

# Iterate through each group and extract the relevant information
for group, group_data in grouped_data:
    constituent_id = group[0]
    appeal_description = group[1]
    grouped_data_list.append([constituent_id, appeal_description])

# Create a DataFrame from the list
grouped_df = pd.DataFrame(grouped_data_list, columns=['Constituent ID', 'Assigned Appeal Description'])

# Print the DataFrame
grouped_df

# COMMAND ----------

import base64
import io

# Assuming you have a DataFrame named final_filtered_PromoData

cols = ['Constituent ID', 'Assigned Appeal Description']
grouped_data = final_filtered_PromoData.groupby(cols)

# Create a list to store the grouped data
grouped_data_list = []

# Iterate through each group and extract the relevant information
for group, group_data in grouped_data:
    constituent_id = group[0]
    appeal_description = group[1]
    grouped_data_list.append([constituent_id, appeal_description, group_data.shape[0]])

# Create a Pandas DataFrame from the list
grouped_df = pd.DataFrame(grouped_data_list, columns=['Constituent ID', 'Assigned Appeal Description', 'Count'])

# Create an Excel file in memory
excel_buffer = io.BytesIO()
grouped_df.to_excel(excel_buffer, index=False)

# Encode the Excel file content as Base64
excel_base64 = base64.b64encode(excel_buffer.getvalue()).decode()

# Provide a link to download the Excel file
displayHTML(f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{excel_base64}" download="grouped_data.xlsx">Click here to download Excel file</a>')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# if source code / appeal id and if it matches take it assign it done
# if appeal id does not find match take second step to find another idenitifying factor to match them 
# example of 7.4 million, 7.1 million match, amount that dont match figure out next steps
