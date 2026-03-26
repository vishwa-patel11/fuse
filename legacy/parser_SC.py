# Databricks notebook source



 ## Helper functions for parsing raw client data

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %run ./RFM_Lookup

# COMMAND ----------

# MAGIC %run ../MC/mc_ukraine

# COMMAND ----------

# MAGIC %run ../FFB/FFB_helpers

# COMMAND ----------

# MAGIC %run ../TSE/TSE_helpers

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# helpers
def check_col(col, pattern):
  r = re.compile(pattern)
  regmatch = np.vectorize(lambda x: bool(r.match(x)))
  return regmatch(col)

def convert_sn(s):
  try:
    return str(int("{:.0f}".format(float(s))))
  except:
    return s

# COMMAND ----------

# MAGIC %md #Global functions

# COMMAND ----------

# Global functions

def apply_client_specific(df, client):
  try:
    return globals()[client](df)
  except:
    print('no client specific applied')
    return df

def apply_func(df, d, col):
  values = d[col]['Values']
  func = d[col]['Function']
  inc = d[col]['Include']
  return globals()[func](df, col, values, inc)

def apply_dedupe(df, d):
  if d:
    col = d['Column']
    func = d['Function']
    print("Client specific Dedupe Applied")
    return globals()[func](df, col)
  else:
    print("df.drop_duplicates applied")
    return df.drop_duplicates()

def apply_filters(df, f):
  if f in df.columns:
    return df
  else:
    return globals()[f](df)
  

# COMMAND ----------

# MAGIC %md #Suppressions

# COMMAND ----------

# Suppressions

def isin(df, col, values, inc):
  mask = df[col].isin(values)
  if inc:
    return(df.loc[mask])
  else:
    return(df.loc[~mask])
  
def contains(df, col, values, inc):
  pattern = '|'.join(values)
  mask = df[col].str.contains(pattern).fillna(value=False)
  if inc:
    return df[mask]
  else:
    return df[~mask]
  
def isin_or(df, col, values, inc):
  conditions = []
  for k,v in values.items():
    condition = df[k].isin(v)
    conditions.append(condition)
  mask = np.logical_or.reduce(conditions)
  if inc:
    return(df[mask])
  else:
    return(df[~mask])
  
def dropna(df, col, values, inc):
  df['mask'] = [str(x) in values for x in df[col].values.tolist()]
  df = df.dropna(subset=['mask'])
  return df[df['mask']]

def greater_than(df, col, values, inc): #Added 8/22/24 to hand HKI
  value = values[0]
  mask = df[col] > value
  if inc:
      return df[mask]
  else:
      return df[~mask]

# COMMAND ----------

# MAGIC %md #Dedupe functions

# COMMAND ----------

# Dedupe functions    
  
def dedupe_or_null(df, col):
  mask = (~df[col].duplicated()) | (df[col].isna())
  return df[mask]

def dedupe(df, col): #keeps first occurence of dupe
  return df.drop_duplicates(col)  

def dedupe_dropna(df, col):
  return df.drop_duplicates(col).dropna(axis=0, subset=[col])

def dedupe_dropna_decoupled(df, col):
  print('drop duplicates of column: ', col)
  print('rows: ', df.shape[0])
  df = df.dropna(axis=0, subset=[col])
  print('rows: ', df.shape[0])
  df = df.drop_duplicates(col)
  print('rows: ', df.shape[0])
  return df

def ignore_dupes(df, col):
  return df

def dedupe_all(df, col):
  return df.drop_duplicates()

def dedupe_keep_last(df, col):
  return df.drop_duplicates(col,keep='last').dropna(axis=0, subset=[col])

def dedupe_naive(df, col):
  cols = ['DonorID', 'GiftAmount', 'GiftDate']
  return df.drop_duplicates(cols, keep='last')

def dedupe_WFP(df, col):
  cols = ['GiftID']
  df = df.sort_values('GiftDate', ascending = False)
  return df.drop_duplicates(cols, keep='first')

def dedupe_naive_U4U(df, col):
  cols = ['DonorID', 'GiftAmount', 'GiftDate', 'GiftID'] #Gift ID added on 
  return df.drop_duplicates(cols, keep='last')

def dedupe_naive_USO(df, col):
  cols = ['Source Code', 'Mail Date', 'Mail Quantity', 'Campaign Type'] 
  return df.drop_duplicates(cols, keep='last')







# COMMAND ----------

# MAGIC %md #Features - all clients

# COMMAND ----------

def DonorGroup(df, feature='DonorGroup', donor_col='DonorID'):
  _df = df.groupby(donor_col).size().to_frame(name='NumGifts').reset_index()
  condition = _df['NumGifts'] > 1
  _df[feature] = np.where(condition, 'Multi', 'Single')
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')


def get_fh_FirstGiftDate(df, feature='FirstGiftDate', donor_col='DonorID', date_col='GiftDate'):
  grouped = df.groupby(donor_col)[date_col].min().to_frame(name=feature)
  return pd.merge(df, grouped, on=donor_col, how='left')


def GiftAmountFlag(df, feature='GiftAmountFlag', gift_col='GiftAmount', amount=10000, low_label='<$10,000', hi_label='>=$10,000'):
  condition = df[gift_col] < amount
  df[feature] = np.where(condition, low_label, hi_label)  
  return df


def GiftHistory(df, feature='GiftHistory', donor='DonorID', gift='GiftID', multi='Multi', single='Single'):
  if gift not in df.columns:
    df[gift] = [i for i in range(df.shape[0])]
    
  _df = df.groupby(donor)[gift].nunique().to_frame(name='NumGifts')
  
  condition = _df['NumGifts'] > 1
  _df[feature] = np.where(condition, multi, single)
  
  cols = [donor, feature]
  return pd.merge(df, _df[feature], on=donor, how='left')


def GiftLevel(df, feature='GiftLevel', gift_col='GiftAmount', null_label='No GiftLevelID'):
  
  expanded = expand_rfm_d(monetary)
  
  df['GiftAmountRounded'] = np.floor(df[gift_col]).astype('Int32')
  condition = df['GiftAmountRounded'] < 1
  df['GiftAmountRounded'] = np.where(condition, 1, df['GiftAmountRounded'])

  df[feature] = df['GiftAmountRounded'].map(expanded)
  df[feature] = df[feature].fillna(value=null_label)
  
  return df


def GiftMonth(df, feature='GiftMonth', date_col='GiftDate'):  
  df[feature] = df[date_col].dt.month  
  return df


def JoinLevel(
  df, feature='JoinLevel', gift_col='GiftAmount', 
  donor_col='DonorID', date_col='GiftDate', 
  first_date='FirstGiftDate', null_label='No GiftLevelID'
  ):
  
  expanded = expand_rfm_d(monetary)
  
  df = df.dropna(axis=0, subset=[gift_col])
  df = df.dropna(axis=0, subset=[donor_col])
  
  mask = df[date_col] == df[first_date]
  _df = df[mask]
  _df = _df.drop_duplicates(donor_col)
  
  _df['GiftAmountRounded'] = np.floor(_df[gift_col]).astype('Int32')
  condition = _df['GiftAmountRounded'] < 1
  _df['GiftAmountRounded'] = np.where(condition, 1, _df['GiftAmountRounded'])

  _df[feature] = _df['GiftAmountRounded'].map(expanded)
  _df[feature] = _df[feature].fillna(value=null_label)
  
  return pd.merge(df, _df[[donor_col, feature]], on=donor_col, how='left')
  

def JoinCY(df, feature='JoinCY', first_date='FirstGiftDate'):
  df[feature] = df[first_date].dt.year
  return df


def JoinFiscal(df, feature='JoinFiscal', first_date='FirstGiftDate'):
  fy_month = schema["firstMonthFiscalYear"] 
  df[feature] = fiscal_from_column(df, first_date, fy_month)
  return df


def JoinMonth(df, feature='JoinMonth', first_date='FirstGiftDate'):
  df[feature] = df[first_date].dt.month
  return df


def JoinYear(df, feature='JoinFiscalYear', first_date='FirstGiftDate'):
  fym = schema['firstMonthFiscalYear']
  df[feature] = fiscal_from_column(df, first_date, fym)
  return df


def JoinFiscalYear(df, feature='JoinFiscalYear', first_date='FirstGiftDate'):
  fym = schema['firstMonthFiscalYear']
  df[feature] = fiscal_from_column(df, first_date, fym)
  return df

# COMMAND ----------

# MAGIC %md #Client transformations

# COMMAND ----------

# MAGIC %md #AFHU

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def AFHU(df):
  condition = (df['AppealCategory'] == 'Solicitation') & (~df['Online Donations - Form Name'].isna())
  df['AppealCategory'] = np.where(condition, 'Online', df['AppealCategory'])
  df['Gift ID'] = df['Gift ID'].astype(str)
  return df

#FH Filter
def AFHU_JoinAppeal(
  df, feature='AFHU_JoinAppeal', donor_col='DonorID', date_col='GiftDate', 
  first_gift_col='FirstGiftDate', appeal_col='AppealCategory'
  ):
  mask = df[date_col] == df[first_gift_col]
  _df = df[mask].drop_duplicates([donor_col, first_gift_col])
  _df[feature] = _df[appeal_col]
  
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=donor_col, how='left')

#FH Filter
def AFHU_JoinChannel(
  df, feature='AFHU_JoinChannel', ref_col='AppealCategory', 
  sort_values=['DonorID', 'GiftDate'], dup_value='DonorID'
  ):  
  
  _df = df.sort_values(sort_values).drop_duplicates(dup_value, keep='first')
  _df[feature] = _df[ref_col]
  
  cols = [dup_value, feature]
  df = pd.merge(df, _df[cols], on=dup_value, how='left')
  
  return df

#FH Filter
def AFHU_Channel(df, feature='AFHU_Channel'):
  df[feature] = 'Other'

  mask = df['Appeal ID'].fillna('').str.contains('DM')
  df.loc[mask, feature] = 'Direct Mail'

  mask = df['Appeal ID'].fillna('').str.contains('SOL')
  df.loc[mask, feature] = 'Solicitation'

  mask = df['Appeal ID'].fillna('').str.contains('EV')#@efp
  df.loc[mask, feature] = 'Event'

  return df
  
#FH Filter
def AFHU_ChannelBehavior(df, feature='AFHU_ChannelBehavior'):
  _df = df.groupby('DonorID')['AFHU_Channel'].nunique().to_frame(name='NumChannels').reset_index()
  _df[feature] = np.where(_df.NumChannels > 1, 'MultiChannel', 'SingleChannel')
  cols = ['DonorID', feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left') 

# COMMAND ----------

# MAGIC %md #CARE

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def CARE(df):
  print('inside client specific')
  df.SourceCode = [convert_sn(s) for s in df.SourceCode]
  mask = df.GiftAmount.notna()
  df = df.loc[mask]
  print("Source Code updates complete")

  #Update DonorID to string
  df['DonorID'] = df['DonorID'].astype(str).str.strip()
  # Remove '.0' suffix from 'DonorID' using regex
  df['DonorID'] = df['DonorID'].str.replace(r'\.0$', '', regex=True)
  print('DonorID updated to string')
  

  #Fill in blank GiftIDs with Legacy SF IDs. Keep Legacy SF ID column 
  if 'Legacy SF ID' in df.columns:
    df['GiftID'] = df['GiftID'].fillna(df['Legacy SF ID'].astype(str) + '_legacy')
  print('Blank GiftIDs filled with Legacy SF IDs if applicable')
  
  print('Client Specific Functions Completed')
  return df




# COMMAND ----------

# MAGIC %md #CHOA

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def CHOA(df):
  df['SourceCode'] = df.AppealID + df.PackageID.str[:4] + df['RFM Code'].str.replace('None','')
  df.SourceCode = df.SourceCode.str.replace('\.0','')
  cond = df.SourceCode.str.startswith(('DM', 'SO', 'CR')).fillna(value=False)
  df['CampaignCode'] = np.where(cond, df.SourceCode.str[:7], None)
  cond = (df.AppealID == 'CR0722') | (df.AppealID == 'SO0822')
  df.CampaignCode = np.where(cond, df.AppealID, df.CampaignCode)
  df.CampaignCode = np.where(df.CampaignCode.isna(),df.AppealID,df.CampaignCode)
  mask = df['Gf_SCMGFlag'].isin(["Soft Credit Recipient"])
  return df.loc[~mask]

#FH Filter
def CHOA_Channel(
  df, feature='CHOA_Channel', donor_col='DonorID', appeal_col='AppealCategory', 
  temp_col='numChannels', multi_label='Multi'
  ):

  grouped = df.groupby(donor_col)[appeal_col].nunique().to_frame(name=temp_col)
  df = pd.merge(df, grouped, on=donor_col, how='left')

  condition = df[temp_col] == 1
  df[feature] = np.where(condition, df[appeal_col], multi_label)
  
  return df

#FH Filter
def CHOA_JoinChannel(
  df, feature='CHOA_JoinChannel', ref_col='AppealCategory', 
  sort_values=['DonorID', 'GiftDate'], dup_value='DonorID'
  ):  
  ''' TODO '''
  _df = df.sort_values(sort_values).drop_duplicates(dup_value, keep='first')
  _df[feature] = _df[ref_col]
  
  cols = [dup_value, feature]
  df = pd.merge(df, _df[cols], on=dup_value, how='left')
  
  return df

#FH Filter
def CHOA_ChannelBehavior(df, feature='CHOA_ChannelBehavior'):
  _df = df.groupby('DonorID')['CHOA_Channel'].nunique().to_frame(name='NumChannels').reset_index()
  _df[feature] = np.where(_df.NumChannels > 1, 'MultiChannel', 'SingleChannel')
  cols = ['DonorID', feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left') 

#FH Filter
def OnlineDisbursements(
  df, feature='OnlineDisbursements', appeal_col='AppealDescription', 
  value='Online Disbursements', true_label='True', false_label='False'
  ):
  # CHOA
  condition = df[appeal_col] == value
  df[feature] = np.where(condition, true_label, false_label)  
  return df


# COMMAND ----------

# MAGIC %md #FFB

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def FFB(df):
    df['DonorID'] = df['DonorID'].astype(str).str.strip()
    # Remove '.0' suffix from 'DonorID' using regex
    df['DonorID'] = df['DonorID'].str.replace(r'\.0$', '', regex=True)

    #Drop columns with bit characters
    df = df.drop(columns=df.filter(like='¿"Contact Id"').columns)

    #Align SourceCode column to Segment Code
    df['SourceCode'] = df['Segment Code']
    print("SourceCode built from Segment Code")

    #Add Campaign Code (Temporary, need to validate 10/9/24)
    # Define the regex pattern
    pattern = re.compile(r'^[A-Z]{2,3}[A-Z]{2}\d{3}$')
    # Apply logic to update CampaignCode only if it matches the pattern
    df['CampaignCode'] = df.apply(
        lambda row: row['SourceCode'][3:10].upper() 
        if isinstance(row['SourceCode'], str) and len(row['SourceCode']) >= 10 and pattern.match(row['SourceCode'][3:10].upper()) 
        else '',
        axis=1)

    print("CampaignCode updated based on SourceCode matching the updated pattern")

    # #CREATE PROGRAM FILTER
    # # Replace 'nan' and 'None' strings with actual NaN values
    # df['Media Outlet Code'].replace(['nan', 'None'], np.nan, inplace=True)
    # df['Segment Code'].replace(['nan', 'None'], np.nan, inplace=True)
    # df['Project Code'].replace(['nan', 'None'], np.nan, inplace=True)
    # # Convert 'Media Outlet Code' to string and strip whitespaces
    # df['Media Outlet Code'] = df['Media Outlet Code'].astype(str).str.strip()
    # # Remove '.0' suffix from 'Media Outlet Code' using regex
    # df['Media Outlet Code'] = df['Media Outlet Code'].str.replace(r'\.0$', '', regex=True)
    # # Convert 'Segment Code' and 'Project Code' to strings and strip whitespaces
    # df['Segment Code'] = df['Segment Code'].astype(str).str.strip()
    # df['Project Code'] = df['Project Code'].astype(str).str.strip()
    # # Define conditions for 'CORE' and 'VC' programs
    # cond_core = (
    #     (df['Media Outlet Code'] == "9005") | 
    #     (df['Media Outlet Code'] == "Direct Marketing") | 
    #     (df['Media Outlet Code'] == "9500")
    # )
    # cond_vc = (
    #     ((df['Media Outlet Code'] == "9000") & 
    #      (df['Segment Code'].str.contains("VC|9500|Mid-Level", case=False, na=False))) | 
    #     (df['Project Code'].str.contains("9500", na=False))
    # )

    # # Apply conditions to create the 'Program' column
    # df['Program'] = np.select([cond_core, cond_vc], ['CORE', 'VC'], default='Other')
    # # Sort by 'Donor ID' and 'Gift Date' to ensure the first gift is at the top for each donor
    # df = df.sort_values(by=['DonorID', 'GiftDate'])
    # # Get the first gift's FFB_Program for each donor and assign it as FFB_JoinProgram
    # df['JoinProgram'] = df.groupby('DonorID')['Program'].transform('first')
    
    
    #Columns to convert to string
    columns_to_check = ['GiftID', 'Media Outlet Code']

    for column in columns_to_check:
        if column in df.columns:
            # Convert to string and remove '.0' only if it is at the end
            df[column] = df[column].astype(str).replace(r'\.0$', '', regex=True)
            print(f"{column} is now a string and '.0' has been stripped where applicable")
        else:
            print(f"{column} column not found in the dataframe")


    print('Completed FFB Client Specific')

    return df

# def FFB(df):
#   df['DonorID'] = df['DonorID'].astype(str).str.strip()
#   # Remove '.0' suffix from 'DonorID' using regex
#   df['DonorID'] = df['DonorID'].str.replace(r'\.0$', '', regex=True)
  
#     #   # keep only numeric DonorIDs
#     #   # "Keycode": "SourceCode"
#     #   col = 'DonorID'
#     #   df[col] = df[col].astype(str)
#     #   mask = pd.to_numeric(df[col], errors='coerce').notnull()
#     #   df = df[mask]
#     #   df[col] = df[col].astype(int)
  
#     #   # keep only numeric CampaignCodes
#     #   col = 'CampaignCode'
#     #   df[col] = df[col].astype(str)
#     #   mask = pd.to_numeric(df[col], errors='coerce').notnull()
#     #   df = df[mask]
#     #   df[col] = df[col].astype(float).astype(int).astype(str)

#     #   # keep only numeric CampaignCodes
#     #   col = 'CampaignCode'
#     #   df[col] = df[col].astype(str)
    
#     #   # Fix Purpose codes
#     #   col = 'Purpose'
#     #   df = df.dropna(axis=0, subset=[col])
#     #   df[col] = df[col].apply(float_to_int)
#     #   condition = df[col].isin(data_mapper.keys())
#     #   df[col] = np.where(condition, df[col].map(data_mapper), df[col])
  
#   # # Fix Source codes (Old code, confirming if correct CC 10/9/24)
#   # col = 'Source'
#   # df[col] = df[col].fillna(value='None')
#   # df[col] = df[col].apply(float_to_int_try)
  
#   # df['GiftDate'] = pd.to_datetime(
#   #   pd.to_datetime(
#   #     df['GiftDate']
#   #   ).dt.date
#   # )
#   # sc = pd.read_csv(os.path.join(filemap.MASTER, 'SourceCode.csv'))
#   # print('sc shape: ', sc.shape)
  
#   # if 'SourceCode' in df.columns:
#   #   df = df.drop('SourceCode', axis=1)
#   # df = (df, sc)
#   # print('source code nulls: ', df.SourceCode.isna().sum())

#   #Align SourceCode column to Segment Code
#   df['SourceCode'] = df['Segment Code']

#   return df

def FFB_JoinChannel(df, feature='FFB_JoinChannel', donor_col='DonorID', gift_col='GiftDate', ref_col='FFB_Channel'):
    # Ensure 'GiftDate' is in datetime format for accurate sorting
    df[gift_col] = pd.to_datetime(df[gift_col], errors='coerce')
    
    # Sort by donor and gift date, get the first gift per donor
    _df = df.sort_values([donor_col, gift_col]).drop_duplicates(subset=donor_col, keep='first')
    
    # Assign the 'FFB_Channel' of the first gift to 'FFB_JoinChannel'
    _df[feature] = _df[ref_col]
    
    # Merge the 'FFB_JoinChannel' back into the original DataFrame
    cols = [donor_col, feature]
    df = pd.merge(df, _df[cols], on=donor_col, how='left')
    
    return df

def FFB_SegmentCode(df):
    # Ensure 'Segment Code' is properly formatted
    df['Segment Code'] = df['Segment Code'].astype(str).str.strip()
    # Replace 'nan' and 'None' strings with actual NaN values
    df['Segment Code'].replace(['nan', 'None', ''], np.nan, inplace=True)
    # Assign 'Segment Code' to 'FFB_SegmentCode'
    df['FFB_SegmentCode'] = df['Segment Code']
    return df

def FFB_MediaOutletCode(df):
    # Ensure 'Media Outlet Code' is properly formatted
    df['Media Outlet Code'] = df['Media Outlet Code'].astype(str).str.strip()
    # Replace 'nan' and 'None' strings with actual NaN values
    df['Media Outlet Code'].replace(['nan', 'None', ''], np.nan, inplace=True)
    # Remove '.0' suffix from 'Media Outlet Code' using regex
    df['Media Outlet Code'] = df['Media Outlet Code'].str.replace(r'\.0$', '', regex=True)
    # Assign 'Media Outlet Code' to 'FFB_MediaOutletCode'
    df['FFB_MediaOutletCode'] = df['Media Outlet Code']
    return df


# def FFB_JoinChannel(df, feature='FFB_JoinChannel', donor_col='DonorID', gift_col='GiftDate', ref_col='Campaign Name'):
#   _df = df.sort_values([donor_col, gift_col]).drop_duplicates(donor_col)
#   _df[feature] = _df[ref_col]

  # cols = [donor_col, feature]
  # return pd.merge(df, _df[cols], on=donor_col, how='left')

def FFB_ChannelBehavior(df, feature='FFB_ChannelBehavior'):
  _df = df.groupby('DonorID')['FFB_Channel'].nunique().to_frame(name='NumChannels').reset_index()
  _df[feature] = np.where(_df.NumChannels > 1, 'MultiChannel', 'SingleChannel')
  cols = ['DonorID', feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left') 



def FFB_Program(df):
    # Replace 'nan' and 'None' strings with actual NaN values
    df['Media Outlet Code'].replace(['nan', 'None'], np.nan, inplace=True)
    df['Segment Code'].replace(['nan', 'None'], np.nan, inplace=True)
    df['Project Code'].replace(['nan', 'None'], np.nan, inplace=True)
    
    # Convert 'Media Outlet Code' to string and strip whitespaces
    df['Media Outlet Code'] = df['Media Outlet Code'].astype(str).str.strip()
    
    # Remove '.0' suffix from 'Media Outlet Code' using regex
    df['Media Outlet Code'] = df['Media Outlet Code'].str.replace(r'\.0$', '', regex=True)
    
    # Convert 'Segment Code' and 'Project Code' to strings and strip whitespaces
    df['Segment Code'] = df['Segment Code'].astype(str).str.strip()
    df['Project Code'] = df['Project Code'].astype(str).str.strip()
    
    # Define conditions for 'CORE' and 'VC' programs
    cond_core = (
        (df['Media Outlet Code'] == "9005") | 
        (df['Media Outlet Code'] == "Direct Marketing") | 
        (df['Media Outlet Code'] == "9500")
    )
    
    cond_vc = (
        ((df['Media Outlet Code'] == "9000") & 
         (df['Segment Code'].str.contains("VC|9500|Mid-Level", case=False, na=False))) | 
        (df['Project Code'].str.contains("9500", na=False))
    )
    
    # Apply conditions to create the 'FFB_Program' column
    df['FFB_Program'] = np.select([cond_core, cond_vc], ['CORE', 'VC'], default='Other')
    
    return df

def FFB_JoinProgram(df):
    # Sort by 'Donor ID' and 'Gift Date' to ensure the first gift is at the top for each donor
    df = df.sort_values(by=['DonorID', 'GiftDate'])
    
    # Get the first gift's FFB_Program for each donor and assign it as FFB_JoinProgram
    df['FFB_JoinProgram'] = df.groupby('DonorID')['FFB_Program'].transform('first')
    
    return df


def FFB_Channel(df):
    # Data Cleaning Steps (from previous code)
    
    # Replace 'nan' and 'None' strings with actual NaN values
    df['Media Outlet Code'].replace(['nan', 'None'], np.nan, inplace=True)
    df['Segment Code'].replace(['nan', 'None'], np.nan, inplace=True)
    df['Project Code'].replace(['nan', 'None'], np.nan, inplace=True)
    df['Original URL'].replace(['nan', 'None'], np.nan, inplace=True)
    
    # Convert 'Media Outlet Code' to string and strip whitespaces
    df['Media Outlet Code'] = df['Media Outlet Code'].astype(str).str.strip()
    # Remove '.0' suffix from 'Media Outlet Code' using regex
    df['Media Outlet Code'] = df['Media Outlet Code'].str.replace(r'\.0$', '', regex=True)
    
    # Convert 'Segment Code' and 'Project Code' to strings and strip whitespaces
    df['Segment Code'] = df['Segment Code'].astype(str).str.strip()
    df['Project Code'] = df['Project Code'].astype(str).str.strip()
    
    # Ensure 'Original URL' is a string and strip whitespaces
    df['Original URL'] = df['Original URL'].astype(str).str.strip()
    
    # Define conditions for 'FFB_Channel'
    conditions = [
        # First priority: Paid Digital
        (df['Original URL'].str.contains("give|givenow|donatenow|givelegacy|giveobituary|givechoice|WEBA50", na=False, case=False)) |
        (df['Segment Code'].str.contains("WEBA50|W50|50YEARS|ONLSPECPAIDADS|ONLSPECWELPSA", na=False, case=False)),
        
        # Second priority: DM
        (df['Media Outlet Code'].isin(["9005", "Direct Marketing"])) & 
        (df['Segment Code'].str.contains("AQ|CR", na=False, case=False)) & 
        (~df['Segment Code'].str.contains("ONL", na=False, case=False)),
        
        # Third priority: Other (based on Media Outlet Code)
        (df['Media Outlet Code'].isin(["9005", "Direct Marketing"])),
        
        # Fourth priority: VC
        ((df['Media Outlet Code'] == "9000") & 
         (df['Segment Code'].str.contains("VC|9500|Mid-Level", na=False, case=False))) | 
         (df['Project Code'].str.contains("9500", na=False))
    ]
    
    choices = ['Paid Digital', 'DM', 'Other', 'VC']
    
    # Apply conditions to create the 'FFB_Channel' column
    df['FFB_Channel'] = np.select(conditions, choices, default='Other')
    
    return df

def Fuse_Program(df):
    # Ensure 'FFB_Program' and 'FFB_Channel' columns exist
    if 'FFB_Program' not in df.columns or 'FFB_Channel' not in df.columns:
        raise ValueError("Columns 'FFB_Program' and 'FFB_Channel' must exist in the DataFrame.")

    # Create the conditions
    condition = (
        (df['FFB_Program'].isin(['CORE', 'VC'])) |
        ((df['FFB_Program'] == 'Other') & (df['FFB_Channel'] == 'Paid Digital'))
    )

    # Apply the conditions to create the 'Fuse Program' column
    df['Fuse_Program'] = np.where(condition, 'Yes', 'No')

    return df




# COMMAND ----------

# MAGIC %md #FWW

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def FWW(df):  
  print('inside FWW client specific')
  df.columns = [x.replace(' ','') for x in df.columns]
  cols = [x for x in df.columns if 'Unnamed' not in x]
  df = df[cols]
  print('cols: ', df.columns)
  
  # fix source codes
  # read supplemental source codes data
  f = 'segment codes for migrated recurring gifts.csv'
  _df = pd.read_csv(os.path.join(filemap.RAW, 'Historical', f))
  print('read segment codes file')

  # rename the columns and take only those needed
  col_map = {
    'Recurring Donation: Segment Code': 'SegmentCode',
    'Opportunity ID': 'GiftID'
  }
  cols = ['GiftID', 'SegmentCode']
  _df = _df.rename(columns=col_map)[cols]
  print('renamed cols')
  
  # merge supplemental source codes back to the transaction dataframe
  df = pd.merge(df, _df, on='GiftID', how='left')
  del _df
  print('merged')
  print(sorted(df.columns))

  # replace null source codes
  condition = df['SourceCode'].isna()
  df['SourceCode'] = np.where(condition, df['SegmentCode'], df['SourceCode'])
  print('fixed scource codes')

  # drop the supplemental column
  df = df.drop('SegmentCode', axis=1)
  print('dropped segment codes')
  
  
  # fix contact ids
  # read contact id mapping
  file = 'Adding contact ID.xlsx'
  sheet = 'FWW_TransactionHistoryWithNullC'
  cols = ['Primary Contact: 18 Digit Contact ID', 'Opportunity ID']
  _df = pd.read_excel(os.path.join(filemap.RAW, 'Historical', file), sheet_name=sheet, usecols=cols).dropna()
  print('contact id')

  # rename the columns
  col_map = {
    'Primary Contact: 18 Digit Contact ID': 'NewContactID',
    'Opportunity ID': 'GiftID'
  }
  _df = _df.rename(columns=col_map)
  print('rename 2')
  
  # merge supplemental contact ids back to the transaction dataframe
  df = pd.merge(df, _df, on='GiftID', how='left')
  del _df
  print('merge 2')

  # replace null contact ids
  col = 'DonorID'
  condition = df[col].isna()
  df[col] = np.where(condition, df['NewContactID'], df[col])
  print('donor id')

  # drop the supplemental column
  df = df.drop('NewContactID', axis=1)
  print('new contact id')
  
  
  # soft credits
  # read the soft credit data
  file = 'Partial Soft Credit Transactions fuse-2022-08-05-09-22-36.csv'
  _df = pd.read_csv(os.path.join(filemap.RAW, 'Historical', file))
  print('read soft credits')

  # rename the columns and take only those needed
  col_map = {
    'Opportunity: Opportunity ID': 'GiftID',
    '18 Digit Contact ID': 'NewContactID',
    'Amount': 'GiftAmount'
  }
  cols = ['GiftID', 'Partial Soft Credit: ID', 'GiftAmount', 'NewContactID']
  _df = _df.rename(columns=col_map)[cols]
  print('rename 3')

  # record the set of soft credits in question, for filtering later
  soft_credits = _df['GiftID'].unique()
  
  # merge the transaction dataframe to the soft credit data
  cols = ['GiftAmount', 'DonorID']
  _df = pd.merge(_df, df.drop(cols, axis=1), on='GiftID', how='left')
  print('merge 3')

  # drop the unneeded merge column
  _df = _df.drop('GiftID', axis=1)
  print('dropped gift id')

  # rename the columns to make them like the transaction history
  col_map = {
    'Partial Soft Credit: ID': 'GiftID',
    'NewContactID': 'DonorID'
  }
  _df = _df.rename(columns=col_map)
  print('rename 4')
  
  # filter the Opportunity IDs in question out of the transaction history
  # as these are now contained in the supplemental dataset (_df)
  mask = df['GiftID'].isin(soft_credits)
  df = df.loc[~mask]
  print('mask for soft credits')
  print('df shape: ', df.shape)
  print('_df shape: ', _df.shape)

  # # combine the two datasets
  # df = df.append(_df)
  try:
    # df = pd.concat([df.reset_index(drop=True), _df.reset_index(drop=True)])
    df = pd.concat([df, _df])
    print('df shape: ', df.shape)
    del _df
    print('concat')
  except Exception as e:
    print('exception: ', e)
    raise e

  def _extract_campaign_codes(s):
    try:
      return '-'.join(s.split('-')[:2])
    except:
      return None
  df['CampaignCode'] = [_extract_campaign_codes(s) for s in df['SourceCode']]  
  print('campaign codes')

  df.columns = [x.replace(' ', '') for x in df.columns]
  df['GiftAmount'] = format_currency(df['GiftAmount'])
  mask = df['GiftAmount'] > 0
  
  df = df.loc[mask]

  # Read in Member Category Code Substitutions file and update Member Category Codes 9/16/24
  file = 'FWW_Members_Category_Code_Substitutions.csv'
  FWW_subs_df = pd.read_csv(os.path.join(filemap.RAW, 'Historical', file))
  # Create a mapping from FWW_subs
  mapping = FWW_subs_df.set_index('Opportunity ID')['Substitute Members Code Category']
  # Replace the values in fww_parquet['MembersCodeCategory'] based on the mapping
  df['MembersCodeCategory'] = df['GiftID'].map(mapping).combine_first(df['MembersCodeCategory'])
  print('Mapped FWW MCC subs')
  
  print('Completed client specific in parser')
  return df
  
#FH Filter
def FWW_Channel(df, feature='FWW_Channel', ref_col='SourceCode'):
  df[feature] = df[ref_col].str[2]
  d = {
    'P': 'Agency Postal',
    'T': 'Telemarketing',
    'E': 'Email',
    'L': 'P2P Mobile',
    'B': 'Broadcast Mobile',
    'W': 'Web',
    'A': 'Online Ads',
    'S': 'Social Media',
    'M': 'in-person Meeting',
    'C': 'Canvass',
    'J': 'Workplace Giving',
    'N': 'Event',
    'H': 'Internal Postal',
    'V': 'Virtual Event',
    'R': 'Proposal',
    'D': 'Phonebanking'
  }
  df[feature] = df[feature].map(d)
  return df

#FH Filter
def FWW_JoinChannel(df, feature='FWW_JoinChannel', donor_col='DonorID', gift_col='GiftDate', ref_col='FWW_Channel'):
  _df = df.sort_values([donor_col, gift_col]).drop_duplicates(donor_col)
  _df[feature] = _df[ref_col]

  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=donor_col, how='left')

#FH Filter
def FWW_C3C4(
  df, feature='FWW_C3C4', pf_col='PrimaryFund', c3_kw='general fund'
  ):
  condition = df[pf_col].str.lower().str.contains(c3_kw).fillna(value=False)
  df[feature] = np.where(condition, 'C3', df[pf_col])
  return df

#FH Filter
def FWW_DonorGroup(
  df, feature='FWW_DonorGroup', source_col='FWW_C3C4', 
  donor_col='DonorID'
  ):
  grouped = df.groupby(donor_col)[source_col].unique().to_frame(name=feature)
  df = pd.merge(df, grouped, on=donor_col, how='left')
  return df

#FH Filter
def FWW_JoinSource(
  df, feature='FWW_JoinSource', source_col='FWW_C3C4', 
  donor_col='DonorID', date_col='GiftDate', temp_col='first_gift_date'
  ):
  grouped = df.groupby(donor_col)[date_col].min().to_frame(temp_col)
  df = pd.merge(df, grouped, on=donor_col, how='left')
  
  mask = df[date_col] == df[temp_col]
  _df = df[mask].drop_duplicates(subset=donor_col, keep='last')
  _df[feature] = _df[source_col]
  
  cols = [donor_col, feature]  
  df = pd.merge(df, _df[cols], on=donor_col, how='left')
  
  del _df
  return df

#FH Filter
def FWW_Online(
  df, feature='FWW_Online', ref_col='OnlineGift', 
  campaign_col='CampaignName', kw='online'
  ):
  condition = df[ref_col].astype(bool)
  df[feature] = np.where(condition, 'True', 'False')
  
  condition = df[campaign_col].str.lower().str.contains(kw).fillna(value=False)
  df[feature] = np.where(condition, 'True', df[feature])
  return df

#FH Filter
def FWW_Program(
  df, feature='FWW_Program', ref_col='MembersCodeCategory'
  ):
  df[feature] = df[ref_col]
  return df

#FH Filter
def FWW_Sustainer(
  df, feature='FWW_Sustainer', sus_gift_col='IsRecurringDonation', 
  donor_col='DonorID', sus_label='Sustainer', one_time_label='1x'
  ):
  mask = df[sus_gift_col].astype(bool)
  sus_donors = df[mask][donor_col].unique()
  
  condition = df[donor_col].isin(sus_donors)
  df[feature] = np.where(condition, sus_label, one_time_label)

  return df

#FH Filter
def FWW_ChannelBehavior(df, feature='FWW_ChannelBehavior'):
  _df = df.groupby('DonorID')['FWW_Channel'].nunique().to_frame(name='NumChannels').reset_index()
  _df[feature] = np.where(_df.NumChannels > 1, 'MultiChannel', 'SingleChannel')
  cols = ['DonorID', feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left') 


# COMMAND ----------

# MAGIC %md #HKI

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def HKI(df):
    print('inside client specific')

    # fix legacy donor id
    OLD_ID = 198024
    NEW_ID = 54545
    df.DonorID = np.where(df.DonorID == OLD_ID, NEW_ID, df.DonorID)
    print('after fixing legacy donor')

    # fix appeal category nulls
    df['AppealCategory'] = df['AppealCategory'].fillna(value='Blank')
    print('after fixing appeal category')

    # One-off fix for H0222 Package B
    # added 02/20/2024 per MRT task in Asana called
    # # 'HKI Campaign Performance Pivot Not Fully Aligning with Transaction File'
    mask = (df.AppealID.isin(['H0222NS'])) & (df.PackageID.str.endswith('B'))
    df.PackageID = np.where(mask, df.PackageID.str[:-1] + 'N', df.PackageID)
    print('completed mapping b to n')

    # remove whitespace from code columns
    cols = ['AppealID', 'PackageID']
    for col in cols:
        # commented out 02/21/2024 per MRT note
        df[col] = df[col].str.replace('NS', '')  # added 6/26/23 per MRT note
        df[col] = df[col].str.upper().str.replace(' ', '').fillna(value='')
    print('completed appeal and package')

    # identify applicable appeal ids
    pattern = '^[A-Z][0-9]{4}[A-Z]{0,2}$'
    c1 = check_col(df['AppealID'], pattern)
    pattern = '^[W][A-Z0-9]{4,7}$'  # added 6/26/23 per MRT note
    c2 = check_col(df['AppealID'], pattern)  # added 6/26/23 per MRT note
    cond1 = (c1) | (c2)
    df['_aid'] = np.where(cond1, df['AppealID'], '')
    print('after regex')

    # create source codes from appeal id and package id
    cond2 = df.apply(lambda x: x['PackageID'].startswith(x['_aid']), axis=1)
    df['SourceCode'] = np.where(cond2, df['PackageID'], df['_aid'] + df['PackageID'])
    print('after lambda')

    # overwrite bad source codes
    df.SourceCode = np.where(cond1, df.SourceCode, '')
    print('create source codes')

    df = df.drop('_aid', axis=1)
    print('Drop _aid')

    # New preprocessing steps
    df['DonorID'] = df['DonorID'].apply(lambda x: str(int(x)) if pd.notnull(x) else np.nan)
    df['Soft Credit Constituent ID'] = df['Soft Credit Constituent ID'].apply(lambda x: str(int(x)) if pd.notnull(x) else np.nan)
    print("Rows where Soft Credit Constituent ID is not null before applying transformation:")
    print(df[df['Soft Credit Constituent ID'].notnull()][['DonorID', 'Soft Credit Constituent ID']].head())

    # HKI-specific transformation for DonorID
    df['DonorID'] = df.apply(
        lambda row: row['Soft Credit Constituent ID'] if pd.notnull(row['Soft Credit Constituent ID']) else row['DonorID'],
        axis=1
    )
    print("Rows where Soft Credit Constituent ID is not null after applying transformation:")
    print(df[df['Soft Credit Constituent ID'].notnull()][['DonorID', 'Soft Credit Constituent ID']].head())

    # HKI-specific transformation for GiftAmount
    df['GiftAmount'] = df['GiftAmount'].replace({'\$': '', ',': ''}, regex=True).astype(float)
    print("After cleaning GiftAmount:")
    print(df['GiftAmount'].head())

    #Columns to convert to string
    columns_to_check = ['Luminate Online Gift ID', 'GiftID', 'GiftIDDescription']
    for column in columns_to_check:
        if column in df.columns:
            # Convert to string and remove '.0' only if it is at the end
            df[column] = df[column].astype(str).replace(r'\.0$', '', regex=True)
            print(f"{column} is now a string and '.0' has been stripped where applicable")
        else:
            print(f"{column} column not found in the dataframe")

    #Fill empty SourceCodes with value from PackageID if available
    df['SourceCode'] = df['SourceCode'].where(df['SourceCode'].notna() & (df['SourceCode'] != ''), df['PackageID'])
    print("Empty SourceCodes filled with PackageID when available")

    print("Client Specific for HKI Completed")
    return df


# COMMAND ----------

# MAGIC %md #MC

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def MC(df):
  
  def _numeric_donor(df, col='DonorID'):
    df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=[col])
    df[col] = df[col].astype(str).str.replace('\.0','')
    return df
  
  # cast donor ids to numeric
  df = _numeric_donor(df)

  # drop PromoSegment
  cols = [x for x in df.columns if x != 'PromoSegment']
  df = df[cols]
  
  # split values on semi-colons
  cols = ['CampaignID', 'CampaignCode']
  for col in cols:
    print('col: ', col)
    df[col] = [str(x).split(';')[0] for x in df[col]]

  # read promo files
  dfs = []
  path = os.path.join(filemap.RAW, 'PromoFiles')
  for promo in os.listdir(path):
    _df = pd.read_csv(os.path.join(path, promo), encoding='ISO-8859-1')
    print('promo shape: ', _df.shape)
    print('promo columns: ', _df.columns)
    dfs.append(_df)
  dfp = pd.concat(dfs).drop_duplicates()
  print('dfp shape: ', dfp.shape)
  print('dfp columns: ', dfp.columns)  
  
  print('before renaming promo df')
  # map headers
  pm = {
    'Constituent ID': 'DonorID',
    'Assigned Appeal ID': 'CampaignCode',
    'Assigned Appeal Marketing Segment': 'PromoSegment',
    'Assigned Package ID': '_PackageID',
    'Assigned Appeal Marketing Source Code': '_SourceCode'
  }
  dfp = dfp.rename(columns=pm)

  print('start new section for cold names')
  # added 6/15/2023 to address the change where source codes for cold names 
  # need the segment code from the promo history 
  # align the column names
  d = {'Marketing Finder Number': 'Finder_Number'}
  _dfp = dfp.rename(columns=d)
  _dfp = _numeric_donor(_dfp)
  print('finished renaming section for cold names')

  # merge in the promo segment
  col = 'Finder_Number'
  _dfp = _dfp.dropna(subset=[col])
  print('df dtypes: ', df.dtypes)
  print('_dfp dtypes: ', _dfp.dtypes)
  cols = ['DonorID', 'Finder_Number', 'PromoSegment']
  df = pd.merge(df, _dfp[cols], on=cols[:2], how='left')
  del _dfp
  print('finished new merge')

  # fill null segment codes
  df.SegmentCode = df.SegmentCode.fillna(df.PromoSegment)
  df = df.drop('PromoSegment', axis=1)
  print('finished new section for cold names')

  print('before casting mail date')
  # cast mail date to datetime
  date_col = 'Assigned Appeal Date'
  dfp[date_col] = pd.to_datetime(dfp[date_col])

  print('before filtering campaign code')
  # all appeals
  m1 = (dfp.CampaignCode.str.startswith(('AG', 'AH', 'AN', 'AW')).fillna(value=False))
  # acquisitions starting with March 2023
  m2 = ((dfp.CampaignCode.str.startswith('QQ')) & (dfp[date_col] >= datetime(2023,3,1)))
  mask = m1 | m2
  dfp = dfp.loc[mask]

  # cast donor ids to numeric
  print('before casting promo donor ids')
  dfp = _numeric_donor(dfp)

  print('before sort and drop')
  # take only the most recent donor - campaign combo
  cols = ['DonorID', 'CampaignCode']
  dfp = dfp.sort_values(date_col).drop_duplicates(cols, keep='last')

  print('before merging back to tx')
  # merge back to the transaction table
  cols = list(pm.values())
  print('cols: ', cols)
  df = pd.merge(df, dfp[cols], on=cols[:2], how='left')
  
  print('before segment codes')
  # fill segment codes
  df.SegmentCode = df.SegmentCode.replace('(No Segment Code)', None)
  df.SegmentCode = np.where(
    df.SegmentCode.notna(), 
    df.SegmentCode.str.rjust(4,'L'), 
    df.PromoSegment.str.rjust(4,'L')
  )

  print('before package codes')
  # fill package codes
  df.PackageID = np.where(
    (df.CampaignCode.str.startswith('AG')) & (df.PackageID.isna()),
    df._PackageID,
    df.PackageID  
  )
  df = df.drop('_PackageID', axis=1)

  print('before source codes')
  try:
    # fill source codes
    df['SourceCode'] = np.where(
      (df.SourceCode.isna()) & (~df.CampaignCode.str.startswith('QQ')),
      df.CampaignCode + df.SegmentCode.str.rjust(4,'L') + df.PackageID.str.ljust(2,'X'),
      df.SourceCode
    )
  except Exception as e:
    print('first sc exception: ', e)

  try:
    print(sorted(df.columns))
    df['SourceCode'] = np.where(
      (df.SourceCode.isna()) & (df.CampaignCode.str.startswith('QQ')),
      df._SourceCode, df.SourceCode
    )
    df = df.drop('_SourceCode', axis=1)
  except Exception as e:
    print('second sc exception: ', e)
  
  print('before campaign codes codes')
  # fill campaign codes
  df.CampaignCode = np.where(
    df.CampaignCode.isna(), 
    df.SourceCode.str[:8], 
    df.CampaignCode
  )
  
  print('before replace z and l')
  cols = ['SourceCode', 'SegmentCode']
  for col in cols:
    df[col] = df[col].str.replace('ZZZZ', 'LLLL')
  
  print('finished MC specific')
  return df


#FH Filter
def MC_AppealSubCategory(df, feature='MC_AppealSubCategory'):
  df[feature] = df['AppealSubCategory']
  return df

#FH Filter
def MC_CampaignID(df, feature='MC_CampaignID'):
  df[feature] = df.CampaignID
  return df

#FH Filter
def MC_JoinCampaignID(df, feature='MC_JoinCampaignID'): 
  _df = df.sort_values(['DonorID', 'GiftDate']).drop_duplicates('DonorID', keep='first')
  _df[feature] = _df['MC_CampaignID']
  
  cols = ['DonorID', feature]
  df = pd.merge(df, _df[cols], on='DonorID', how='left')
  
  return df

#FH Filter
def MC_JoinFuseDM(df, feature='MC_JoinFuseDM'): 
  _df = df.sort_values(['DonorID', 'GiftDate']).drop_duplicates('DonorID', keep='first')
  _df[feature] = _df['MC_FuseDM']
  
  cols = ['DonorID', feature]
  df = pd.merge(df, _df[cols], on='DonorID', how='left')
  
  return df

#FH Filter
def MC_Donor984126(df, feature='MC_Donor984126'):
  df[feature] = df.DonorID.astype(str) == '984126'
  return df

#FH Filter
def MC_FuseDM(df, feature='MC_FuseDM'):
  ids = ['143', '145', '146']
  m1 = df.CampaignID.astype(str).isin(ids)
  m2 = (df.CampaignID.astype(str) == '160') & (df.AppealCategory == 'Direct Mail')
  df[feature] = m1 | m2
  return df

#FH Filter
def MC_MidLevelDonor(df, feature='MC_MidLevelDonor'):
  _filemap = Filemap('MC')
  _df = pd.read_csv(os.path.join(_filemap.RAW, 'MidLevelDonors', 'ML Pool.csv'))

  donors = _df['CnBio_ID'].dropna().astype('str').str.replace(' ','').unique()
  df[feature] = df.DonorID.dropna().astype('str').str.replace(' ','').isin(donors)
  
  return df

#FH Filter
def MC_UkraineDonor(df, feature='MC_UkraineDonor', donor='DonorID'):
  df = isUkraine(df, 'UkraineGift')
  _df = df.groupby(donor)['UkraineGift'].sum().to_frame(name='NumUkraineGifts').reset_index()
  _df[feature] = _df['NumUkraineGifts'] > 0
  cols = [donor, feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')

#FH Filter
def MC_UkraineJoin(df, feature='MC_UkraineJoin', donor_col='DonorID', gift_col='GiftDate'):
  _df = df.sort_values([donor_col, gift_col]).drop_duplicates(donor_col)
  _df = isUkraine(_df, feature)
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')

# COMMAND ----------

# MAGIC %md #NJH

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def NJH(df):
  cols = ["Gf_Appeal", "Gf_Package"]
  for c in cols:
    df[c] = df[c].fillna(value = "")
  df['SourceCode'] = df.Gf_Appeal + df.Gf_Package

  return df

#FH Filter
def NJH_DM(df, feature = 'NJH_DM', campaign_col='Gf_Campaign', na_value='Other', program_col='Program', split_value=' FY', dm_string='Direct', dm_value='DM', other_string='Annual', other_value='AF'):
  
  df[campaign_col] = df[campaign_col].fillna(value=na_value)
  df[program_col] = [x[0] for x in df[campaign_col].str.split(split_value)]

  df[feature] = na_value

  condition = df[program_col].str.contains(dm_string).fillna(value=False)
  df[feature] = np.where(condition, dm_value, df[feature])

  condition = df[program_col].str.contains(other_string).fillna(value=False)
  df[feature] = np.where(condition, other_value, df[feature])
  
  return df

#FH Filter
def NJH_GiftChannel(df, feature='NJH_GiftChannel', program_col='NJH_Program', type_col='NJH_ProgramType'):
  df[feature] = 'Other'
  # condition1 = (df[program_col] == 'Direct Mail')
  condition1 = (df[program_col].str.contains('Direct'))

  kws = ['Digital']
  condition = condition1 & (df[type_col].isin(kws))
  df[feature] = np.where(condition, 'Web', df[feature])

  kws = ['Renewal', 'Lapsed', 'IC', 'Guarantor', 'Patient', 'Acquisition']
  condition = condition1 & (df[type_col].isin(kws))
  df[feature] = np.where(condition, 'DM', df[feature])
  
  return df

#FH Filter
def NJH_JoinChannel(df, feature='NJH_JoinChannel', donor_col='DonorID', gift_col='GiftDate', ref_col='NJH_GiftChannel'):
  _df = df.sort_values([donor_col, gift_col]).drop_duplicates(donor_col)
  _df[feature] = _df[ref_col]

  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=donor_col, how='left')

#FH Filter
def NJH_Patient(df, feature = 'NJH_Patient', appeal_col='Gf_Appeal', appeal_len=3, first_char='P', true_label='True', false_label='False'):
  
  df['lens_agree'] = df[appeal_col].str.len() == appeal_len
  df['starts_with_char'] = df[appeal_col].str.startswith(first_char) 

  condition = (df['lens_agree']) & (df['starts_with_char'])
  df[feature] = np.where(condition, true_label, false_label)
  
  return df.drop(['lens_agree', 'starts_with_char'], axis=1)

#FH Filter
def NJH_Program(df, feature='NJH_Program', ref_col='Gf_Campaign', delim=' FY'):
  df[feature] = df[ref_col].str.split(delim).str[0]
  # df[feature] = df['Program']
  return df

#FH Filter
def NJH_ProgramType(df, feature='NJH_ProgramType', ref_col='Gf_Appeal'):
  df[ref_col] = df[ref_col].str.upper()
  df[feature] = None  

  keys = [
    'D' , 'G', 'P', 'H', 'X', 'DM', 'DW', 
    'ORF', 'WEB UNSOLICITED', 'R', 'REC'
  ]
  values = [
    'Acquisition', 'Guarantor', 'Patient', 
    'IC', 'Lapsed', 'Digital', 'Digital', 
    'Digital', 'Digital', 'Renewal', 'Recurring'
  ]
  
  for i in range(len(keys)):
    condition = df[ref_col].str.startswith(keys[i])
    df[feature] = np.where(condition, values[i], df[feature])
    
  return df





# COMMAND ----------

# MAGIC %md #RADY

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def RADY(df):
    print("RADY client-specific columns processing started")

    # Convert 'Reference' to string, replacing missing values with 'none'
    if 'Reference' in df.columns:
        df['Reference'] = df['Reference'].astype(str).fillna('none')
        print('Reference converted to string if exists')

    # Convert 'ZipCode' to string
    if 'ZipCode' in df.columns:
        df['ZipCode'] = df['ZipCode'].astype(str)
        print('ZipCode converted to string if exists')

    #For acquisition campaigns if PackageCode is blank then update with "CampaignName" (formerly "Reference")
    if 'CampaignCode' in df.columns and 'PackageCode' in df.columns:
      mask = (
          df['CampaignCode'].str.startswith('DMA', na=False) &
          (df['PackageCode'].isna() | (df['PackageCode'].str.strip() == '')))
      
      df.loc[mask, 'PackageCode'] = df.loc[mask, 'CampaignName']

    # Update SourceCode based on CampaignCode and PackageCode
    if 'CampaignCode' in df.columns and 'PackageCode' in df.columns:
        df['SourceCode'] = df.apply(
            lambda row: f"{row['CampaignCode']}-{row['PackageCode']}" 
            if str(row['CampaignCode']).startswith(('DM', 'DR')) and pd.notna(row['PackageCode']) and row['PackageCode'] != ''
            else row['CampaignCode'] if str(row['CampaignCode']).startswith(('DM', 'DR'))
            else '', axis=1)
        print("SourceCode updated to CampaignCode-PackageCode if CampaignCode starts with 'DM' or 'DR' and PackageCode exists, otherwise just CampaignCode or blank.")

    # Update AppealID blanks with values from CampaignCode
    if 'AppealID' in df.columns and 'CampaignCode' in df.columns:
        df['AppealID'] = df['AppealID'].fillna(df['CampaignCode'])
        print("Blank AppealIDs updated with CampaignCode value")

    # Drop rows with NaNs in critical columns
    cols = ['GiftDate', 'GiftAmount', 'DonorID']
    df = df.dropna(subset=[col for col in cols if col in df.columns])
    print("N/As removed from GiftDate, GiftAmount, DonorID if present")

    print("RADY client-specific columns successfully applied")
    return df

#FH Filter
def RADY_Channel(df, feature='RADY_Channel'): 
  '''
  Direct mail = any source code beginning with DM
  e-phil = digital
  DR = would also be lumped as Direct Mail
  EV = Giveathon/ CMN and Rady
  '''
  df[feature] = 'Other'

  mask = ((df.CampaignCode.str.startswith('DM')) | (df.CampaignCode.str.startswith('DR'))).fillna(value=False)
  df.loc[mask, feature] = 'Direct Mail'

  mask = (df.CampaignCode.str.startswith('EPHIL')).fillna(value=False)
  df.loc[mask, feature] = 'Digital'

  mask = (df.CampaignCode.str.startswith('EV')).fillna(value=False)
  df.loc[mask, feature] = 'Giveathon/ CMN and Rady'

  return df

#FH Filter
def RADY_ChannelBehavior(df, feature='RADY_ChannelBehavior'): 
  _df = df.groupby('DonorID')['RADY_Channel'].nunique().to_frame(name='NumChannels').reset_index()
  _df[feature] = np.where(_df.NumChannels > 1, 'MultiChannel', 'SingleChannel')
  cols = ['DonorID', feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')

#FH Filter
def RADY_GiftSubtype(df, feature='RADY_GiftSubtype'): 
  df[feature] = df['Gift Subtype']
  return df

#FH Filter
def RADY_GiftType(df, feature='RADY_GiftType'): 
  df[feature] = df['Gift Type']
  return df

#FH Filter
def RADY_JoinChannel( 
  df, feature='RADY_JoinChannel', donor_col='DonorID',  
  gift_col='GiftDate', ref_col='RADY_Channel'
  ):
  _df = df.sort_values([donor_col, gift_col]).drop_duplicates(donor_col)
  _df[feature] = _df[ref_col]
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=donor_col, how='left')

#FH Filter
def RADY_Online(df, feature='RADY_Online'):
  df[feature] = df['Online']
  return df

#FH Filter
def RADY_PaymentMethod(df, feature='RADY_PaymentMethod'):
  df[feature] = df['Payment Method']
  return df

#FH Filter
def RADY_Sustainer(df, feature='RADY_Sustainer'):
  mask = df.RADY_GiftType.fillna(value='').str.lower().str.contains('recur')
  donors = df.loc[mask, 'DonorID'].unique()
  mask = df.DonorID.isin(donors)
  df[feature] = np.where(mask, 'Sustainer', 'OneTime')
  return df



# COMMAND ----------

# MAGIC %md #TCI

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def TCI(df):
  cols = [x for x in df.columns if 'unnamed' in x.lower()]
  df = df.drop(cols, axis=1)
  
  df.GiftID = df.GiftID.astype(str)
  # drop duplicate Gift IDs after July 1, 2019
  df['GiftDate'] = pd.to_datetime(df['GiftDate'])
  mask = df['GiftDate'] >= '2019-07-01'

  cols = ['DonorID', 'GiftAmount', 'GiftDate']
  _df = df[mask].drop_duplicates(cols, keep='last')
  df = df[~mask].append(_df)

  # read donor mapping file, needed due to client's database migration
  print('inside tci donor mapping code')
  ref = pd.read_excel(os.path.join(filemap.RAW, 'DonorID_Mapping', 'TCI All Records SF and RE ID.xlsx'))
  mask = ref['RE Constituent ID'].notna()
  ref = ref.loc[mask]
  print('read reference file - shape: ', ref.shape)
  cols = ['RE Constituent ID', 'SF Account ID']
  ref[cols] = ref[cols].astype(str)
  donor_map = dict(zip(ref['RE Constituent ID'], ref['SF Account ID']))
  print('created donor id map')
  df.DonorID = df.DonorID.astype(str)
  df['TempDonorID'] = df.DonorID.map(donor_map)
  df.DonorID = np.where(df.TempDonorID.isna(), df.DonorID, df.TempDonorID)
  df = df.drop('TempDonorID', axis=1)
  print('num null donor ids: ', df.DonorID.isna().sum())
  print('df.shape: ', df.shape)
  print('completed mapping - exiting')

  #Align SourceCode column to package
  df['SourceCode'] = df['Package']
  print('SourceCode column built based on Package')

  print('TCI client specific complete')
  return df


#FH Filter
def TCI_AppealList(df, feature='TCI_AppealList', appeal_col='AppealID'):  
  df[feature] = df[appeal_col]
  return df

#FH Filter
def TCI_DM(df, feature='TCI_DM'):
  if 'GiftSource' not in df.columns:
    df['TCI_DM'] = None
    return df

  # updated for revenue beginning 1/1/2021
  dm_names = ['Merkle', 'Engage']
  dm_whitemail_names = ['White Mail']
  digital_names = ['Classy', 'Facebook', 'Internet']
  
  df.GiftSource = df.GiftSource.fillna(value='')

  df[feature] = 'Other'

  mask = df.GiftSource.isin(dm_names)
  df.loc[mask, feature] = 'DM'

  mask = df.GiftSource.isin(dm_whitemail_names)
  df.loc[mask, feature] = 'DM Whitemail'

  mask = df.GiftSource.isin(digital_names)
  df.loc[mask, feature] = 'Digital'
  
  return df

#FH Filter
def TCI_JoinSource(df, feature='TCI_JoinSource', donor_col='DonorID', gift_col='GiftDate', ref_col='TCI_DM'):
  _df = df.sort_values([donor_col, gift_col]).drop_duplicates(donor_col)
  _df[feature] = _df[ref_col]
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=donor_col, how='left')



# COMMAND ----------

# MAGIC %md #TLF

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def TLF(df): #Changes made on 9/13/24
  print('inside TLF client specific')
  print(df.columns)
  #Update Source Codes
  if 'GiftAmount' in df.columns:
    #   #d = {'Segment Code': 'SourceCode'}
    #   #df = df.rename(columns=d)
    # df['SourceCode'] = df['Segment Code']
    # df.SourceCode = df.SourceCode.astype(str).str.upper()
    # df['CampaignCode'] = df.SourceCode.astype(str).fillna(value='').str[:6]

    # #Update to string types
    # #df['Segment Code'] = df['Segment Code'].astype(str)
    # df['SourceCode'] = df['SourceCode'].astype(str)
    df['Segment Code'] = df['Segment Code'].astype(str)
    df['DAF'] = df['DAF'].astype(str)
    df['IRA'] = df['IRA'].astype(str)

    #Convert GiftID to a string and remove .0 from end
    df['GiftID'] = df['GiftID'].apply(lambda x: str(int(x)) if pd.notnull(x) else '')
    

    if 'Captured Scanline' in df.columns and 'Segment Code' in df.columns:
        # Ensure 'Captured Scanline' is treated as a string
        df['Captured Scanline'] = df['Captured Scanline'].astype(str)
        
        # Extract the 10-digit substring from Captured Scanline
        extracted_scanline = df['Captured Scanline'].str[11:21]
        
        # Use np.where to apply the new condition
        df['SourceCode'] = np.where(
            (df['Captured Scanline'].notna()) &  # Condition 1: Not NaN
            (df['Captured Scanline'].str.strip() != '') &  # Condition 2: Not empty string
            (extracted_scanline.str.len() == 10),  # Condition 3: Exactly 10 characters
            extracted_scanline,  # Use the 10-digit substring if valid
            df['Segment Code']  # Use Segment Code otherwise
        )
        print("SourceCode column added from Scanline if valid; otherwise, Segment Code is used.")
    else:
        print("Required columns ('Captured Scanline' and 'Segment Code') are missing.")

    #Make sure SourceCode is a string and uppercase
    df.SourceCode = df.SourceCode.astype(str).str.upper()
    #Add campaign code
    df['CampaignCode'] = df.SourceCode.astype(str).fillna(value='').str[:6]
    print("CampaignCode column update with SourceCode values")
    
    print('Completed TLF Client Specific')
  return df

#FH Filter
def TLF_CampaignName(df, feature='TLF_CampaignName', ref_col='Campaign Name'):
  df[feature] = df[ref_col]
  return df

#FH Filter
def TLF_DAF(df, feature='TLF_DAF', ref_col='DAF'):
  df[feature] = df[ref_col]
  return df

#FH Filter
def TLF_IRA(df, feature='TLF_IRA', ref_col='IRA'):
  df[feature] = df[ref_col]
  return df

#FH Filter
def TLF_JoinCampaignName(df, feature='TLF_JoinCampaignName'):  
  df = df.sort_values('GiftDate')
  mask = df['GiftDate'] == df['FirstGiftDate']
  _df = df[mask].drop_duplicates('DonorID')
  _df[feature] = _df['Campaign Name']

  cols = ['DonorID', feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')

#FH Filter
def TLF_JoinReportChannel(df, feature='TLF_JoinReportChannel'):  
  df = df.sort_values('GiftDate')
  mask = df['GiftDate'] == df['FirstGiftDate']
  _df = df[mask].drop_duplicates('DonorID')
  _df[feature] = _df['TLF_ReportChannel']

  cols = ['DonorID', feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')
#rouce

#FH Filter
def TLF_JoinSource(df, feature='TLF_JoinSource'):  
  df = df.sort_values('GiftDate')
  mask = df['GiftDate'] == df['FirstGiftDate']
  _df = df[mask].drop_duplicates('DonorID')
  _df[feature] = _df['TLF_Source']

  cols = ['DonorID', feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')

#FH Filter
def TLF_ReportChannel(df, feature='TLF_ReportChannel'):
  df[feature] = 'Other'
  
  mask = (df['UTM Medium'].isin(['email'])) & (df.Channel.isin(['Web']))
  df[feature] = np.where(mask, 'Email', df[feature])
  
  mask = (df['UTM Medium'].isin(['dm', 'redir'])) & (df.Channel.isin(['Web']))
  df[feature] = np.where(mask, 'Direct Mail', df[feature])
  
  mask = df.Channel.isin(['Direct Mail', 'Email'])
  df[feature] = np.where(mask, df.Channel, df[feature])
  return df

#FH Filter
def TLF_Source(df, feature='TLF_Source', ref_col='Channel'):
  df[feature] = df[ref_col]
  mask = df['Segment Code'].str.lower().str.contains('honor|memorial') 
  df.loc[mask, feature] = 'Honor/Memorial'
  return df

# def TLF_update_codes(df):
#   if 'GiftAmount' in df.columns:
#     d = {'Segment Code': 'SourceCode'}
#     df = df.rename(columns=d)
#     df.SourceCode = df.SourceCode.astype(str).str.upper()
#     df['CampaignCode'] = df.SourceCode.astype(str).fillna(value='').str[:6]

#     d = {'ï»¿"Gift Id"': 'TempGiftID'}
#     df = df.rename(columns=d)
#     df.GiftID = np.where(df.GiftID.isna(), df.TempGiftID, df.GiftID)
  
#   return df




# COMMAND ----------

# MAGIC %md #TSE

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def TSE(df):
  df = df.dropna(subset=['AppealID', 'GiftAmount'])
  
  df = df.fillna(value=np.nan)
  
  # for c in parquet_cols:
  #   df[c] = df[c].fillna(value='None').astype(str)
  
  df['CampaignCode'] = df.SourceCode.str[:4]
  print('completed TSE client specific')
  return df

#FH Filter
def TSE_Channel(df, feature='TSE_Channel', ref_col='TSE_Program', dm_kw='DM', online_kw='Online'):
  df[feature] = 'Other'

  condition = df[ref_col].notna()
  df[feature] = np.where(condition, dm_kw, df[feature])

  condition = df[ref_col] == online_kw
  df[feature] = np.where(condition, online_kw, df[feature])
  
  return df

#FH Filter
def TSE_JoinChannel(df, feature='TSE_JoinChannel', donor_col='DonorID', date_col='GiftDate', ref_col='TSE_Channel'):
  _df = df.sort_values([donor_col, date_col]).drop_duplicates(donor_col)
  _df[feature] = _df[ref_col]
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')

#FH Filter
def TSE_Program(df, feature='TSE_Program'):
  null_cols = schema['ClientValues']['NullColumns']['Values']
  char_map = schema['ClientValues']['CharMap']['Values']  
  
  df = make_feature_TSE_Program(df, feature, null_cols, char_map)
  print('finished applying TSE Program')
  return df.dropna(subset=[feature]) 

#FH Filter
def TSE_RecurringGift(df, feature='TSE_RecurringGift', gift_col='GiftType', kw='recur', sust='Sustainer', one_time='1x'):
  condition = df[gift_col].str.lower().str.contains(kw)
  df[feature] = np.where(condition, sust, one_time)
  return df

#FH Filter
def TSE_Sustainer(df, feature='TSE_Sustainer', gift_col='TSE_RecurringGift', kw='Sustainer', donor_col='DonorID'):
  mask = df[gift_col] == kw
  sustainers = df[mask][donor_col].unique()
  condition = df[donor_col].isin(sustainers)
  df[feature] = np.where(condition, True, False)
  return df




# COMMAND ----------

# MAGIC %md # USO

# COMMAND ----------

def collect_transaction_file_USO(
    folder,
    filename,  # <- new parameter: specify single file name
    columns,
    encoding,
    key_mapper,
    date_format,
    directory='Data'
):
    print('collect_transaction_file started')
    
    path = os.path.join(folder, directory, filename)  # Full path to the file
    
    try:
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(path, encoding=encoding)
        elif filename.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(path)
        else:
            raise ValueError(f"Unsupported file format: {filename}")
        
        df.columns = df.columns.str.strip()  # Strip leading/trailing whitespace
        df = df.rename(columns=key_mapper)

        if columns:
            df = df[[col for col in columns if col in df.columns]]  # filter columns safely

        df['Mail Date'] = pd.to_datetime(df['Mail Date'], format=date_format, errors='coerce')

        if 'ï»¿"Gift Id"' in df.columns:
            df.rename(columns={'ï»¿"Gift Id"': 'GiftID'}, inplace=True)
            print("""Renamed ï»¿"Gift Id" to GiftID from transaction file load in""")

        total_row_count = len(df)
        print(f"Total number of rows in the DataFrame: {total_row_count}")
    
    
    except Exception as e:
        print(f"Error processing {filename}: {str(e)}")
        df = pd.DataFrame()  # return empty DataFrame on error
    
    print("collect_transaction_file finished")
    return df

# COMMAND ----------

# MAGIC %md #U4U

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def U4U(df):
  df['SourceCode'] = df['Package ID']
  print("SourceCode column created to align with Package ID")

  df['CampaignCode'] = ''
  print('Empty CampaignCode column created')
  return df

#FH Filter
def U4U_CampaignID(df, feature='U4U_CampaignID'):
  df[feature] = df['Campaign ID']
  print('U4U_CampaignID column successfully created')
  return df

#FH Filter
def U4U_DirectMail(df, feature='U4U_DirectMail'):
  # match appeal ids
  _filemap = Filemap('U4U')
  fn = 'U4U Appeal ID Match.xlsx'
  appeal_ids = pd.read_excel(os.path.join(_filemap.RAW, 'AppealIDs', fn))['Appeal ID']
  m1 = (df['Appeal ID'].isin(appeal_ids))

  # match campaign ids
  kws = ['Direct Mail', 'MailAcquisition', 'MailRetention']
  m2 =  (df['Campaign ID'].isin(kws))

  # match "Appeal ID" containing "mail" but not "email"
  m3 = df['Appeal ID'].str.contains(r'mail', case=False, na=False) & ~df['Appeal ID'].str.contains(r'email', case=False, na=False)

  df[feature] = m1 | m2 | m3
  print('U4U_DirectMail column successfully created')
  return df

#FH Filter
def U4U_JoinCampaignID(df, feature='U4U_JoinCampaignID'): 
  _df = df.sort_values(['DonorID', 'GiftDate']).drop_duplicates('DonorID', keep='first')
  _df[feature] = _df['U4U_CampaignID']
  
  cols = ['DonorID', feature]
  df = pd.merge(df, _df[cols], on='DonorID', how='left')
  print('U4U_JoinCampaignID column successfully created')
  return df

#FH Filter
def U4U_JoinDirectMail(df, feature='U4U_JoinDirectMail'): 
  _df = df.sort_values(['DonorID', 'GiftDate']).drop_duplicates('DonorID', keep='first')
  _df[feature] = _df['U4U_DirectMail']
  
  cols = ['DonorID', feature]
  df = pd.merge(df, _df[cols], on='DonorID', how='left')
  print('U4U_JoinDirectMail column successfully created')
  return df

# COMMAND ----------

# MAGIC %md #VOA

# COMMAND ----------

#FH Filter
def VOA_DirectMail(df, feature='VOA_DirectMail'):
  df[feature] = 'Other'
  mask = df['Fund'] == 'DIRECTMAIL'
  df.loc[mask, feature] = 'Direct Mail'
  return df

#FH Filter
def VOA_JoinDirectMail(df, feature='VOA_JoinDirectMail'):  
  ''' TODO '''
  _df = df.sort_values(['DonorID', 'GiftDate']).drop_duplicates('DonorID', keep='first')
  _df[feature] = _df['VOA_DirectMail']
  
  cols = ['DonorID', feature]
  df = pd.merge(df, _df[cols], on='DonorID', how='left')
  
  return df



# COMMAND ----------

# MAGIC %md #WFP

# COMMAND ----------

#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def WFP(df):
  df.SourceCode = df.SourceCode.str.strip().str.replace('-','')
  mask = df.GiftAmount.notna()
  return df.loc[mask]

#FH Filter
def WFP_DisasterGift(df, feature='WFP_DisasterGift', ref_col='Campaign Name'):
  
  df[feature] = 'Other'
  
  kws = [
    '1900 WFP.org Redirect', 'WFP Redirect', 'Whitemail', 'Individual Giving Offline', 'Individual Philanthropy Offline',
    'Ads Other', 'Email', 'Facebook Ads', 'Organic Social', 'Referral', 'Search Ads', 'Urgentgram', 'Web'
  ]
  # Nobel Peace Prize
  c1 = ((df['GiftDate']>='2019-10-09') & (df['GiftDate']<='2019-12-31'))
  c2 = (df[ref_col].isin(kws))
  df[feature] = np.where(c1&c2, 'NPP', df[feature])
  
  # Pandemic
  c1 = ((df['GiftDate']>='2020-03-15') & (df['GiftDate']<='2020-12-31'))
  c2 = (df[ref_col].isin(kws))
  df[feature] = np.where(c1&c2, 'Pandemic', df[feature])
  
  # Madagascar
  _ = kws.pop(kws.index('Urgentgram'))
  _kws = kws + ['2100 Mini Campaigns', '2100 You Tube Ads', '2000 FB Ads', '2111 Google Ads']
  
  c1 = ((df['GiftDate']>='2021-11-01') & (df['GiftDate']<='2021-12-31'))
  c2 = (df[ref_col].isin(_kws))
  
  mad_kws = [
    'ABC Madagascar Disaster Web', 'Madagascar Disaster Organic Social', 
    'Madagascar Unsourced Giving Offline', 'Madagascar YouTube Ads'
  ]
  c3 = df[ref_col].isin(mad_kws)
  df[feature] = np.where((c1&c2)|(c3), 'Madagascar', df[feature])
  
  # Ukraine
  kws = [
    '1900 WFP.org Redirect', 'WFP Redirect', 
    'Whitemail', 'Individual Giving Offline'
  ]
  c1 = ((df['GiftDate']>='2022-02-28') & (df['GiftDate']<='2022-05-18'))
  c2 = (df[ref_col].isin(kws))
  
  ukr_kws = [
    'Ukraine Conflict - Ads Other',
    'Ukraine Conflict - Email',
    'Ukraine Conflict - Facebook Ads',
    'Ukraine Conflict - Organic Social',
    'Ukraine Conflict - Referral',
    'Ukraine Conflict - Search Ads',
    'Ukraine Conflict - Urgentgram',
    'Ukraine Conflict - Web'
  ]
  c3 = df[ref_col].isin(mad_kws)
  df[feature] = np.where((c1&c2)|(c3), 'Ukraine', df[feature])
  
  return df

#FH Filter
def WFP_DisasterJoin(
  df, feature='WFP_DisasterJoin', ref_col='WFP_DisasterGift', 
  sort_values=['DonorID', 'GiftDate'], dup_value='DonorID'
  ):  
  ''' TODO '''
  _df = df.sort_values(sort_values).drop_duplicates(dup_value, keep='first')
  _df[feature] = _df[ref_col]
  
  cols = [dup_value, feature]
  df = pd.merge(df, _df[cols], on=dup_value, how='left')
  
  return df

#FH Filter
def WFP_DonorChannelGroup(
  df, feature='WFP_DonorChannelGroup', channel_col='WFP_GiftChannel', 
  donor_col='DonorID', multi_label='Multi', dm_label='DM Only', 
  digital_label='Digital Only', other_label='Other Only'
  ): 
  ''' TODO '''
  grouped = df.groupby(donor_col).agg(all=(channel_col, 'unique'))
  grouped = df.groupby(donor_col)[channel_col].unique().to_frame(name='AllChannels').reset_index()

  grouped['AllChannels'] = [','.join(sorted(x)) for x in grouped['AllChannels']]

  grouped[feature] = multi_label

  condition = grouped['AllChannels'] == 'DM'
  grouped[feature] = np.where(condition, dm_label, grouped[feature])

  condition = (grouped['AllChannels'] == 'Digital') | (grouped['AllChannels'] == 'Blank') | (grouped['AllChannels'] == 'Blank,Digital') 
  grouped[feature] = np.where(condition, digital_label, grouped[feature])

  condition = grouped['AllChannels'] == 'Other'
  grouped[feature] = np.where(condition, other_label, grouped[feature])

  cols = [donor_col, feature]
  df = pd.merge(df, grouped[cols], on=donor_col, how='left')
  return df

#FH Filter
def WFP_Donor0014T000004x9wfQAA(
  df, feature='WFP_Donor0014T000004x9wfQAA'
  ):
  df[feature] = df.DonorID.astype(str) == '0014T000004x9wfQAA'
  return df


# def WFP_GiftChannel(
#   df, feature='WFP_GiftChannel', 
#   channel_col='Channel Type'
#   ):
#   df[feature] = 'Other'
  
#   dm_types = ['Direct Mail', 'Individual Philanthropy Offline']
#   df.loc[df[channel_col].isin(dm_types), feature] = 'DM'

#   digital_types = ['Ads - Other', 'Advocacy', 'DRTV', 'Email', 'Facebook Ads', 'Individual Philanthropy Online', 'IP Ads - Other', 'IP Facebook', 'Organic Social', 'Search Ads', 'Share-The-Meal', 'SMS', 'WFP Redirect', 'WFP USA Web', 'Referral']
#   df.loc[df[channel_col].isin(digital_types), feature] = 'Digital'

#   return df


# def WFP_GiftChannelDetail(
#   df, feature='WFP_GiftChannelDetail', channel_col='Channel Type'
#   ):
#   df[feature] = df[channel_col]
#   return df


# def WFP_GiftChannelSubtype(
#   df, feature='WFP_GiftChannelSubtype', channel_col='Channel Type'
#   ):
  
#   df[feature] = 'Other'

#   cols = ['Direct Mail', 'Individual Philanthropy Offline', 'DRTV', 'Email', 'SMS']
#   for col in cols:
#     mask = df[channel_col] == col
#     df.loc[mask, feature] = col
    
#   ad_values = ['Ads - Other', 'Facebook Ads', 'IP Ads - Other', 'IP Facebook']
#   df.loc[df[channel_col].isin(ad_values), feature] = 'Ads'

#   website_values = ['WFP Redirect', 'WFP USA Web', 'Referral']
#   df.loc[df[channel_col].isin(website_values), feature] = 'Website'

#   organic_values = ['Individual Philanthropy Online', 'Organic Social']
#   df.loc[df[channel_col].isin(organic_values), feature] = 'Organic' 

#   search_values = ['Search Ads']
#   df.loc[df[channel_col].isin(search_values), feature] = 'Search'  

#   return df


# def WFP_JoinChannel(
#   df, feature='WFP_JoinChannel', channel_col='WFP_GiftChannel', 
#   sort_values=['DonorID', 'GiftDate'], dup_value='DonorID'
#   ):  
#   ''' TODO '''
#   _df = df.sort_values(sort_values).drop_duplicates(dup_value, keep='first')
#   _df[feature] = _df[channel_col]
  
#   cols = [dup_value, feature]
#   df = pd.merge(df, _df[cols], on=dup_value, how='left')
  
#   return df


# def WFP_JoinChannelDetail(
#   df, feature='WFP_JoinChannelDetail', channel_col='WFP_GiftChannelDetail', 
#   sort_values=['DonorID', 'GiftDate'], dup_value='DonorID'
#   ):  
#   ''' TODO '''
#   _df = df.sort_values(sort_values).drop_duplicates(dup_value, keep='first')
#   _df[feature] = _df[channel_col]
  
#   cols = [dup_value, feature]
#   df = pd.merge(df, _df[cols], on=dup_value, how='left')
  
#   return df


# def WFP_JoinChannelSubtype(
#   df, feature='WFP_JoinChannelSubtype', channel_col='WFP_GiftChannelSubtype', 
#   sort_values=['DonorID', 'GiftDate'], dup_value='DonorID'
#   ):  
#   ''' TODO '''
#   _df = df.sort_values(sort_values).drop_duplicates(dup_value, keep='first')
#   _df[feature] = _df[channel_col]
  
#   cols = [dup_value, feature]
#   df = pd.merge(df, _df[cols], on=dup_value, how='left')
  
#   return df

# Modified functions to accommodate the new client request

# def WFP_GiftChannel(
#     df, feature='WFP_GiftChannel', channel_col='Channel Type'
# ):
#     # Default to 'Other'
#     df[feature] = 'Other'

#     # Define types and update Gift Channel based on Channel Type
#     dm_types = ['Direct Mail', 'Individual Philanthropy Offline', 'Direct Mail Online']
#     df.loc[df[channel_col].isin(dm_types), feature] = 'DM'

#     digital_types = [
#         'Ads - Other', 'Advocacy', 'DRTV', 'Email', 'Facebook Ads',
#         'Individual Philanthropy Online', 'IP Ads - Other', 'IP Facebook',
#         'Organic Social', 'Search Ads', 'Share-The-Meal', 'SMS',
#         'WFP Redirect', 'WFP USA Web', 'Referral', 'IP Search Ads'
#     ]
#     df.loc[df[channel_col].isin(digital_types), feature] = 'Digital'

#     canvassing_types = ['Face-to-Face']
#     df.loc[df[channel_col].isin(canvassing_types), feature] = 'Canvassing'

#     return df

#FH Filter
def WFP_GiftChannelDetail(
    df, feature='WFP_GiftChannelDetail', channel_col='Channel Type'
):
    # Default to 'Other'
    df[feature] = 'Other'

    # Explicitly map "Direct Mail Online"
    df.loc[df[channel_col] == 'Direct Mail Online', feature] = 'Direct Mail Online'

    # For other cases, copy Channel Type values
    df.loc[df[feature] == 'Other', feature] = df[channel_col]

    return df


# def WFP_GiftChannelSubtype(
#     df, feature='WFP_GiftChannelSubtype', channel_col='Channel Type'
# ):
#     # Default to 'Other'
#     df[feature] = 'Other'

#     # Explicitly map "Direct Mail Online"
#     df.loc[df[channel_col] == 'Direct Mail Online', feature] = 'Direct Mail Online'

#     # Map other specific types to Gift Sub-Channel
#     cols = ['Direct Mail', 'Individual Philanthropy Offline', 'DRTV', 'Email', 'SMS']
#     for col in cols:
#         mask = df[channel_col] == col
#         df.loc[mask, feature] = col

#     ad_values = ['Ads - Other', 'Facebook Ads', 'IP Ads - Other', 'IP Facebook']
#     df.loc[df[channel_col].isin(ad_values), feature] = 'Ads'

#     website_values = ['WFP Redirect', 'WFP USA Web', 'Referral']
#     df.loc[df[channel_col].isin(website_values), feature] = 'Website'

#     organic_values = ['Individual Philanthropy Online', 'Organic Social']
#     df.loc[df[channel_col].isin(organic_values), feature] = 'Organic'

#     search_values = ['Search Ads', 'IP Search Ads']
#     df.loc[df[channel_col].isin(search_values), feature] = 'Search'

#     canvassing_values = ['Face-to-Face']
#     df.loc[df[channel_col].isin(canvassing_values), feature] = 'Face-to-Face'

#     return df

#FH Filter
def WFP_GiftChannel(
    df, feature='WFP_GiftChannel', channel_col='Channel Type'
):
    # Default to 'Other'
    df[feature] = 'Other'

    # Explicitly prioritize "Direct Mail Online" first
    df.loc[df[channel_col] == 'Direct Mail - Online', feature] = 'DM'

    # Define types and update Gift Channel based on Channel Type
    dm_types = ['Direct Mail', 'Individual Philanthropy Offline']
    df.loc[df[channel_col].isin(dm_types), feature] = 'DM'

    digital_types = [
        'Ads - Other', 'Advocacy', 'DRTV', 'Email', 'Facebook Ads',
        'Individual Philanthropy Online', 'IP Ads - Other', 'IP Facebook',
        'Organic Social', 'Search Ads', 'Share-The-Meal', 'SMS',
        'WFP Redirect', 'WFP USA Web', 'Referral', 'IP Search Ads'
    ]
    df.loc[df[channel_col].isin(digital_types), feature] = 'Digital'

    canvassing_types = ['Face-to-Face']
    df.loc[df[channel_col].isin(canvassing_types), feature] = 'Canvassing'

    return df

#FH Filter
def WFP_GiftChannelSubtype(
    df, feature='WFP_GiftChannelSubtype', channel_col='Channel Type'
):
    # Default to 'Other'
    df[feature] = 'Other'

    # Explicitly prioritize "Direct Mail Online" first
    df.loc[df[channel_col] == 'Direct Mail - Online', feature] = 'Direct Mail - Online'

    # Map other specific types to Gift Sub-Channel
    cols = ['Direct Mail', 'Individual Philanthropy Offline', 'DRTV', 'Email', 'SMS']
    for col in cols:
        mask = df[channel_col] == col
        df.loc[mask, feature] = col

    ad_values = ['Ads - Other', 'Facebook Ads', 'IP Ads - Other', 'IP Facebook']
    df.loc[df[channel_col].isin(ad_values), feature] = 'Ads'

    website_values = ['WFP Redirect', 'WFP USA Web', 'Referral']
    df.loc[df[channel_col].isin(website_values), feature] = 'Website'

    organic_values = ['Individual Philanthropy Online', 'Organic Social']
    df.loc[df[channel_col].isin(organic_values), feature] = 'Organic'

    search_values = ['Search Ads', 'IP Search Ads']
    df.loc[df[channel_col].isin(search_values), feature] = 'Search'

    canvassing_values = ['Face-to-Face']
    df.loc[df[channel_col].isin(canvassing_values), feature] = 'Face-to-Face'

    return df



# def WFP_GiftChannel(
#     df, feature='WFP_GiftChannel', channel_col='Channel Type'
# ):
#     df[feature] = 'Other'

#     dm_types = ['Direct Mail', 'Individual Philanthropy Offline', 'Direct Mail Online']
#     df.loc[df[channel_col].isin(dm_types), feature] = 'DM'

#     digital_types = [
#         'Ads - Other', 'Advocacy', 'DRTV', 'Email', 'Facebook Ads',
#         'Individual Philanthropy Online', 'IP Ads - Other', 'IP Facebook',
#         'Organic Social', 'Search Ads', 'Share-The-Meal', 'SMS',
#         'WFP Redirect', 'WFP USA Web', 'Referral', 'IP Search Ads'
#     ]
#     df.loc[df[channel_col].isin(digital_types), feature] = 'Digital'

#     canvassing_types = ['Face-to-Face']
#     df.loc[df[channel_col].isin(canvassing_types), feature] = 'Canvassing'

#     return df


# def WFP_GiftChannelDetail(
#     df, feature='WFP_GiftChannelDetail', channel_col='Channel Type'
# ):
#     df[feature] = df[channel_col]
#     return df


# def WFP_GiftChannelSubtype(
#     df, feature='WFP_GiftChannelSubtype', channel_col='Channel Type'
# ):
#     df[feature] = 'Other'

#     cols = ['Direct Mail', 'Individual Philanthropy Offline', 'DRTV', 'Email', 'SMS', 'Direct Mail Online']
#     for col in cols:
#         mask = df[channel_col] == col
#         df.loc[mask, feature] = col

#     ad_values = ['Ads - Other', 'Facebook Ads', 'IP Ads - Other', 'IP Facebook']
#     df.loc[df[channel_col].isin(ad_values), feature] = 'Ads'

#     website_values = ['WFP Redirect', 'WFP USA Web', 'Referral']
#     df.loc[df[channel_col].isin(website_values), feature] = 'Website'

#     organic_values = ['Individual Philanthropy Online', 'Organic Social']
#     df.loc[df[channel_col].isin(organic_values), feature] = 'Organic'

#     search_values = ['Search Ads', 'IP Search Ads']
#     df.loc[df[channel_col].isin(search_values), feature] = 'Search'

#     canvassing_values = ['Face-to-Face']
#     df.loc[df[channel_col].isin(canvassing_values), feature] = 'Face-to-Face'

#     return df

#FH Filter
def WFP_JoinChannel(
    df, feature='WFP_JoinChannel', channel_col='WFP_GiftChannel',
    sort_values=['DonorID', 'GiftDate'], dup_value='DonorID'
):
    _df = df.sort_values(sort_values).drop_duplicates(dup_value, keep='first')
    _df[feature] = _df[channel_col]

    cols = [dup_value, feature]
    df = pd.merge(df, _df[cols], on=dup_value, how='left')

    return df

#FH Filter
def WFP_JoinChannelDetail(
    df, feature='WFP_JoinChannelDetail', channel_col='WFP_GiftChannelDetail',
    sort_values=['DonorID', 'GiftDate'], dup_value='DonorID'
):
    _df = df.sort_values(sort_values).drop_duplicates(dup_value, keep='first')
    _df[feature] = _df[channel_col]

    cols = [dup_value, feature]
    df = pd.merge(df, _df[cols], on=dup_value, how='left')

    return df

#FH Filter
def WFP_JoinChannelSubtype(
    df, feature='WFP_JoinChannelSubtype', channel_col='WFP_GiftChannelSubtype',
    sort_values=['DonorID', 'GiftDate'], dup_value='DonorID'
):
    _df = df.sort_values(sort_values).drop_duplicates(dup_value, keep='first')
    _df[feature] = _df[channel_col]

    cols = [dup_value, feature]
    df = pd.merge(df, _df[cols], on=dup_value, how='left')

    return df


#FH Filter
def WFP_LegacyGiftChannel(
  df, feature='WFP_GiftChannel', channel_col='Channel Type', 
  dm_label='DM', digital_label='Digital', blank_label='Blank', 
  other_label='Other'
  ):  
  ''' TODO '''
  df[feature] = other_label
  
  dm_kws = ['DIRECT MAIL']
  condition = df[channel_col].str.upper().isin(dm_kws)
  df[feature] = np.where(condition, dm_label, df[feature])
  
  digital_kws = ['ADS - OTHER', 'CORPORATE - DIGITAL', 'DRTV', 'FACEBOOK ADS', 'ORGANIC SOCIAL',
                 'P2P', 'SEARCH ADS', 'SHARE-THE-MEAL', 'WFP REDIRECT', 'WFP USA WEB']
  condition = df[channel_col].str.upper().isin(digital_kws)
  df[feature] = np.where(condition, digital_label, df[feature])  
  
  blank_kws = ['NONE']
  condition = df[channel_col].str.upper().isin(blank_kws)
  df[feature] = np.where(condition, blank_label, df[feature])  
  
  return df

#FH Filter
def WFP_RecurringGift(df, feature='WFP_RecurringGift'):
  df[feature] = df['Recurring Gift ID'].notna()
  return df

#FH Filter
def WFP_Sustainer(df, feature='WFP_Sustainer'):
  sustainers = df.loc[df.WFP_RecurringGift, 'DonorID'].unique()
  df[feature] = df.DonorID.isin(sustainers)
  return df

#FH Filter
def WFP_UkraineDonor(df, feature='WFP_UkraineDonor'):
  _df = df.groupby('DonorID').GiftDate.min().to_frame(name='FirstGiftDate').reset_index()
  _df[feature] = (_df.FirstGiftDate >= datetime(2022,2,24)) & (_df.FirstGiftDate <= datetime(2022,5,31))
  cols = ['DonorID', feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')




# COMMAND ----------

# MAGIC %md #Legacy clients / RFPs

# COMMAND ----------

# DBTITLE 1,Legacy Clients / RFPs
# FH features / filters

def AH(df):
  df['SourceCode'] = df.CampaignCode.fillna(value='X'*10) + df.PackageID.fillna(value='Y'*5)
  return df

#FH Filter
def AH_CampaignID(df, feature='AH_CampaignID', ref_col='CampaignAlias'):
  df[feature] = df[ref_col]
  return df

#FH Filter
def AH_ChannelBehavior(
  df, feature='AH_ChannelBehavior', donor_col='DonorID', 
  campaign_col='AH_CampaignID', temp_col='numChannels', 
  multi_label='Multi'
  ):

  grouped = df.groupby(donor_col)[campaign_col].nunique().to_frame(name=temp_col)
  df = pd.merge(df, grouped, on=donor_col, how='left')

  condition = df[temp_col] == 1
  df[feature] = np.where(condition, df[campaign_col], multi_label)
  
  return df

#FH Filter
def AH_FundID(df, feature='AH_FundID', ref_col='FundID'):
  df[feature] = df[ref_col]
  return df

#FH Filter
def AH_JoinCampaignID(
  df, feature='AH_JoinCampaignID', donor_col='DonorID', 
  date_col='GiftDate', first_gift_col='FirstGiftDate', 
  ref_col='CampaignAlias'
  ):
  mask = df[date_col] == df[first_gift_col]
  _df = df[mask].drop_duplicates([donor_col, first_gift_col])
  _df[feature] = _df[ref_col]
  
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=donor_col, how='left')

#FH Filter
def AH_JoinFundID(
  df, feature='AH_JoinFundID', donor_col='DonorID', 
  date_col='GiftDate', first_gift_col='FirstGiftDate', 
  ref_col='FundID'
  ):
  mask = df[date_col] == df[first_gift_col]
  _df = df[mask].drop_duplicates([donor_col, first_gift_col])
  _df[feature] = _df[ref_col]
  
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=donor_col, how='left')

#FH Filter
def SHH_AppealDescription(
  df, feature='SHH_AppealDescription', ref_col='AppealDescription'
  ):
  df[feature] = df[ref_col]
  return df

#FH Filter
def SHH_GiftDescription(
  df, feature='SHH_GiftDescription', ref_col='Gf_AttrCat_1_01_Description'
  ):
  df[feature] = df[ref_col]
  return df

#FH Filter
def TPL_ClassCode(df, feature='TPL_ClassCode', ref_col='CLASSCODE'):
  df[feature] = df[ref_col]
  return df
#FH Filter
def TPL_Classification(df, feature='TPL_Classification', ref_col='CLASSIFICATION_DESC'):
  df[feature] = df[ref_col]
  return df


def WWH(df):
  df= df.dropna(axis=0, subset=['AppealID', 'DonorID'])
  df['AppealID'] = df['AppealID'].str.upper()
  
  suppressions = schema['ClientValues']
  contains_kw = suppressions['AppealID']['Values']['Contains']
  pattern = '|'.join(contains_kw)
  mask = df['AppealID'].str.contains(pattern).fillna(value=False)
  df = df[~mask]
  
  equals_kw = suppressions['AppealID']['Values']['Equals']
  mask = df['AppealID'].isin(equals_kw)
  df = df[~mask]
  
  startswith_kw = tuple(suppressions['AppealID']['Values']['StartsWith'])
  mask = df['AppealID'].str.startswith(startswith_kw).fillna(value=False)
  df = df[~mask]
  
  df['GiftDate'] = pd.to_datetime(df['GiftDate'])
  df = drop_future_dates(df, 'GiftDate')
  df['GiftAmount'] = format_currency(df['GiftAmount'])
  
  mask = df['AppealID'].str[:2].str.isdigit()
  return df[mask]







# COMMAND ----------

# DBTITLE 1,UPMC RFP
def UPMC(df):
  print('inside client specific')

  #Update DonorID to string
  df['DonorID'] = df['DonorID'].astype(str).str.strip()
  # Remove '.0' suffix from 'DonorID' using regex
  df['DonorID'] = df['DonorID'].str.replace(r'\.0$', '', regex=True)
  print('DonorID updated to string')

  #Update GiftID to string
  df['GiftID'] = df['GiftID'].astype(str).str.strip()
  # Remove '.0' suffix from 'DonorID' using regex
  df['GiftID'] = df['GiftID'].str.replace(r'\.0$', '', regex=True)
  print('GiftID updated to string')

  
  print('Client Specific Functions Completed')
  return df


#FH Filter UPMC
def UPMC_JoinSource(df, feature='UPMC_JoinSource', donor_col='DonorID', gift_col='GiftDate', ref_col='GiftType'):
  _df = df.sort_values([donor_col, gift_col]).drop_duplicates(donor_col)
  _df[feature] = _df[ref_col]
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=donor_col, how='left')

def UPMC_JoinChannel(df, feature='UPMC_JoinChannel', donor_col='DonorID', gift_col='GiftDate', ref_col='Program'):
  _df = df.sort_values([donor_col, gift_col]).drop_duplicates(donor_col)
  _df[feature] = _df[ref_col]
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=donor_col, how='left')

def UPMC_JoinGiftChannelDetail(df, feature='UPMC_JoinGiftChannelDetail', donor_col='DonorID', gift_col='GiftDate', ref_col='ProgramType'):
  _df = df.sort_values([donor_col, gift_col]).drop_duplicates(donor_col)
  _df[feature] = _df[ref_col]
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=donor_col, how='left')



# COMMAND ----------

# MAGIC %md #File management

# COMMAND ----------

# DBTITLE 1,File Management
# File management

def apply_transfer(directories, file_list):
  client_files = []
  
  for k in directories.keys():
    url = directories[k]
    _ = get_all_contents(client_context, url)
    
    client_files.extend(_)
    client_files = [x for x in client_files if x not in file_list]
    client_files = [x for x in client_files if x.split('/')[-1] not in file_list] # added 3/27/2023
    client_files = list(set(client_files)) # added 5/16/22
    
    for cf in client_files:
      transfer_file_basic(cf, check=True)
      
  return client_files


def record_file_list(client_files, cf):
  if client_files:
    cf = cf.append(pd.DataFrame({'FileName': client_files}))
    cf.drop_duplicates().to_csv(os.path.join(filemap.MASTER, 'filenames.csv'), index=False)


def handle_ancillaries(folder, ancillaries, check=False):
  files = os.listdir(folder)
  print('files in folder: ', files)
  for f in files:
    print('f: ', f)
    if os.path.isfile(os.path.join(folder, f)):
      if f in exclusions:
        os.remove(os.path.join(folder, f))
      else:
        for a in ancillaries:
          print('a: ', a)
          if a in f:
            if check:
              print('a: %s, f: %s' %(a,f))
            if ancillaries[a]['action'] == 'keep':
              directory = ancillaries[a]['directory']
              os.rename(
                os.path.join(folder, f),
                os.path.join(folder, directory, f)
              )
            else:
              os.remove(os.path.join(folder, f))
              
              
def handle_transactions(folder):
  files = os.listdir(folder)
  for f in [x for x in files if '.txt' not in x]:
    if os.path.isfile(os.path.join(folder, f)):
      os.rename(
        os.path.join(folder, f),
        os.path.join(folder, 'Data', f)
      )
      
      

def reorder_col_names(cols_a, cols_b):
  #list of columns where common columns between cols_a and cols_b come first, followed by any remaining columns from cols_a
  reordered_a = list(set(cols_a).intersection(set(cols_b)).union(set(cols_a)))
  #list of columns where common columns between cols_b and cols_a come first, followed by any remaining columns from cols_b
  reordered_b = list(set(cols_b).intersection(set(cols_a)).union(set(cols_b)))
  #return lists for both column sets
  return reordered_a, reordered_b

def collect_transaction_files(folder, columns, encoding, key_mapper, date_format, directory='Data'):
    print("collect_transaction_files started")
    dfs = [] # Initialize an empty list to store individual DataFrames
    path = os.path.join(folder, directory) # Construct the full path to the target directory
    
    for f in os.listdir(path): # Iterate over each file in the specified directory
        if '.csv' in f.lower() or 'xls' in f.lower(): # Check if the file is a CSV or Excel file
            print(f)
            try:
                if '.csv' in f.lower():
                    _df = pd.read_csv(os.path.join(path, f), encoding=encoding) # Read the CSV file with the specified encoding
                elif '.xls' in f.lower():  
                    _df = pd.read_excel(os.path.join(path, f)) # Read the Excel file
                _df = _df.rename(columns=key_mapper) #Rename _df columns based on the client KeyMapper in schema
                dfs.append(_df) #Append _df to the list of DataFrame objects
            except Exception as e:
                print(f"Error processing {f}: {str(e)}")
                continue
    if dfs:
        df = pd.concat(dfs, axis=0, ignore_index=True)  #Concatenate all the DFs in the list into a single DF
    else:
        df = pd.DataFrame()  # Create an empty DF if no valid files were processed
    
    if columns:
        df = df[columns] #If specific columns are provided in schema['Columns'], filter the DF to include only those columns
    
    
    if 'ï»¿"Gift Id"' in df.columns:
      df.rename(columns={'ï»¿"Gift Id"': 'GiftID'}, inplace=True)
      print("""Renamed ï»¿"Gift Id" to GiftID from transaction files load in""")

    if 'Ã¯Â»Â¿Close Date' in df.columns:
      df.rename(columns={'Ã¯Â»Â¿Close Date': 'GiftDate'}, inplace=True)
      print("""Renamed Ã¯Â»Â¿Close Date" to GiftDate from transaction files load in""")
    
    if any("Close Date" in col for col in df.columns):
      matching_col = next(col for col in df.columns if "Close Date" in col)
      df.rename(columns={matching_col: "GiftDate"}, inplace=True)
      print(f'Renamed "{matching_col}" to "GiftDate" from transaction files load-in.')

    if 'Salesforce Account ID' in df.columns:
      #df['Salesforce Account ID'] = df['Salesforce Account ID'].astype(object)
      df.rename(columns={'Salesforce Account ID': 'DonorID'}, inplace=True)
      print("""'Salesforce Account ID" to DonorID from transaction files load in""")
      
    if 'Opportunity ID' in df.columns:
      df.rename(columns={'Opportunity ID': 'GiftID'}, inplace=True)
      print("""Renamed Ã¯Â»Â¿Close Date" to GiftID from transaction files load in""")

    df['GiftDate'] = pd.to_datetime(df['GiftDate'], format=date_format, errors='coerce') #Convert GiftDate to datetime format

    total_row_count = len(df)
    print(f"Total number of rows in the DataFrame: {total_row_count}")
    #duplicate_count = df.duplicated(subset=['GiftID', 'DonorID']).sum()
    #print(f"Number of duplicate rows based on GiftID and DonorID: {duplicate_count}")

    print("collect_transaction_files finished")
    
    return df
  



# COMMAND ----------

# DBTITLE 1,Custom File Management


# COMMAND ----------



# COMMAND ----------

