# Databricks notebook source
# MAGIC
# MAGIC
# MAGIC
# MAGIC %md #Parser Notebook

# COMMAND ----------

# MAGIC %md ## Imports and Runs

# COMMAND ----------

# DBTITLE 1,imports
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,utilities
# MAGIC %run ./utilities

# COMMAND ----------

# DBTITLE 1,RFM_Lookup
# MAGIC %run ./RFM_Lookup

# COMMAND ----------

# DBTITLE 1,mc_ukraine
# MAGIC %run ../MC/mc_ukraine

# COMMAND ----------

# DBTITLE 1,FFB_helpers
# MAGIC %run ../FFB/FFB_helpers

# COMMAND ----------

# DBTITLE 1,mount_datalake
# COMMAND ----------

# DBTITLE 1,transfer_files
# MAGIC %run ./transfer_files

# COMMAND ----------

# DBTITLE 1,helpers
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

# MAGIC %md ## Unviersal Functions

# COMMAND ----------

# DBTITLE 1,Global Functions
def apply_client_specific(df, client):
    print(f"🔎 apply_client_specific | client={client} | rows={len(df):,}")

    try:
        from common.clients_loader import get_client_parser
        parser = get_client_parser(client)
        if parser is None:
            # Fallback: legacy globals() lookup for Databricks %run compatibility
            if client in globals() and callable(globals()[client]):
                print(f"➡️ Calling legacy client-specific function: {client}()")
                result = globals()[client](df)
            else:
                print(f"⚠️ No client-specific function found for '{client}'")
                return df
        else:
            print(f"➡️ Calling client-specific function from clients.{client}.parser")
            try:
                result = parser(df, client)
            except TypeError:
                result = parser(df)

        print(f"✅ Finished client-specific function: {client} | rows={len(result):,}")
        return result

    except Exception as e:
        print(f"❌ Error applying client-specific function '{client}': {e}")
        import traceback
        traceback.print_exc()
        return df


def apply_func(df, d, col):
    print(f"🔧 apply_func | column={col} | rows={len(df):,}")

    values = d[col]['Values']
    func   = d[col]['Function']
    inc    = d[col]['Include']

    print(f"   • Function={func}")
    print(f"   • Include={inc}")
    print(f"   • Values count={len(values) if hasattr(values, '__len__') else values}")

    if func not in globals():
        print(f"⚠️ Function '{func}' not found in globals() — skipping")
        return df

    result = globals()[func](df, col, values, inc)

    print(f"✅ Finished apply_func | rows={len(result):,}")
    return result


def apply_dedupe(df, d):
    print(f"🧹 apply_dedupe | rows before={len(df):,}")

    if d:
        col  = d['Column']
        func = d['Function']

        print(f"➡️ Client-specific dedupe applied")
        print(f"   • Function={func}")
        print(f"   • Column={col}")

        if func not in globals():
            print(f"⚠️ Dedupe function '{func}' not found — skipping")
            return df

        result = globals()[func](df, col)
    else:
        print("➡️ Default df.drop_duplicates() applied")
        result = df.drop_duplicates()

    print(f"✅ Finished apply_dedupe | rows after={len(result):,}")
    return result


def apply_filters(df, f):
    print(f"🔍 apply_filters | filter={f} | rows={len(df):,}")

    if f in df.columns:
        print(f"⏭️ Column '{f}' already exists — skipping filter")
        return df
    if f not in globals():
        print(f"⚠️ Filter function '{f}' not found in globals() — skipping")
        return df

    print(f"➡️ Applying filter function: {f}()")
    result = globals()[f](df)

    print(f"✅ Finished apply_filters | rows after={len(result):,}")
    return result

  

# COMMAND ----------

# DBTITLE 1,Suppressions
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
    
def starts_with(df, col, values, inc):
  # Build regex: ^(val1|val2|val3)
  pattern = '^(' + '|'.join(map(str, values)) + ')'
  mask = df[col].astype(str).str.match(pattern, na=False)

  if inc:
      return df.loc[mask]
  else:
      return df.loc[~mask]


def offset_pair_giftid_suppression(df, col, values, inc):
    """
    Drops ONLY exact 1-to-1 cancelling pairs (+X matched to -X)
    within each Gift ID and absolute amount group.
    
    Parameters:
    - df: DataFrame
    - col: Gift ID column
    - values: Payment Amount column
    - inc: If True → include everything
           If False → drop exact cancelling pairs
    """

    if inc:
        # Include everything → do nothing
        return df.copy()

    df = df.copy()
    gift_id_col = col
    amount_col = values

    # Helper column for absolute amounts
    df['_abs_amt_temp'] = df[amount_col].abs()
    rows_to_remove = []

    # Group by Gift ID + abs(amount)
    grouped = df.groupby([gift_id_col, '_abs_amt_temp'])

    for (_, _), group in grouped:
        pos_idx = group[group[amount_col] > 0].index.tolist()
        neg_idx = group[group[amount_col] < 0].index.tolist()
        n_pairs = min(len(pos_idx), len(neg_idx))

        if n_pairs > 0:
            rows_to_remove.extend(pos_idx[:n_pairs])
            rows_to_remove.extend(neg_idx[:n_pairs])

    mask = df.index.isin(rows_to_remove)
    df_filtered = df[~mask].drop(columns=['_abs_amt_temp'])

    return df_filtered

# COMMAND ----------

# DBTITLE 1,Dedupe Functions
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

def dedupe_all_except(df, col=None):
    ignore_cols = ['Transaction File Name', 'Row Added At']
    dedupe_cols = [c for c in df.columns if c not in ignore_cols]
    return df.drop_duplicates(subset=dedupe_cols)

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

# DBTITLE 1,Features - all clients
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

# MAGIC %md ## Client transformations

# COMMAND ----------

# DBTITLE 1,AFHU
# AFHU(df) moved to clients/AFHU/parser.py

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

# DBTITLE 1,CARE
# CARE(df) moved to clients/CARE/parser.py



# COMMAND ----------

# DBTITLE 1,CHOA
# CHOA(df) moved to clients/CHOA/parser.py

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

# DBTITLE 1,FFB
# FFB(df) moved to clients/FFB/parser.py

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
        (df['Media Outlet Code'] == "Direct Marketing") #| 
        #(df['Media Outlet Code'] == "9500")
    )
    
    cond_vc = (
        ((df['Media Outlet Code'] == "9000") & 
         (df['Segment Code'].str.contains("VC|9500|Mid-Level", case=False, na=False))) | 
        (df['Project Code'].str.contains("9500", na=False))
    )
    
    cond_major = (
        (df['Media Outlet Code'] == "9000"))

    cond_general = (
        ((df['Media Outlet Code'] == "9200") |
         (df['Media Outlet Code'] == "Events")))
        
    cond_events = (
        ((df['Media Outlet Code'] == "9900") |
         (df['Media Outlet Code'] == "9999")))
    

    # Apply conditions to create the 'FFB_Program' column
    df['FFB_Program'] = np.select([cond_core, cond_vc, cond_major, cond_general, cond_events], ['CORE', 'VC', 'MAJOR', 'GENERAL', 'EVENTS'], default='Other')
    
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
        ## (df['Original URL'].str.contains("give|givenow|donatenow|givelegacy|giveobituary|givechoice|WEBA50|google|meta|bing|display", na=False, case=False)) |
        ## (df['Segment Code'].str.contains("WEBA50|W50|50YEARS|ONLSPECPAIDADS|ONLSPECWELPSA", na=False, case=False)),
       (
            df['Original URL'].str.lower().isin([
                "give", "givenow", "donatenow",
                "givelegacy", "giveobituary",
                "givechoice", "weba50"
            ]) |
            df['Original URL'].str.contains("google|meta|bing|display", na=False, case=False) |
            df['Segment Code'].str.contains("WEBA50|W50|50YEARS|ONLSPECPAIDADS|ONLSPECWELPSA", na=False, case=False) |
            (df['Segment Code'].str.contains("GIVE", na=False, case=False) & ~df['Segment Code'].str.contains("TODAY", na=False))
        ),

        # Second priority: DM
        (df['Media Outlet Code'].isin(["9005", "Direct Marketing"])) & 
        (df['Segment Code'].str.contains("AQ|CR|VC", na=False, case=False)) & 
        (~df['Segment Code'].str.contains("ONL|GEN|VCRLE|Email|VFV", na=False, case=False)),
        
        # Third priority: Other (based on Media Outlet Code)
        (df['Media Outlet Code'].isin(["9005", "Direct Marketing"]))
        
        # # Fourth priority: VC
        # ((df['Media Outlet Code'] == "9000") & 
        #  (df['Segment Code'].str.contains("VC|9500|Mid-Level", na=False, case=False))) | 
        #  (df['Project Code'].str.contains("9500", na=False))
    ]
    
    choices = ['Paid Digital', 'DM', 'Other'] #, 'VC']
    
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

# DBTITLE 1,FS
### FUSE SAMPLE client ###

# * Note: Fuse Sample (FS) uses MC as a template. Parser functions are copied from MC and are (and should be) nearly identical

# FS(df) moved to clients/FS/parser.py
# [FS function body removed - see clients/FS/parser.py]

# [FS function body removed - 280+ lines moved to clients/FS/parser.py]
# apply_client_specific uses clients_loader.get_client_parser('FS')

#FH Filter
def FS_AppealSubCategory(df, feature='FS_AppealSubCategory'):
  df[feature] = df['AppealSubCategory']
  return df

#FH Filter
def FS_CampaignID(df, feature='FS_CampaignID'):
  df[feature] = df.CampaignID
  return df

#FH Filter
def FS_JoinCampaignID(df, feature='FS_JoinCampaignID'): 
  _df = df.sort_values(['DonorID', 'GiftDate']).drop_duplicates('DonorID', keep='first')
  _df[feature] = _df['MC_CampaignID']
  
  cols = ['DonorID', feature]
  df = pd.merge(df, _df[cols], on='DonorID', how='left')
  
  return df

#FH Filter
def FS_JoinFuseDM(df, feature='FS_JoinFuseDM'): 
  _df = df.sort_values(['DonorID', 'GiftDate']).drop_duplicates('DonorID', keep='first')
  _df[feature] = _df['FS_FuseDM']
  
  cols = ['DonorID', feature]
  df = pd.merge(df, _df[cols], on='DonorID', how='left')
  
  return df

#FH Filter
def FS_Donor984126(df, feature='FS_Donor984126'):
  df[feature] = df.DonorID.astype(str) == '984126'
  return df

#FH Filter
def FS_FuseDM(df, feature='FS_FuseDM'):
  ids = ['143', '145', '146']
  m1 = df.CampaignID.astype(str).isin(ids)
  m2 = (df.CampaignID.astype(str) == '160') & (df.AppealCategory == 'Direct Mail')
  df[feature] = m1 | m2
  return df

#FH Filter
def FS_MidLevelDonor(df, feature='FS_MidLevelDonor'):
  _filemap = Filemap('FS')
  _df = pd.read_csv(os.path.join(_filemap.RAW, 'MidLevelDonors', 'ML Pool.csv'))

  donors = _df['CnBio_ID'].dropna().astype('str').str.replace(' ','').unique()
  df[feature] = df.DonorID.dropna().astype('str').str.replace(' ','').isin(donors)
  
  return df

#FH Filter
def FS_UkraineDonor(df, feature='FS_UkraineDonor', donor='DonorID'):
  df = isUkraine(df, 'UkraineGift')
  _df = df.groupby(donor)['UkraineGift'].sum().to_frame(name='NumUkraineGifts').reset_index()
  _df[feature] = _df['NumUkraineGifts'] > 0
  cols = [donor, feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')

#FH Filter
def FS_UkraineJoin(df, feature='FS_UkraineJoin', donor_col='DonorID', gift_col='GiftDate'):
  _df = df.sort_values([donor_col, gift_col]).drop_duplicates(donor_col)
  _df = isUkraine(_df, feature)
  cols = [donor_col, feature]
  return pd.merge(df, _df[cols], on=cols[0], how='left')

#FH Filter
def FS_Sustainer(df, feature='FS_Sustainer'):
  sustainers = df.loc[df.RecurringGift, 'DonorID'].unique()
  df[feature] = df.DonorID.isin(sustainers)
  return df






# Generate artificial gifts

# import os
# import pandas as pd
# import hashlib
# import numpy as np

# def FS_generate_artificial_gifts(df):

#     filemap = Filemap('FS')
#     parquet_path = filemap.MASTER + "artificial_gifts.parquet"

#     df = df.copy()
#     df['GiftDate'] = pd.to_datetime(df['GiftDate'])

#     # --------------------------------------------------
#     # 1️⃣ Load existing artificial gifts (minimal columns first)
#     # --------------------------------------------------
#     if os.path.exists(parquet_path):
#         artificial_existing = pd.read_parquet(
#             parquet_path,
#             columns=["GiftID", "GiftDate"]
#         )
#         artificial_existing['GiftDate'] = pd.to_datetime(artificial_existing['GiftDate'])

#         # infer last processed real date
#         prev_max_real_date = artificial_existing['GiftDate'].max() - pd.Timedelta(days=7)
#     else:
#         artificial_existing = pd.DataFrame()
#         prev_max_real_date = pd.Timestamp.min

#     # --------------------------------------------------
#     # 2️⃣ Filter new real data only
#     # --------------------------------------------------
#     new_real = df[df['GiftDate'] > prev_max_real_date]

#     if new_real.empty:
#         print("No new real data.")
#         if os.path.exists(parquet_path):
#             artificial_existing = pd.read_parquet(parquet_path)
#             return pd.concat([df, artificial_existing], ignore_index=True)
#         return df

#     # --------------------------------------------------
#     # 3️⃣ Assign growth rates (vectorized)
#     # --------------------------------------------------
#     conditions = [
#         new_real['GiftFiscal'] >= 2026,
#         new_real['GiftFiscal'] == 2025
#     ]

#     choices = [0.15, 0.10]

#     new_real = new_real.copy()
#     new_real['GrowthRate'] = np.select(conditions, choices, default=0.05)

#     # --------------------------------------------------
#     # 4️⃣ Deterministic hash bucket (stable across runs)
#     # --------------------------------------------------
#     def hash_to_bucket(x):
#         return (int(hashlib.md5(str(x).encode()).hexdigest(), 16) % 100) / 100

#     buckets = new_real['GiftID'].map(hash_to_bucket)

#     artificial_new = new_real[buckets < new_real['GrowthRate']].copy()

#     if artificial_new.empty:
#         print("No artificial gifts generated.")
#         if os.path.exists(parquet_path):
#             artificial_existing = pd.read_parquet(parquet_path)
#             return pd.concat([df, artificial_existing], ignore_index=True)
#         return df

#     # --------------------------------------------------
#     # 5️⃣ Modify artificial gifts
#     # --------------------------------------------------
#     artificial_new['GiftID'] = artificial_new['GiftID'].astype(str) + "_A7D"
#     artificial_new['GiftDate'] = artificial_new['GiftDate'] + pd.Timedelta(days=7)
#     artificial_new = artificial_new.drop(columns=['GrowthRate'])

#     # --------------------------------------------------
#     # 6️⃣ Append ONLY artificial gifts to parquet
#     # --------------------------------------------------
#     if os.path.exists(parquet_path):
#         artificial_existing_full = pd.read_parquet(parquet_path)
#         artificial_updated = pd.concat(
#             [artificial_existing_full, artificial_new],
#             ignore_index=True
#         )
#     else:
#         artificial_updated = artificial_new

#     artificial_updated.to_parquet(parquet_path, index=False)

#     print(f"{len(artificial_new)} artificial gifts added.")

#     # --------------------------------------------------
#     # 7️⃣ Return real + artificial
#     # --------------------------------------------------
#     return pd.concat([df, artificial_updated], ignore_index=True)

# COMMAND ----------

# DBTITLE 1,FWW
# FWW(df) moved to clients/FWW/parser.py

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

# DBTITLE 1,HKI
# HKI(df) moved to clients/HKI/parser.py

# COMMAND ----------

# DBTITLE 1,HFHI
# HFHI(df) moved to clients/HFHI/parser.py

# COMMAND ----------

# DBTITLE 1,JS
def JS(df):

  
  df['CampaignCode'] = df['SourceCode'].str[:5] # Creating campaign code from source code
  df['ProgramType'] = df['Attributions'].notnull().map({True: "IRA/DAF Gift", False: ""}) # Assigning Attributions (IRA/DAF status) to Program Type variable if not null
  df["State"] = df["Preferred State/Province"] # Consolidating State variable
  df["Preferred Zip/Postal"] = df["Preferred Zip/Postal"].astype(str)
  df["ZipCode"] = df["Preferred Zip/Postal"] # Renaming Zip code variable
  df['ZipCode'] = df['ZipCode'].astype(str).str.zfill(5) # add 0 to front if length is 4

  return df

    # Transfer Digital Reporting data to datalake



### Digital Reporting ###

# Programmatic 
# def JS_transfer_digital_programmatic_ads_data(
#     digital_data_url='Shared Documents/JS/Digital Reporting/Programmatic Ads Data'
# ):
    
#     print("JS Programmatic data transfer started")
#     try:
#         import os
#         import io
#         import json
#         import pandas as pd
#         from office365.sharepoint.files.file import File

#         # ---------------------------------------------
#         # SharePoint Setup
#         # ---------------------------------------------
#         SHAREPOINT_URL_JS = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"

#         filemap_sp = Filemap('Fuse')
#         with open(os.path.join(filemap_sp.SCHEMA, 'schema.json'), 'r') as f:
#             USER = json.load(f)['SharePointUID']

#         client_context_js = get_sharepoint_context_app_only(SHAREPOINT_URL_JS)
#         #print("SharePoint context loaded.")

#         # ---------------------------------------------
#         # Find files in directory
#         # ---------------------------------------------
#         client_files = get_folder_files(client_context_js, digital_data_url)

#         TARGET_PREFIX = "JUSTSAFE_" #change as necessary
#         target_file_path = None

#         for file_path in client_files:
#             file_name = os.path.basename(file_path)

#             # Accept .xlsx OR .csv
#             if file_name.startswith(TARGET_PREFIX) and (file_name.endswith(".xlsx") or file_name.endswith(".csv")):
#                 target_file_path = file_path
#                 break

#         if not target_file_path:
#             raise FileNotFoundError(
#                 f"No .xlsx or .csv file found starting with '{TARGET_PREFIX}' in {digital_data_url}"
#             )

#         #print(f"Found file: {target_file_path}")

#         # ---------------------------------------------
#         # Download file from SharePoint
#         # ---------------------------------------------
#         response = File.open_binary(client_context_js, target_file_path)
#         bytes_file = io.BytesIO(response.content)

#         # ---------------------------------------------
#         # Read CSV or Excel
#         # ---------------------------------------------
#         if target_file_path.lower().endswith(".xlsx"):
#             prog_ads_data = pd.read_excel(bytes_file, sheet_name=0)
#         else:
#             prog_ads_data = pd.read_csv(bytes_file)

#         # ---------------------------------------------
#         # Cleaning
#         # ---------------------------------------------
#         if "Client Spend" in prog_ads_data.columns:
#             prog_ads_data["Client Spend"] = (
#                 prog_ads_data["Client Spend"].astype(float).round(4)
#             )

#         # ---------------------------------------------
#         # Export to datalake
#         # ---------------------------------------------
#         filemap = Filemap('JS')
#         out_path = os.path.join(
#             filemap.CURATED, "JS_Digital", "JS_Programmatic.csv"
#         )

#         prog_ads_data.to_csv(out_path, float_format="%.4f", index=False)

#         print(f"JS Programmatic ads data transferred to datalake: {out_path}")

#     except Exception as e:
#         print(f"Error occurred: {e}")
#         pass



def JS_transfer_digital_programmatic_ads_data(
    digital_data_url='Shared Documents/JS/Digital Reporting/Programmatic Ads Data'
):
    
    print("JS Programmatic data transfer started")
    try:
        import os
        import io
        import json
        import pandas as pd
        from office365.sharepoint.files.file import File

        # ---------------------------------------------
        # SharePoint Setup
        # ---------------------------------------------
        SHAREPOINT_URL_JS = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"

        filemap_sp = Filemap('Fuse')
        with open(os.path.join(filemap_sp.SCHEMA, 'schema.json'), 'r') as f:
            USER = json.load(f)['SharePointUID']

        client_context_js = get_sharepoint_context_app_only(SHAREPOINT_URL_JS)

        # ---------------------------------------------
        # Find most recently created file in directory with prefix
        # ---------------------------------------------
        client_files = get_folder_files(client_context_js, digital_data_url, return_metadata=True)

        TARGET_PREFIX = "JUSTSAFE_"
        target_file_path = None

        matching_files = [
            (file_path, created_time)
            for file_path, created_time, mod_time in client_files
            if os.path.basename(file_path).startswith(TARGET_PREFIX)
            and (file_path.endswith(".xlsx") or file_path.endswith(".csv"))
        ]

        if not matching_files:
            raise FileNotFoundError(
                f"No .xlsx or .csv file found starting with '{TARGET_PREFIX}' in {digital_data_url}"
            )

        # Pick the most recently created file
        target_file_path = max(matching_files, key=lambda x: x[1])[0]

        # ---------------------------------------------
        # Download file from SharePoint
        # ---------------------------------------------
        response = File.open_binary(client_context_js, target_file_path)
        bytes_file = io.BytesIO(response.content)

        # ---------------------------------------------
        # Read CSV or Excel
        # ---------------------------------------------
        if target_file_path.lower().endswith(".xlsx"):
            prog_ads_data = pd.read_excel(bytes_file, sheet_name=0)
        else:
            prog_ads_data = pd.read_csv(bytes_file)

        # ---------------------------------------------
        # Cleaning
        # ---------------------------------------------
        if "Client Spend" in prog_ads_data.columns:
            prog_ads_data["Client Spend"] = (
                prog_ads_data["Client Spend"].astype(float).round(4)
            )

        # ---------------------------------------------
        # Export to datalake
        # ---------------------------------------------
        filemap = Filemap('JS')
        out_path = os.path.join(
            filemap.CURATED, "JS_Digital", "JS_Programmatic.csv"
        )

        prog_ads_data.to_csv(out_path, float_format="%.4f", index=False)

        print(f"JS Programmatic ads data transferred to datalake: {out_path}")

    except Exception as e:
        print(f"Error occurred: {e}")
        pass



# Email ads

def JS_transfer_digital_email_ads_data(email_path):
    """Connects to SharePoint, reads most recently created CSV/XLSX file, exports to datalake"""
    
    print("JS Email ads data transfer started")

    try:
        import os
        import json
        import pandas as pd
        from io import BytesIO

        # -----------------------------------
        # Connect to SharePoint
        # -----------------------------------
        SHAREPOINT_URL = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"

        filemap_sp = Filemap('Fuse')
        with open(os.path.join(filemap_sp.SCHEMA, 'schema.json'), 'r') as f:
            USER = json.load(f)['SharePointUID']

        client_context = get_sharepoint_context_app_only(SHAREPOINT_URL)

        # -----------------------------------
        # Get all files in folder
        # -----------------------------------
        files = get_folder_files(
            client_context,
            "Shared Documents/JS/Digital Reporting/Email Ads Data"
        )

        file_info_list = []

        for file_path in files:

            # Only consider CSV/XLSX
            if not file_path.lower().endswith((".csv", ".xlsx")):
                continue

            file_obj = client_context.web.get_file_by_server_relative_url(file_path)
            client_context.load(file_obj)
            client_context.execute_query()

            file_info_list.append({
                "path": file_path,
                "name": file_obj.properties["Name"],
                "created": pd.to_datetime(file_obj.properties["TimeCreated"]),
                "modified": pd.to_datetime(file_obj.properties["TimeLastModified"])
            })

        if not file_info_list:
            raise Exception("No CSV or XLSX files found in SharePoint folder.")

        # -----------------------------------
        # Get most recently CREATED file
        # -----------------------------------
        latest_file = max(file_info_list, key=lambda x: x["created"])

        print(f"Most recent file found: {latest_file['name']}")
        print(f"Created on: {latest_file['created']}")

        # -----------------------------------
        # Download file into memory
        # -----------------------------------

        from io import BytesIO

        file = client_context.web.get_file_by_server_relative_url(
            latest_file["path"]
        )

        file_buffer = BytesIO()
        file.download(file_buffer)
        client_context.execute_query()
        file_buffer.seek(0)

        # -----------------------------------
        # Read into pandas
        # -----------------------------------
        if latest_file["name"].lower().endswith(".csv"):
            df = pd.read_csv(file_buffer)
        else:
            df = pd.read_excel(file_buffer)

            df = pd.read_excel(file_bytes)

        # -----------------------------------
        # Export to datalake
        # -----------------------------------
        filemap = Filemap('JS')
        out_path = os.path.join(
            filemap.CURATED,
            "JS_Digital",
            "JS Email Report.csv"
        )

        df.to_csv(out_path, float_format="%.4f", index=False)

        print(f"JS Email ads data transferred to datalake: {out_path}")

    except Exception as e:
        print(f"Error occurred: {e}")

    

# # Meta ads
# def JS_transfer_digital_meta_ads_data(digital_data_url='Shared Documents/JS/Digital Reporting/Meta Ads Data'):
#   """
#   Reads Meta Ads Excel files from SharePoint and exports them as CSVs to the ASJ datalake.
#   Appends new data to existing files and deduplicates based on date-ad ID composite key,
#   keeping records from the new file when duplicates exist.
#   """
#   print("JS Meta ads data transfer started")
#   try:
#       import os
#       import json
#       import pandas as pd
#       import io
#       from office365.sharepoint.files.file import File

#       # -------------------------------
#       # SharePoint setup
#       # -------------------------------
#       SHAREPOINT_URL_JS = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"
#       filemap_sp = Filemap('Fuse')

#       with open(os.path.join(filemap_sp.SCHEMA, 'schema.json'), 'r') as f:
#           USER = json.load(f)['SharePointUID']

#       # Initialize client context
#       client_context_js = get_sharepoint_context_app_only(SHAREPOINT_URL_JS)

#       # Retrieve all files in the SharePoint folder
#       client_files = get_folder_files(client_context_js, digital_data_url)

#       # -------------------------------
#       # Define target prefixes for Meta files
#       # -------------------------------
#       TARGET_PREFIXES = [
#           "Just-Safe-Tracker",
#           "Just-Safe-Reactions"
#       ]

#       # Define the deduplication columns (same for both files)
#       DATE_COL = "Day"
#       AD_ID_COL = "Ad ID"

#       # -------------------------------
#       # Process each target file
#       # -------------------------------
#       for prefix in TARGET_PREFIXES:
#           target_file_path = None

#           # Find the first matching Excel file for this prefix
#           for file_path in client_files:
#               file_name = os.path.basename(file_path)
#               if file_name.startswith(prefix) and file_name.endswith('.xlsx'):
#                   target_file_path = file_path
#                   break

#           # Skip if no file found
#           if not target_file_path:
#               print(f"No Excel file found starting with '{prefix}' in {digital_data_url}. Skipping.")
#               continue

#           # Download the file
#           response = File.open_binary(client_context_js, target_file_path)
#           bytes_file = io.BytesIO(response.content)

#           # Read into pandas DataFrame
#           df_new = pd.read_excel(bytes_file, sheet_name=0)

#           # -------------------------------
#           # Cleaning / processing
#           # -------------------------------
#           if 'Spend' in df_new.columns:
#               df_new['Spend'] = df_new['Spend'].astype(float).round(4)

#           # -------------------------------
#           # Load existing data if it exists
#           # -------------------------------
#           filemap = Filemap('JS')
#           output_name = f"Meta-{prefix}.csv"
#           output_path = os.path.join(filemap.CURATED, 'JS_Digital', output_name)

#           if os.path.exists(output_path):
#               df_existing = pd.read_csv(output_path)
#               print(f"Found existing data for '{prefix}': {len(df_existing)} rows")
#           else:
#               print(f"No existing data found for '{prefix}', creating new file")
#               df_existing = pd.DataFrame()

#           # -------------------------------
#           # Verify required columns exist
#           # -------------------------------
#           if DATE_COL not in df_new.columns or AD_ID_COL not in df_new.columns:
#               print(f"Warning: Required columns not found in '{prefix}'")
#               print(f"Expected: '{DATE_COL}' and '{AD_ID_COL}'")
#               print(f"Available columns: {df_new.columns.tolist()}")
#               # If columns not found, just save new data
#               df_final = df_new
#           else:
#               print(f"Using deduplication key: {DATE_COL} + {AD_ID_COL}")
              
#               # -------------------------------
#               # Deduplicate: new file takes precedence
#               # -------------------------------
#               if len(df_existing) > 0:
#                   # Normalize date columns for comparison
#                   df_new[DATE_COL] = pd.to_datetime(df_new[DATE_COL], errors='coerce')
#                   df_existing[DATE_COL] = pd.to_datetime(df_existing[DATE_COL], errors='coerce')
                  
#                   # Add source marker to track which file data came from
#                   df_new['_source'] = 'new'
#                   df_existing['_source'] = 'old'
                  
#                   # Combine datasets
#                   df_combined = pd.concat([df_new, df_existing], ignore_index=True)
                  
#                   # Sort so new records come first, then deduplicate
#                   df_combined = df_combined.sort_values(by='_source', ascending=True)  # 'new' < 'old' alphabetically
                  
#                   # Keep first occurrence (which will be from new file due to sorting)
#                   df_final = df_combined.drop_duplicates(subset=[DATE_COL, AD_ID_COL], keep='first')
                  
#                   # Remove the source marker column
#                   df_final = df_final.drop(columns=['_source'])
                  
#                   rows_before = len(df_combined)
#                   rows_after = len(df_final)
#                   duplicates_removed = rows_before - rows_after

#               else:
#                   df_final = df_new


#           # -------------------------------
#           # Export to datalake
#           # -------------------------------
#           df_final = df_final.sort_values(by="Day", ascending=False)
#           df_final.to_csv(output_path, float_format="%.4f", index=False)
#           print(f"JS Meta ads data '{prefix}' transferred to datalake: {output_name}")

#   except Exception as e:
#       print(f"Path may not exist or another error occurred: {e}")
#       pass




# COMMAND ----------

# DBTITLE 1,MC
#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet

#Updates to add? 
""" df['SourceCode'] = (df['SourceCode'].fillna('') + df['SegmentCode'].fillna('')).replace('', np.nan) 

"""

def MC(df):
    """
    MC client-specific processing.
    - Prints BEFORE/AFTER for each major step.
    - Prints an error if anything fails mid-way.
    - ALWAYS returns the (possibly partial) df.
    """
    print("=== Starting MC Client Specific Functions ===")
    from datetime import datetime
    import traceback

    try:

        print("Starting: Add SustainerGiftFlag")
        sustainer_flag_column_name = 'SustainerGiftFlag'
        condition = df['CampaignID'].astype(str).isin(['147'])
        df[sustainer_flag_column_name] = np.where(condition, 'Sustainer', '1x')
        print("Finished: Add SustainerGiftFlag")


        # --- Add RecurringGift ---
        print("Starting: Add RecurringGift")
        recurring_gift_column_name = 'RecurringGift'
        condition = df['CampaignID'].astype(str).isin(['147'])
        df[recurring_gift_column_name] = np.where(condition, True, False)
        print("Finished: Add RecurringGift")

        # --- Helper: numeric donor ---
        print("Starting: Define helper _numeric_donor")
        def _numeric_donor(_df, col='DonorID'):
            _df[col] = pd.to_numeric(_df[col], errors='coerce')
            _df = _df.dropna(subset=[col])
            _df[col] = _df[col].astype(str).str.replace(r'\.0', '', regex=True)
            return _df
        print("Finished: Define helper _numeric_donor")

        # --- Cast DonorID to numeric ---
        print("Starting: Cast DonorID to numeric")
        df = _numeric_donor(df)
        print("Finished: Cast DonorID to numeric")

        #Add Source Code (Based on James inputs 9/2/25)
        print("Starting: Add SourceCode (New Logic 9/2/25)")
        df['SourceCode'] = (
          df['CampaignCode'].fillna('').replace('None', '') +
          df['SegmentCode'].fillna('').replace('None', '') +
          df['PackageCode'].fillna('').replace('None', ''))
        print("Finished: Add SourceCode (New Logic 9/2/25)")



        # --- Add Source Code logic for Unsourced Fuse Campaigns ----------------------
        print("=== Starting: Add Source Code logic for Unsourced Fuse Campaigns ===")

        # Preserve original SourceCode
        df['SourceCode_OG'] = df['SourceCode']
        print("[1/8] Created backup column: SourceCode_OG")

        # Load Source Code reference file
        print("[2/8] Loading Source Code reference data...")
        sc = load_sc('MC').sort_values('MailDate', ascending=False)
        print(f"    Loaded {len(sc)} source code records for MC")

        # Define prefix mapping for MC
        prefix_map_mc = {
            "AH": "Appeal",
            "AB": "Appeal",
            "AG": "Appeal",
            "AN": "Appeal",
            "MA": "Appeal",
            "QQ": "Cold",
            "MQ": "Cold",
            "AW": "Welcome"
        }
        print(f"[3/8] Prefix map initialized with {len(prefix_map_mc)} keys")

        # Extract valid SourceCodes from sc
        valid_sc_codes = set(sc['SourceCode'].astype(str))
        print(f"[4/8] Extracted {len(valid_sc_codes)} valid SourceCodes")

        # Identify rows matching prefixes + campaign IDs + NOT in reference SC list
        mask = (
            df['SourceCode'].str.startswith(tuple(prefix_map_mc.keys())) &
            df['CampaignID'].astype(str).isin(['143', '145']) &
            ~df['SourceCode'].isin(valid_sc_codes)
        )
        print(f"[5/8] Identified {mask.sum()} potential unsourced campaign rows")

        # Map prefix to SourceType
        df.loc[mask, 'SourceType'] = (
            df.loc[mask, 'SourceCode']
            .str[:2]
            .map(prefix_map_mc)
        )
        print(f"[6/8] Mapped SourceType for {df.loc[mask, 'SourceType'].notna().sum()} rows")

        # Get GiftFiscal based on schema
        print("[7/8] Calculating GiftFiscal values...")
        schema = get_schema('MC')
        fy_month = schema["firstMonthFiscalYear"]
        df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', fy_month)
        print("    GiftFiscal column added")

        # ---------------------------------------------------------------------------
        # Build new SourceCode
        #   - If CampaignCode exists → use it directly
        #   - Else → fallback to Unsourced logic
        # ---------------------------------------------------------------------------
        has_campaign_code = mask & df['CampaignCode'].notna()

        df.loc[has_campaign_code, 'SourceCode'] = (
            df.loc[has_campaign_code, 'CampaignCode'].astype(str)
        )

        fallback_mask = mask & df['CampaignCode'].isna()

        df.loc[fallback_mask, 'SourceCode'] = (
            df.loc[fallback_mask, 'GiftFiscal'].astype(str)
            + df.loc[fallback_mask, 'SourceType']
            + "Unsourced"
        )

        print(
            f"[8/8] Updated SourceCode values — "
            f"{has_campaign_code.sum()} from CampaignCode, "
            f"{fallback_mask.sum()} using fallback logic"
        )

        print("=== Finished: Add Source Code logic for Unsourced Fuse Campaigns ===")

      #------------------------------------------------------------------------------------------------------------------
        print("=== Starting: Add Sustainer Status Column ===")

        # Step 1 — Ensure GiftDate is datetime
        print("[1/7] Converting GiftDate to datetime...")
        df['GiftDate'] = pd.to_datetime(df['GiftDate'])

        # Step 2 — Find the max gift date in the data to determine snapshot month
        print("[2/7] Finding latest GiftDate in dataset...")
        max_gift_date = df['GiftDate'].max()
        print(f"     Latest GiftDate found: {max_gift_date}")

        # Ensure column exists up front
        if 'Sustainer Status' not in df.columns:
            df['Sustainer Status'] = "N/A"


        # Step 3 — Define the last complete month
        snapshot_month = max_gift_date.to_period('M')
        last_complete_month = snapshot_month - 1
        print(f"[3/7] Last complete month identified as: {last_complete_month}")

        # Step 4 — Filter to recurring gifts
        print("[4/7] Filtering recurring gifts (RecurringGift == 1)...")
        rg = df[df['RecurringGift'] == 1].copy()
        rg['gift_month'] = rg['GiftDate'].dt.to_period('M')
        print(f"     Found {len(rg):,} recurring gift rows")

        # Step 5 — Keep recurring gifts on or before last complete month
        print("[5/7] Filtering to recurring gifts on/before last complete month...")
        rg = rg[rg['gift_month'] <= last_complete_month]
        print(f"     Remaining rows after filter: {len(rg):,}")

        # Step 6 — Calculate recency in months
        print("[6/7] Calculating months since last recurring gift...")
        rg['months_ago'] = (
            (last_complete_month.year - rg['gift_month'].dt.year) * 12
            + (last_complete_month.month - rg['gift_month'].dt.month)
        )

        # Step 7 — Identify most recent recurring gift per donor
        print("[7/7] Determining most recent recurring gift per donor...")
        recent_rg = (
            rg.groupby('DonorID')['months_ago']
              .min()
              .reset_index()
              .rename(columns={'months_ago': 'months_since_last_recurring'})
        )
        print(f"     Computed recency for {recent_rg['DonorID'].nunique():,} donors")

        # Classification function
        def classify_sustainer(m):
            if pd.isna(m):
                return "N/A"
            m = int(m)
            if m == 0:
                return "Active"
            elif 1 <= m <= 5:
                return "Lapsing- Short"
            elif 6 <= m <= 11:
                return "Lapsing-Long"
            else:
                return "Lapsed"

        recent_rg['Sustainer Status'] = recent_rg['months_since_last_recurring'].apply(classify_sustainer)

        # 🔑 In-place update (NO MERGE)
        print("🔄 Updating Sustainer Status in-place (no duplicate columns)...")
        status_map = recent_rg.set_index('DonorID')['Sustainer Status']

        df['Sustainer Status'] = (
            df['DonorID']
            .map(status_map)
            .fillna("N/A")
        )

        print("=== Complete: Add Sustainer Status Column ===")



        #---------------------------------------------------------------------------------
        print("🔧 Assigning GiftChannel from CampaignID")

        # Normalize CampaignID once
        df["CampaignID"] = (
            df["CampaignID"]
                .astype(str)
                .str.replace(".0", "", regex=False)
                .str.strip()
        )

        DIGITAL_IDS       = {"132", 132, "133", 133, "134", 134, "135",135, "136", 136, "137", 137, "139" ,139}
        DIRECT_MAIL_IDS   = {"143", 143, "145", 145,  "146", 146, "160", 160}
        DAF_STOCK_IRA_IDS = {"157", 157}
        SUSTAINER_IDS     = {"147", 147}

        df["GiftChannel"] = "Other"

        df.loc[df["CampaignID"].isin(DIGITAL_IDS),       "GiftChannel"] = "Digital"
        df.loc[df["CampaignID"].isin(DIRECT_MAIL_IDS),   "GiftChannel"] = "Direct Mail"
        df.loc[df["CampaignID"].isin(DAF_STOCK_IRA_IDS), "GiftChannel"] = "DAF, Stock, IRA"
        df.loc[df["CampaignID"].isin(SUSTAINER_IDS),     "GiftChannel"] = "Sustainer"

        print(
            "✅ GiftChannel assigned | "
            f"Digital={df['GiftChannel'].eq('Digital').sum():,}, "
            f"Direct Mail={df['GiftChannel'].eq('Direct Mail').sum():,}, "
            f"Sustainer={df['GiftChannel'].eq('Sustainer').sum():,}, "
            f"DAF/Stock/IRA={df['GiftChannel'].eq('DAF, Stock, IRA').sum():,}, "
            f"Other={df['GiftChannel'].eq('Other').sum():,}"
        )

        #--------------------------------------------------------------
        print("🔧 Assigning GiftChannelSubtype from CampaignID")

        SUBTYPE_MAP = {
            "132": "Email Receipts",
            "133": "Email Renewal",
            "134": "Email Prospect Conversion",
            "135": "Digital Marketing Paid Search",
            "136": "Digital Marketing Renewal",
            "137": "Digital Marketing Acquisition",
            "139": "Organic Website Visits",
            "143": "Direct Mail Renewal",
            "145": "Direct Mail Acquisition",
            "146": "Direct Mail Receipts",
            "147": "Sustainer",
            "148": "Telemarketing",
            "149": "Emergency Fundraising",
            "150": "Community Fundraising",
            "156": "Affinity Partnerships",
            "157": "DAFs, Stocks, IRAs",
            "159": "Texting",
            "160": "Organic Direct Mail"
        }

        df["GiftChannelSubtype"] = df["CampaignID"].map(SUBTYPE_MAP).fillna("Other")

        print(
            "✅ GiftChannelSubtype assigned | "
            f"Mapped={df['CampaignID'].isin(SUBTYPE_MAP).sum():,}, "
            f"Other={df['GiftChannelSubtype'].eq('Other').sum():,}"
        )
        #---------------------------------------------------------------------------------

        print("=== MC Client Specific Functions completed successfully ===")

    except Exception as e:
        print("!!! ERROR in MC(): Function stopped unexpectedly !!!")
        print("Error details:", str(e))
        print("Traceback:\n", traceback.format_exc())
        # DO NOT raise; we will return the partial df.

    finally:
        print("=== MC Function Finished (success or failure) — returning df ===")
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

#FH Filter
def MC_Sustainer(df, feature='MC_Sustainer'):
  sustainers = df.loc[df.RecurringGift, 'DonorID'].unique()
  df[feature] = df.DonorID.isin(sustainers)
  return df

# COMMAND ----------

# DBTITLE 1,NJH
#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet

def NJH(df):
  cols = ["Gf_Appeal", "Gf_Package"]
  for c in cols:
    df[c] = df[c].fillna(value = "")
  df['SourceCode'] = df.Gf_Appeal + df.Gf_Package


  print("NJH client specific columns successfully applied")
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

# DBTITLE 1,RADY
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
  Direct mail = any campaign code beginning with DM
  e-phil = digital
  DR = would also be lumped as Direct Mail
  EV = Giveathon/ CMN and Rady
  '''
  df[feature] = 'N/A'

  mask = ((df.CampaignCode.str.startswith('DM')) | (df.CampaignCode.str.startswith('DR'))).fillna(value=False)
  df.loc[mask, feature] = 'Direct Mail'

  mask = (df.CampaignCode.str.startswith('EPHIL')).fillna(value=False)
  df.loc[mask, feature] = 'Digital'

  # updated condition to exact matches only
  mask = df.CampaignCode.isin(['EV-GiveathonCMN', 'EV-GiveathonRCHSD'])
  df.loc[mask, feature] = 'EV-Giveathon'

  other_codes = ["SDMag", "SOFTASK", "UNSOL-Tribute", "UNSOL-General Donati"]
  mask = df.CampaignCode.isin(other_codes)
  df.loc[mask, feature] = 'Other'

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


### Digital Reporting ###

# Programmatic
def RADY_transfer_digital_programmatic_ads_data(programmatic_path):
  '''Calls Choozle API and pulls RADY Programmatic data report'''
  print("RADY Programmatic data transfer started")

  '''* All data besides Revenue: *'''
  if programmatic_path == "Shared Documents/RADY/Digital Reporting/Programmatic Ads Data": # if programmatic folder in sharepoint exists, pull report from Choozle API
    
    try: 
      import os
      import time
      import hmac
      import hashlib
      import requests
      import pandas as pd
      from datetime import datetime, timezone, date

      ## Required params
      BASE_API = "https://app.choozle.com/api" 
      EMAIL = 'fusedigital@fusefundraising.com'

      choozle_key = dbutils.secrets.get( # Get Choozle API key from vault
        scope='fuse-etl-key-vault', 
        key='choozle-api-key' 
        )

      API_SECRET = choozle_key

      ## Account and optional params
      date_start_param = '2020-01-01'
      date_end_param = date.today()

      ## Function to get token
      def get_RADY_choozle_auth_token(email: str, secret: str) -> str:
          timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
          message = f"{email}{timestamp}".encode("utf-8")
          signature = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()

          resp = requests.post(
              f"{BASE_API}/auth", 
              headers={"Content-Type": "application/x-www-form-urlencoded"},
              data={"email": email, "timestamp": timestamp, "signature": signature},
              timeout=30
          )
          resp.raise_for_status()
          return resp.json()["token"]


      ## Function to fetch report
      def fetch_RADY_choozle_reports(token: str, account_id: int, date_start: str, date_end: str, **filters):
          headers = {"token": token}
          params = {
              "account_id": account_id,
              "date_start": date_start,
              "date_end": date_end,
              **filters
          }
          resp = requests.get(f"{BASE_API}/reports", headers=headers, params=params, timeout=60)
          resp.raise_for_status()
          return resp.json()

      ## Get report
      if __name__ == "__main__":
          token = get_RADY_choozle_auth_token(EMAIL, API_SECRET)
          print("✅ Auth token retrieved")

          report = fetch_RADY_choozle_reports(
              token=token,
              account_id=19927, # RADY account id
              date_start=date_start_param,  # optional filters: start and end date
              date_end=date_end_param,
          )
          print("✅ Report retrieved")

      report = pd.DataFrame(report) # Change to df
      
      ## Export to datalake
      filemap = Filemap('RADY')
      out_path = os.path.join(
      filemap.CURATED, "RADY_Digital", "RADY-Programmatic.csv")
      report.to_csv(out_path, float_format="%.4f", index=False)
      print(f"RADY Programmatic ads data transferred to datalake: {out_path}")

    except Exception as e:
          print(f"Error occurred: {e}")
          pass

  '''* Revenue data: *''' # Get revenue data (not included in Choozle API pull)

  import os
  import io
  import json
  from office365.sharepoint.files.file import File

  print("RADY Programmatic Revenue data transfer started")

  ## read in most recent conversion revenue CSV from Sharepoint

  ## SharePoint Setup
  SHAREPOINT_URL_RADY = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"

  filemap_sp = Filemap('Fuse')
  with open(os.path.join(filemap_sp.SCHEMA, 'schema.json'), 'r') as f:
      USER = json.load(f)['SharePointUID']

  client_context_rady = get_sharepoint_context_app_only(SHAREPOINT_URL_RADY)

  # Get all files in contact report folder
  files = get_folder_files(client_context_rady, 'Shared Documents/RADY/Digital Reporting/Programmatic Ads Data/Conversion Revenue')

  # Get latest contact report
  file_info_list = []

  for file_path in files:
      file_obj = client_context_rady.web.get_file_by_server_relative_url(file_path)
      client_context_rady.load(file_obj)
      client_context_rady.execute_query()

      file_info_list.append({
          "path": file_path,
          "name": file_obj.properties["Name"],
          "modified": pd.to_datetime(file_obj.properties["TimeLastModified"]),
          "created": pd.to_datetime(file_obj.properties["TimeCreated"])})

  latest_file = max(file_info_list, key=lambda x: x["created"]) # Most recently created report metadata

  response = File.open_binary(client_context_rady, latest_file['path']) # Reading in report
  bytes_file = io.BytesIO(response.content)

  if latest_file['name'].lower().endswith(".csv"):
    new_revenue_data = pd.read_csv(bytes_file)
  else:
    new_revenue_data = pd.read_excel(bytes_file, sheet=0)

  ## read un current master csv in datalake
  filemap = Filemap('RADY')
  cr_master = pd.read_csv(os.path.join(filemap.CURATED, "RADY_Digital", "RADY-Programmatic-Conversion-Revenue.csv"))
  cr_master_archive = cr_master.copy() # for archiving purposes

  ## Ensure similar formatting for master and new data
  cr_master['Conversion Time'] = pd.to_datetime(cr_master['Conversion Time'],
     # format='%Y-%m-%dT%H:%M:%S%z',  # e.g., 2025-12-03T22:09:45+00:00
     # infer_datetime_format=True,
      errors='coerce'
  )
  new_revenue_data['Conversion Time'] = pd.to_datetime(new_revenue_data['Conversion Time'],
      #format='%Y-%m-%dT%H:%M:%S%z',  # e.g., 2025-12-03T22:09:45+00:00
      #infer_datetime_format=True,
      errors='coerce'
  )

  cr_master['_source'] = 'master'  # Add a source flag
  new_revenue_data['_source'] = 'new'

  new_revenue_data['_file_created'] = latest_file['created'] # add 3rd failsafe for deduping (date created)
  cr_master['_file_created'] = pd.NaT

  # all_cols = sorted(set(cr_master.columns) | set(new_revenue_data.columns)) # align schema
  # cr_master = cr_master.reindex(columns=all_cols)
  # new_revenue_data = new_revenue_data.reindex(columns=all_cols)


  ## Appending latest data to master
  cr_master_updated = pd.concat([cr_master, new_revenue_data], ignore_index=True)
  cr_master_updated['_source'] = pd.Categorical(cr_master_updated['_source'], categories=['master', 'new'], ordered=True) # convert to category col
  
  sort_keys = ['Conversion Time', '_source', '_file_created']
  cr_master_updated = cr_master_updated.sort_values(by=sort_keys, ascending=[True, True, True], na_position='first') # add third true if adding in date last modified condition

  ## Deduplicating based on Conversion ID and recency
  cr_master_updated = (
      cr_master_updated.drop_duplicates(subset='Conversion ID', keep='last')
              .drop(columns=['_source', '_file_created'])
              .reset_index(drop=True)
  )


  ## Exporting to datalake
  out_path = os.path.join(filemap.CURATED, "RADY_Digital", "RADY-Programmatic-Conversion-Revenue.csv")
  cr_master_updated.to_csv(out_path, float_format="%.4f", index=False) # Exporting updated master
  cr_master_archive.to_csv(os.path.join(filemap.CURATED, "RADY_Digital", "Archive", "RADY-Programmatic-Conversion-Revenue-ARCHIVE.csv"), float_format="%.4f", index=False) # Archiving old master
  print(f"RADY Programmatic ads data and Revenue data transferred to datalake: {out_path}")
    

# COMMAND ----------

# DBTITLE 1,TCI
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


  # --- Get GiftFiscal ------------------------------------------------------------
  schema = get_schema('TCI')
  fy_month = schema["firstMonthFiscalYear"] 
  df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', fy_month)

  # --- Align SourceCode to Package -----------------------------------------------
  df['SourceCode'] = df['Package']
  print("SourceCode column built based on Package")

  # --- Initialize SC Determination column ----------------------------------------
  df['SC Determination'] = 'TRX File Raw'

  # --- Additional SourceCode logic updates ---------------------------------------
  print("Applying additional SourceCode logic updates...")

  # 1) Fundraise Up or blank/None SourceCode
  df = df.reset_index(drop=True)
  mask = (
      df['GiftSource'].eq('Fundraise Up') &
      (
          df['SourceCode'].isna() |
          (df['SourceCode'] == '') |
          (df['SourceCode'].astype(str).str.lower() == 'none')
      )
  )

  df.loc[mask, 'SourceCode'] = (
      (df['GiftFiscal'].fillna('').astype(str) + df['AppealID'].fillna('') + df['GiftSource'].fillna(''))
      .str.upper()
      .str.replace(' ', '', regex=False)
  )
  df.loc[mask, 'SC Determination'] = 'Fundraise Up Logic'
  print(f"Fundraise Up logic applied to {mask.sum():,} rows where SourceCode was blank, null, or 'None'.")

  # 2) Blank/None SourceCode(Package) (not Fundraise Up)
  mask_mid = (
      (
          df['SourceCode'].isna() |
          (df['SourceCode'] == '') |
          (df['SourceCode'].astype(str).str.lower() == 'none')
      ) &
      (~df['GiftSource'].eq('Fundraise Up'))
  )

  df.loc[mask_mid, 'SourceCode'] = (
      (df['GiftFiscal'].fillna('').astype(str) + df['AppealID'].fillna(''))
      .str.upper()
      .str.replace(' ', '', regex=False)
  )
  df.loc[mask_mid, 'SC Determination'] = 'Blank SourceCode(Package) Logic'
  print(f"Blank/None SourceCode(Package) logic applied to {mask_mid.sum():,} rows where GiftSource != 'Fundraise Up'.")

  # 3) Short SourceCode(Package) logic (1Q / 1A prefixes, len < 9)
  df = df.reset_index(drop=True)
  mask2 = (
      df['SourceCode'].notnull() &
      (df['SourceCode'].str.len() < 9) &
      (df['SourceCode'].str.startswith(('1Q', '1A')))
  )

  df.loc[mask2, 'SourceCode'] = (
      (df['GiftFiscal'].fillna('').astype(str).str.strip() + 
      df['AppealID'].fillna('').astype(str).str.strip())
      .str.upper()
      .str.replace(' ', '', regex=False)
  )
  df.loc[mask2, 'SC Determination'] = 'Short SourceCode(Package) Logic'
  print(f"Short SourceCode(Package) logic applied to {mask2.sum():,} rows (SourceCode(Package) starts with '1Q' or '1A' and len < 9).")


  # 4) Fuse Campaign logic (SourceCode populated, len = 9, starts with 1Q or 1A)
  mask3 = (
      df['SourceCode'].notna() &
      (df['SourceCode'].str.len() == 9) &
      (df['SourceCode'].str.startswith(('1Q', '1A')))
  )

  df.loc[mask3, 'SC Determination'] = 'Fuse Campaign'
  print(f"Fuse Campaign logic applied to {mask3.sum():,} rows (SourceCode populated, len = 9, starts with '1Q' or '1A').")

  print("Additional SourceCode(Package) logic updates complete.")
  
  #-----------------------------------------------------------------------------
  # --- Add CampaignCode for Fuse Campaigns -------------------------------------
  mask_fuse_campaign = df['SC Determination'].eq('Fuse Campaign')
  if mask_fuse_campaign.any():
      df.loc[mask_fuse_campaign, 'CampaignCode'] = df.loc[mask_fuse_campaign, 'SourceCode'].str[:5]
      print(f"CampaignCode column added for {mask_fuse_campaign.sum():,} Fuse Campaign rows based on first 5 characters of SourceCode.")
  else:
      print("No Fuse Campaign rows found — skipping CampaignCode assignment.")

  #Create and Combine State and City columns
  df["State"] = df["Preferred State Legacy"].fillna(df["Mailing State/Province"])
  print("State column added from combined Preferred State Legacy and Mailing State/Province")
  df["City"] = df["Preferred City Legacy"].fillna(df["Mailing City"])
  print("City column added from combined Preferred State Legacy and Mailing State/Province")

  print('TCI client specific complete')

  #Creating Appeal Category from Appeal ID
  df['AppealCategory'] = df['AppealID']

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

# DBTITLE 1,TLF
#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def TLF(df): #Changes made on 9/13/24
  print('inside TLF client specific')
  print(df.columns)
  #Update Source Codes
  if 'GiftAmount' in df.columns:

    # #Update to string types
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

  





# COMMAND ----------

# DBTITLE 1,USO
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



def transfer_cost_data(cost_data_url):
    import os  # Import os at the top of the file
    import json
    import pandas as pd
    import io
    from office365.sharepoint.files.file import File
    try:
        # SharePoint settings
        SHAREPOINT_URL_USO = "https://mindsetdirect.sharepoint.com"
        filemap_sp = Filemap('Fuse')
        with open(os.path.join(filemap_sp.SCHEMA, 'schema.json'), 'r') as f:
            USER = json.load(f)['SharePointUID']

        # initiate client context (transfer_files)
        client_context_USO = get_sharepoint_context_app_only(SHAREPOINT_URL_USO)
        print("Get Sharepoint contect - Confirmed")

        # get site contents (transfer_files)
        client_files = []
        client_files = get_all_contents(client_context_USO, cost_data_url)
        print("Contents of directory retrieved")

        # READ IN DFS FROM client_files
        from office365.sharepoint.files.file import File
        import pandas as pd
        import io
        import os

        dfs = {}  # dictionary to hold all dataframes

        for rel_url in client_files:
            # download file from SharePoint
            response = File.open_binary(client_context_USO, rel_url)
            bytes_file = io.BytesIO(response.content)

            # figure out file extension
            ext = os.path.splitext(rel_url)[1].lower()

            if ext in [".xlsx", ".xls"]:
                df = pd.read_excel(bytes_file)
            elif ext == ".csv":
                df = pd.read_csv(bytes_file)  # add delimiter="|" if some CSVs are pipe-delimited
            else:
                print(f"Skipping unsupported file type: {rel_url}")
                continue

            # use the filename (without path) as dictionary key
            filename = os.path.basename(rel_url)
            dfs[filename] = df

        # Example: loop through all results
        for name, df in dfs.items():
            print(f"\n{name} -> {df.shape[0]} rows, {df.shape[1]} cols")
            print(df.head())
        #dfs['USO Campaign Costs.xlsx']

        # Transfer to datalake
        uso_cost = dfs['USO Campaign Costs.xlsx']
        uso_cost.to_csv(os.path.join(filemap.MASTER, "USO Campaign Costs.csv"))
        print("Cost data transferred to datalake")

    except Exception as e:
        print(f"Error: {e}")
        pass

# COMMAND ----------

# DBTITLE 1,U4U
#Called in apply_client_specific(df, client) to apply client specific columns when building data.parquet
def U4U(df):
  df['SourceCode'] = df['Package ID']
  print("SourceCode column created to align with Package ID")
  # TO DO: If source code has 2 campaign codes under the same source code, add a 1, 2, etc. to the campaign code (ie if one source code is associated with 2 or more appeal IDs, add identifiers at the end)

  df['CampaignCode'] = ''
  print('Empty CampaignCode column created')

  ########## Dealing with Source Codes with more than 1 Appeal ID

  # Step 1: Identify Source Codes associated with more than 1 Appeal ID

  sc_appeal_counts = df.groupby(['SourceCode', 'Appeal ID']).size().reset_index(name='count')
  print("Source Code distribution across Appeal IDs:")
  print(sc_appeal_counts.head(10))

  # Find Source Codes that appear in multiple appeal IDs
  sc_appeal_summary = sc_appeal_counts.groupby('SourceCode')['Appeal ID'].nunique().reset_index()
  sc_appeal_summary.columns = ['SourceCode', 'num_appeal_ids']

  # Show problematic Source Codes
  problematic_sc = sc_appeal_summary[sc_appeal_summary['num_appeal_ids'] > 1]
  print(f"\n Source Codes appearing in multiple Appeal IDs: {len(problematic_sc)}")
  print(problematic_sc.head(10))

  # Show details for problematic Source Codes
  if len(problematic_sc) > 0:
      print("\nDetailed view of problematic packages:")
      for sc in problematic_sc['SourceCode'].head(5):  # Show first 5 examples
          appeals = df[df['SourceCode'] == sc]['Appeal ID'].unique()
          print(f"Package {sc} appears in Appeal IDs: {appeals}")

  #Step 2: Fix by adding suffixes

  # Create a copy to work with
  df_fixed = df.copy()

  # Find Source Codes that appear in multiple appeal IDs
  sc_counts = df.groupby('SourceCode')['Appeal ID'].nunique()
  multi_appeal_sc = sc_counts[sc_counts > 1].index

  print(f"Found {len(multi_appeal_sc)} Source Codes in multiple appeal IDs")

  # For each problematic Source Codes, add suffixes
  for sc in multi_appeal_sc:
      # Get all rows with this Source Code
      mask = df_fixed['SourceCode'] == sc
      
      # Get unique appeal IDs for this Source Code
      appeal_ids = df_fixed[mask]['Appeal ID'].unique()
      
      # Keep the first appeal ID as-is, add suffixes to others
      for i, appeal_id in enumerate(appeal_ids):
          if i == 0:
              continue  # Keep first one unchanged
          else:
              # Add suffix to this appeal ID's Source Codes
              appeal_mask = mask & (df_fixed['Appeal ID'] == appeal_id)
              df_fixed.loc[appeal_mask, 'SourceCode'] = f"{sc}_{i+1}"
  print("Source Codes fixed and appended with necessary suffixes")


  

  df = df_fixed.copy()

    ##########

#---------------Add Gift Channel for U4U from Campaign ID--------------------------------
  import numpy as np

  # -----------------------
  # Normalize Campaign ID
  # -----------------------
  cid = (
      df["Campaign ID"]
        .astype(str)
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "", regex=True)
  )

  # -----------------------
  # Keyword groups (normalized)
  # -----------------------
  direct_mail_keys = ["directmail", "mailacquisition", "mailretention"]

  digital_keys = [
      "digitalacquisition", "digitalads", "digitalretention",
      "email", "website"
  ]

  other_keys = [
      "event", "hightouch", "sustainerexisting", "whitemail",
      "otherretention", "majorgiving", "boardgiveget",
      "legacygiving", "thirdparty"
  ]

  direct_pat = "|".join(direct_mail_keys)
  digital_pat = "|".join(digital_keys)
  other_pat  = "|".join(other_keys)

  # -----------------------
  # Appeal ID override list (FULL)
  # If Appeal ID is in this set, force Direct Mail
  # -----------------------

  import os
  import io
  import json
  import pandas as pd
  from office365.sharepoint.files.file import File
  
  SHAREPOINT_URL_U4U = "https://mindsetdirect.sharepoint.com/sites/ClientFiles" # DM Appeal ID directory, change as necessary (if DM Appeal ID doc moves)
  filemap_sp = Filemap('Fuse')
  with open(os.path.join(filemap_sp.SCHEMA, 'schema.json'), 'r') as f:
      USER = json.load(f)['SharePointUID']

  client_context_U4U = get_sharepoint_context_app_only(SHAREPOINT_URL_U4U)

  # Get DM Appeal ID file
  files = get_folder_files(client_context_U4U, 'U4U/Reporting 2.0') # Get folder
  dm_appeal_file = next(f for f in files if os.path.basename(f).startswith("DM Appeal IDs") and f.lower().endswith(('.xlsx', '.xls'))) # Read in DM Appeal ID
  response = File.open_binary(client_context_U4U, dm_appeal_file)
  bytes_file = io.BytesIO(response.content)
  dm_appeal_df = pd.read_excel(bytes_file)
  direct_mail_appeals = set(dm_appeal_df.iloc[:, 0].dropna()) # get unique list of Appeal IDs from first column of df

  # Create Appeal ID column
  appeal_is_direct = df["Appeal ID"].isin(direct_mail_appeals) # Assign boolean value if in direct_mail_appeals list

  # -----------------------
  # Final GiftChannel logic
  # -----------------------
  df["GiftChannel"] = np.select(
      [
          appeal_is_direct,                          # override FIRST
          cid.str.contains(direct_pat, na=False),
          cid.str.contains(digital_pat, na=False),
          cid.str.contains(other_pat, na=False),
      ],
      ["Direct Mail", "Direct Mail", "Digital", "Other"],
      default="N/A"
  )


  print("GiftChannel Added")

  df['gift_channel_subtype'] = df['Campaign ID']
  print('gift_channel_subtype added from Campaign ID')
  df['gift_channel_detail'] = df['Appeal ID']
  print('gift_channel_detail added from Appeal ID')

  print('U4U client specific function complete')
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

# DBTITLE 1,WFP
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
  print("WFP_DisasterGift complete")
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
  print("WFP_DisasterJoin complete")

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
  print("WFP_DonorChannelGroup complete")
  return df

#FH Filter
def WFP_Donor0014T000004x9wfQAA(
  df, feature='WFP_Donor0014T000004x9wfQAA'
  ):
  df[feature] = df.DonorID.astype(str) == '0014T000004x9wfQAA'
  print("WFP_Donor0014T000004x9wfQAA complete")

  return df



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
    print("WFP_GiftChannelDetail complete")

    print("WFP_GiftChannelDetail complete")
    return df



# FH Filter
def WFP_GiftChannel(df, feature='WFP_GiftChannel', channel_col='Channel Type'):
    print("\n🔎 Starting WFP_GiftChannel transformation")
    print(f"Total rows: {len(df):,}")

    if channel_col not in df.columns:
        print(f"⚠️ Column '{channel_col}' not found in dataframe")
        return df

    print(f"Unique values in '{channel_col}':")
    print(df[channel_col].dropna().unique())

    # Default to 'Other'
    df[feature] = 'Other'
    print(f"✅ Initialized '{feature}' column to 'Other'")

    # Explicitly prioritize "Direct Mail Online" first
    dm_online_count = (df[channel_col] == 'Direct Mail - Online').sum()
    df.loc[df[channel_col] == 'Direct Mail - Online', feature] = 'DM'
    print(f"📬 Direct Mail - Online mapped to 'DM' | Rows affected: {dm_online_count:,}")

    # Define types and update Gift Channel based on Channel Type
    dm_types = ['Direct Mail', 'Individual Philanthropy Offline'] # DM
    dm_count = df[channel_col].isin(dm_types).sum()
    df.loc[df[channel_col].isin(dm_types), feature] = 'DM'
    print(f"📬 DM types mapped to 'DM' | Rows affected: {dm_count:,}")

    digital_types = [ 
        'Ads - Other', 'Advocacy', 'DRTV', 'Email', 'Facebook Ads',
        'Individual Philanthropy Online', 'IP Ads - Other', 'IP Facebook',
        'Organic Social', 'Search Ads', 'Share-The-Meal', 'SMS',
        'WFP Redirect', 'WFP USA Web', 'Referral', 'IP Search Ads'
    ]
    digital_count = df[channel_col].isin(digital_types).sum() # Digital
    df.loc[df[channel_col].isin(digital_types), feature] = 'Digital'
    print(f"💻 Digital types mapped to 'Digital' | Rows affected: {digital_count:,}")

    canvassing_types = ['Face-to-Face'] # Canvassing
    canvassing_count = df[channel_col].isin(canvassing_types).sum()
    df.loc[df[channel_col].isin(canvassing_types), feature] = 'Canvassing'
    print(f"🤝 Canvassing types mapped to 'Canvassing' | Rows affected: {canvassing_count:,}")

    ipo_types = ['Individual Philanthropy Offline'] # Individual Philanthropy Offline
    ipo_count = df[channel_col].isin(ipo_types).sum()
    df.loc[df[channel_col].isin(ipo_types), feature] = 'Individual Philanthropy Offline'
    print(f"$ Individual Philanthropy Offline types mapped to 'Individual Philanthropy Offline' | Rows affected: {ipo_count:,}")

    print("\n📊 Final distribution of WFP_GiftChannel:")
    print(df[feature].value_counts(dropna=False))

    print("✅ WFP_GiftChannel transformation complete\n")

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
    print("WFP_GiftChannelSubtype complete")
    return df



#FH Filter
def WFP_JoinChannel(
    df, feature='WFP_JoinChannel', channel_col='WFP_GiftChannel',
    sort_values=['DonorID', 'GiftDate'], dup_value='DonorID'
):
    _df = df.sort_values(sort_values).drop_duplicates(dup_value, keep='first')
    _df[feature] = _df[channel_col]

    cols = [dup_value, feature]
    df = pd.merge(df, _df[cols], on=dup_value, how='left')
    print("WFP_JoinChannel complete")
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
    print("WFP_JoinChannelDetail complete")
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
    print("WFP_JoinChannelSubtype complete")
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
  print("WFP_LegacyGiftChannel complete")
  return df

#FH Filter
def WFP_RecurringGift(df, feature='WFP_RecurringGift'):
  df[feature] = df['Recurring Gift ID'].notna()
  print('WFP_RecurringGift complete')
  return df

#FH Filter
def WFP_Sustainer(df, feature='WFP_Sustainer'):
  sustainers = df.loc[df.WFP_RecurringGift, 'DonorID'].unique()
  df[feature] = df.DonorID.isin(sustainers)
  print("WFP_Sustainer complete")
  return df

def WFP_SustainerStatus(df, feature="WFP_SustainerStatus"):
  import os
  import io
  import json
  import pandas as pd
  from office365.sharepoint.files.file import File

  # SharePoint Setup
  SHAREPOINT_URL_WFP = "https://mindsetdirect.sharepoint.com/sites/ClientDataShare"

  filemap_sp = Filemap('Fuse')
  with open(os.path.join(filemap_sp.SCHEMA, 'schema.json'), 'r') as f:
      USER = json.load(f)['SharePointUID']

  client_context_WFP = get_sharepoint_context_app_only(SHAREPOINT_URL_WFP)

  # Get all files in contact report folder
  files = get_folder_files(client_context_WFP, 'Shared Documents/WFP/Transaction Files/Contact Reports')

  # Get latest contact report

  file_info_list = []

  for file_path in files:
      file_obj = client_context_WFP.web.get_file_by_server_relative_url(file_path)
      client_context_WFP.load(file_obj)
      client_context_WFP.execute_query()

      file_info_list.append({
          "path": file_path,
          "name": file_obj.properties["Name"],
          "modified": pd.to_datetime(file_obj.properties["TimeLastModified"]),
          "created": pd.to_datetime(file_obj.properties["TimeCreated"]),
          "size": int(file_obj.properties.get("Length", 0))
      })

  # Get and read in most recent contact report
  # latest_cr = max(file_info_list, key=lambda x: x["created"]) # Most recently modified contact report metadata
  latest_cr = max(file_info_list, key=lambda x: x["created"]) # Most recently modified --> Note: This is a placeholder for now. Change back when next report comes (Feb2026)

  response = File.open_binary(client_context_WFP, latest_cr['path']) # Reading in report
  bytes_file = io.BytesIO(response.content)

  if latest_cr['name'].lower().endswith(".xlsx"):
    contact_report_data = pd.read_excel(bytes_file, sheet_name=0)
  else:
    contact_report_data = pd.read_csv(bytes_file)

  # Cleaning
  contact_report_data = contact_report_data.rename(columns={'Account ID': 'DonorID'}) # Renaming account id to donor id (for merging)
  contact_report_data = contact_report_data[['DonorID','Sustainer Status']] # dropping all cols besides donor id and sustainer status

  # Merging onto df
  df = pd.merge(df, contact_report_data, on='DonorID', how='left')
  df = df.rename(columns={'Sustainer Status': feature})
  print('WFP_SustainerStatus complete')
  return df


#FH Filter
def WFP_UkraineDonor(df, feature='WFP_UkraineDonor'):
  import datetime
  _df = df.groupby('DonorID').GiftDate.min().to_frame(name='FirstGiftDate').reset_index()
  _df[feature] = (_df.FirstGiftDate >= datetime.datetime(2022,2,24)) & (_df.FirstGiftDate <= datetime.datetime(2022,5,31))
  cols = ['DonorID', feature]
  print("WFP_UkraineDonor complete")
  return pd.merge(df, _df[cols], on=cols[0], how='left')




# COMMAND ----------

# MAGIC %md ##Legacy clients / RFPs

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

# DBTITLE 1,SierraClub RFP
def SierraClub(df):
  print("Client Specific Functions Started")

  print("Client Specific Function Ended")
  return df

def _read_csv_robust(fp, encoding):
    # Try a few increasingly-forgiving parses
    attempts = [
        dict(engine="python", sep=None),  # auto-detect delimiter
        dict(engine="python", sep=None, encoding="utf-8-sig"),  # handle BOM
        dict(engine="python", sep=",", quotechar='"', doublequote=True, escapechar="\\"),
        dict(engine="python", sep=",", on_bad_lines="skip"),  # skip malformed lines
    ]
    last_err = None
    for i, opts in enumerate(attempts, 1):
        try:
            df = pd.read_csv(fp, encoding=encoding, **opts)
            print(f"  ✓ CSV parsed with attempt #{i}: {opts}")
            return df
        except Exception as e:
            last_err = e
            print(f"  ✗ Attempt #{i} failed: {e}")
            continue
    # Final hail-mary: replace undecodable bytes and try again
    try:
        with open(fp, "r", encoding=encoding, errors="replace") as f:
            df = pd.read_csv(f, engine="python", sep=None)
            print("  ✓ CSV parsed with errors='replace'")
            return df
    except Exception as e:
        raise e if last_err is None else last_err




# COMMAND ----------

# MAGIC %md ##File management

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
    cf = pd.concat([cf, pd.DataFrame({"FileName": client_files})], ignore_index=True)
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

  
def collect_transaction_files(folder, columns, encoding, key_mapper, date_format, schema, directory='Data'):  
    print("collect_transaction_files started")
    dfs = []  # Initialize an empty list to store individual DataFrames
    path = os.path.join(folder, directory)  # Construct the full path to the target directory

    # Use schema-defined delimiter if present, otherwise default to ","
    delimiter = schema.get("CSV_Delimeter", ",")
    print(f"Using CSV delimiter: {repr(delimiter)}")

    for f in os.listdir(path):  # Iterate over each file in the specified directory
        if ".csv" in f.lower() or "xls" in f.lower():  # Check if the file is a CSV or Excel file
            print(f"→ Processing: {f}")
            try:
                if ".csv" in f.lower():
                    # Count raw lines first
                    file_path = os.path.join(path, f)
                    with open(file_path, encoding=encoding, errors="ignore") as fh:
                        total_lines = sum(1 for _ in fh) - 1  # subtract 1 for header

                    # Read CSV with bad lines skipped
                    _df = pd.read_csv(
                        file_path,
                        encoding=encoding,
                        sep=delimiter,
                        engine="python",  
                        error_bad_lines=False  # skip bad lines (deprecated in >=1.3, use on_bad_lines="skip")
                    )

                    skipped_lines = total_lines - len(_df)
                    if skipped_lines > 0:
                        print(f"⚠️ Skipped {skipped_lines} malformed lines in {f}")

                elif ".xls" in f.lower():
                    _df = pd.read_excel(os.path.join(path, f))  # Read the Excel file

                # Rename _df columns based on the client KeyMapper in schema
                _df = _df.rename(columns=key_mapper)

                dfs.append(_df)  # Append _df to the list of DataFrame objects
            except Exception as e:
                print(f"Error processing {f}: {str(e)}")
                continue

    if dfs:
        df = pd.concat(dfs, axis=0, ignore_index=True)  # Concatenate all the DFs in the list
    else:
        df = pd.DataFrame()  # Create an empty DF if no valid files were processed

    # If specific columns are provided, filter the DF
    if columns and not df.empty:
        df = df[[c for c in columns if c in df.columns]]

    # Convert GiftDate to datetime format
    if "GiftDate" in df.columns:
        df["GiftDate"] = pd.to_datetime(df["GiftDate"], format=date_format, errors="coerce")

    # Fix BOM variant if present
    if 'ï»¿"Gift Id"' in df.columns:
        df.rename(columns={'ï»¿"Gift Id"': 'GiftID'}, inplace=True)
        print('Renamed ï»¿"Gift Id" to GiftID from transaction files load in')

    # Diagnostics
    total_row_count = len(df)
    print(f"Total number of rows in the DataFrame: {total_row_count}")
    if {"GiftID", "DonorID"}.issubset(df.columns):
        duplicate_count = df.duplicated(subset=["GiftID", "DonorID"]).sum()
        print(f"Number of duplicate rows based on GiftID and DonorID: {duplicate_count}")

    print("collect_transaction_files finished")
    return df
  



# COMMAND ----------

# DBTITLE 1,Custom File Management
