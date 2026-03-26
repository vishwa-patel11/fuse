# Databricks notebook source
# MAGIC %md # source_code_validation (production functions)

# COMMAND ----------

# DBTITLE 1,imports
import calendar

# COMMAND ----------

# DBTITLE 1,FFB_helpers
# MAGIC %run ../FFB/FFB_helpers

# COMMAND ----------

# DBTITLE 1,parser
# MAGIC %run ./parser

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

# DBTITLE 1,mount_datalake
# COMMAND ----------

# DBTITLE 1,Global Functions
# globals
def _get_client_fn(client, fn_suffix):
    """Get client-specific function from clients.{client}.source_code or globals."""
    try:
        from common.clients_loader import get_client_fn
        fn = get_client_fn(client, fn_suffix)
        if fn is not None:
            return fn
    except ImportError:
        pass
    return globals().get(f"{client}_{fn_suffix}")


def fix_client_codes(df, client):
    h = f"{client}_initiate_codes"
    print(f"🔎 fix_client_codes: looking for function '{h}'")

    fn = _get_client_fn(client, "initiate_codes")
    if fn is None:
        print(f"⚠️ fix_client_codes: '{h}' not found — returning df unchanged")
        return df

    try:
        print(f"✅ fix_client_codes: dispatching to {fn.__name__}")
        try:
            return fn(df, client)
        except TypeError:
            return fn(df)
    except Exception as e:
        print(f"❌ fix_client_codes: error inside {h}: {type(e).__name__}: {e}")
        return df


def update_client_codes(df, client):
    h = f"{client}_update_codes"
    print(f"🔎 update_client_codes: looking for function '{h}'")

    fn = _get_client_fn(client, "update_codes")
    if fn is None:
        print(f"⚠️ update_client_codes: '{h}' not found — returning df unchanged")
        return df

    try:
        print(f"✅ update_client_codes: dispatching to {fn.__name__}")
        try:
            return fn(df, client)
        except TypeError:
            return fn(df)
    except Exception as e:
        print(f"❌ update_client_codes: error inside {h}: {type(e).__name__}: {e}")
        return df


def create_campaign_names(df, client):
    h = f"{client}_create_names"
    print(f"🔎 create_campaign_names: looking for function '{h}'")

    fn = _get_client_fn(client, "create_names")
    if fn is None:
        print(f"⚠️ create_campaign_names: '{h}' not found — returning df unchanged")
        return df

    try:
        print(f"✅ create_campaign_names: dispatching to {fn.__name__}")
        try:
            return fn(df, client)
        except TypeError:
            return fn(df)
    except Exception as e:
        print(f"❌ create_campaign_names: error inside {h}: {type(e).__name__}: {e}")
        return df


def add_dimension_filters(df, client):
    h = f"{client}_add_filters"
    print(f"🔎 add_dimension_filters: looking for function '{h}'")

    fn = _get_client_fn(client, "add_filters")
    if fn is None:
        print(f"⚠️ add_dimension_filters: '{h}' not found — returning df unchanged")
        return df

    try:
        print(f"✅ add_dimension_filters: dispatching to {fn.__name__}")
        try:
            return fn(df, client)
        except TypeError:
            return fn(df)
    except Exception as e:
        print(f"❌ add_dimension_filters: error inside {h}: {type(e).__name__}: {e}")
        return df


def add_dataset_filters(df, client):
    h = f"{client}_dataset_filters"
    print(f"🔎 add_dataset_filters: looking for function '{h}'")

    fn = _get_client_fn(client, "dataset_filters")
    if fn is None:
        print(f"⚠️ add_dataset_filters: '{h}' not found — returning df unchanged")
        return df

    try:
        print(f"✅ add_dataset_filters: dispatching to {fn.__name__}")
        try:
            return fn(df, client)
        except TypeError:
            return fn(df)
    except Exception as e:
        print(f"❌ add_dataset_filters: error inside {h}: {type(e).__name__}: {e}")
        return df


def add_campperf_dfc_updates(df, client):
    h = f"{client}_campperf_dfc_updates"
    print(f"🔎 add_campperf_dfc_updates: looking for function '{h}'")

    fn = _get_client_fn(client, "campperf_dfc_updates")
    if fn is None:
        print(f"⚠️ add_campperf_dfc_updates: '{h}' not found — returning df unchanged")
        return df

    try:
        print(f"✅ add_campperf_dfc_updates: dispatching to {fn.__name__}")
        try:
            return fn(df, client)
        except TypeError:
            return fn(df)
    except Exception as e:
        print(f"❌ add_campperf_dfc_updates: error inside {h}: {type(e).__name__}: {e}")
        return df


def add_raw_sc_updates(df, client):
    h = f"{client}_add_raw_sc_updates"
    print(f"🔎 add_raw_sc_updates: looking for function '{h}'")

    fn = _get_client_fn(client, "add_raw_sc_updates")
    if fn is None:
        print(f"⚠️ add_raw_sc_updates: '{h}' not found — returning df unchanged")
        return df

    try:
        print(f"✅ add_raw_sc_updates: dispatching to {fn.__name__}")
        try:
            return fn(df, client)
        except TypeError:
            return fn(df)
    except Exception as e:
        print(f"❌ add_raw_sc_updates: error inside {h}: {type(e).__name__}: {e}")
        return df


# COMMAND ----------

# DBTITLE 1,All Client Filter Functions
def Fuse_Campaign_Lookup(df,client):
  # Load list of unique source codes
  sc = load_sc(client)

  # Check if `SourceCode` in `df` exists in `sc['SourceCode']` and add new column
  df["fuse_tracked_campaign"] = df["CampaignCode"].isin(sc["CampaignCode"])
  print("fuse_tracked_campaign column added")
  return df

# COMMAND ----------

# MAGIC %md # Client Specific

# COMMAND ----------

# DBTITLE 1,AFHU
# AFHU_* moved to clients/AFHU/source_code.py

# COMMAND ----------

# DBTITLE 1,AH
def AH_helper(df):
  col_name = 'SourceCode'
  if col_name in df.columns: #
    df = df.rename(columns={col_name: 'PackageID'})
    
  df.PackageID = df.PackageID.str.strip().str.upper()
  cond = df.PackageID.str.len() > 4
  df['LastChar'] = df.PackageID.str[-1]
  df['_PackageID'] = np.where(cond, '___' + df.LastChar, df.PackageID)
  df['ListCode'] = np.where(cond, df.PackageID.str[1:4], None)
  
  df.CampaignCode = df.CampaignCode.str.strip()
  df.PackageID = df.PackageID.str.strip()
  df['_SourceCode'] = df.CampaignCode.fillna(value='X'*10) + df.PackageID.fillna(value='Y'*5)
  
  cond = (df.CampaignCode == df.PackageID) #& (df.CampiagnCode != '')
  df['SourceCode'] = np.where(cond, df.CampaignCode+'YYYY', df._SourceCode)
  
  df = df.drop_duplicates('SourceCode', keep='last')
  return df

def AH_initiate_codes(df):
  return AH_helper(df)

def AH_update_codes(df): #update_client_codes(df, client)
  mask = df.CampaignCode == ''
  return AH_helper(df.loc[~mask])

# COMMAND ----------

# DBTITLE 1,CARE

def CARE_update_codes(df): #update_client_codes(df, client)
    mask = df.SourceCode.notna()
    df = df.loc[mask]
  
    df.SourceCode = df.SourceCode.astype(str)
    df.SourceCode = df.SourceCode.str.replace(' ','')
    mask = df.SourceCode.str.contains('nan').fillna(value=False)
    df = df.loc[~mask]
  
    df['CampaignCode'] = df.SourceCode.str[:6]
    if 'RFMCode' in df.columns:
        df['RFMCode'] = df['RFMCode'].fillna(value='').astype(str)
  
    df.SourceCode = df.SourceCode.str.replace(' ','')
    df['LastChar'] = df.SourceCode.str[-1]
    
    # Use np.select to choose between different scenarios for 'SynthSC' calculation
    conditions = [
        df['CampaignCode'].str.startswith('1924M'),
        df['CampaignCode'].str.startswith('1324M')
    ]
    choices = [
        df.SourceCode.str[:7] + df.LastChar + '0',  # Position 8 for '1924M' and '1324M'
        df.SourceCode.str[:7] + df.LastChar + '0'   # Same as above, included for clarity
    ]
    default_choice = df.SourceCode.str[:10] + df.LastChar + '0'  # Default case, position 11
    
    df['SynthSC'] = np.select(conditions, choices, default=default_choice)
    
    df['SourceCode'] = np.where(
        df.SourceCode.str.endswith('0'), 
        df.SourceCode, 
        df.SynthSC
    )
    df = df.dropna(axis=0, subset=['SourceCode']) 
    df = df.drop(['SynthSC', 'LastChar'], axis=1)

    # Omit / dedupe gifts with same Gift ID and opposite gift amount (e.g. 100 and -100)
    

  
    print('end client specific')
    return df

def _CARE_SC_ChannelBehavior(df):
  old_d = {
    '1': 'DM Only',
    '2': 'Digital Only',
    '3': 'DM and Digital',
    '4': 'Other'
  }
  new_d = {
    '1': 'DM Only',
    '2': 'Digital Only',
    '3': 'DM and Other',
    '4': 'Other',
    '5': 'TM Only',
    '6': 'Digital and Other (no DM)'
  }
  df['SC_Channel_Behavior'] = np.where(
    df.SourceCode.str[2:4].astype(int) < 24 , 
    df.SourceCode.str[6].map(old_d),  
    df.SourceCode.str[6].map(new_d)
    )
  return df

def CARE_Segment_Filter(df, feature='CARE_Segment', package_col='PackageName'):
    """
    Applies a CARE-specific segment filter based on the 'PackageName' field to classify records into specific segments
    by checking if the segment keywords are included as substrings.

    Parameters:
    - df: The dataframe containing the data.
    - feature: The name of the new column to be created with segment classifications (default: 'CARE_Segment').
    - package_col: The column name containing the package names (default: 'PackageName').

    Returns:
    - df: The updated dataframe with the new CARE segment classification column.
    """
    # Default to 'Other'
    df[feature] = 'Other'

    # Map segments to keywords
    segment_mapping = {
        'Sustainer': 'Sustainer',
        'High Value': 'High Value',
        'Maximize': 'Maximize',
        'Retain': 'Retain',
        'No Gift': 'No Gift',
        'Yes Gift': 'Yes Gift',
        'Landing Page': 'Landing Page',
        'Whitemail': 'Whitemail'
    }

    # Assign segments based on inclusion of keywords
    for segment, keyword in segment_mapping.items():
        df.loc[df[package_col].str.contains(keyword, case=False, na=False), feature] = segment
    print ("CARE_Segment_Filter applied")
    return df


def _CARE_ProgramFilter(df):
  # PC can be identified with source codes starting with 1224, 1924, 1323 or 0823AK. 
  # IC can be identified with source codes starting with 1124 or 1324.

  # initialize columns, set default
  df['Program'] = None
  # Program = PC
  mask = df.CampaignCode.str.startswith(('1224', '1924', '1323', '0823AK'))
  df.loc[mask, 'Program'] = 'PC'
  # Program = IC
  mask = df.CampaignCode.str.startswith(('1124', '1324'))
  df.loc[mask, 'Program'] = 'IC'
  print("CARE program filter added")
  return df

def CARE_add_filters(df):
  print("CARE_add_filters executing")
  print("CARE_SC_ChannelBehavior Started")
  df = _CARE_SC_ChannelBehavior(df)
  print("CARE_SC_ChannelBehavior Complete")

  print("CARE_Segment started")
  if "PackageName" in df.columns:
    feature = "segment"
    package_col = 'PackageName'
    # Default to 'Other'
    df[feature] = 'Other'
    # Map segments to keywords
    segment_mapping = {
        'Sustainer': 'Sustainer',
        'High Value': 'High Value',
        'Maximize': 'Maximize',
        'Retain': 'Retain',
        'No Gift': 'No Gift',
        'Yes Gift': 'Yes Gift',
        'Landing Page': 'Landing Page',
        'Whitemail': 'Whitemail'
    }
    # Assign segments based on inclusion of keywords
    for segment, keyword in segment_mapping.items():
        df.loc[df[package_col].str.contains(keyword, case=False, na=False), feature] = segment
    print ("CARE_Segment_Filter_Source_Code applied")

  print("CARE_add_filters complete")  

  return df

def CARE_dataset_filters(df):
  df = _CARE_ProgramFilter(df)
  print('_CARE_ProgramFilter')
  df = CARE_Segment_Filter(df)
  print('CARE_Segment_Filter')
  return df

def CARE_initiate_codes(df):
  df.SourceCode = df.SourceCode.str.replace(' ','')
  mask = df.SourceCode.str.contains('nan').fillna(value=False)
  df = df.loc[~mask]
  cols =['SynthSC', 'LastChar']
  df['LastChar'] = df.SourceCode.str[-1]
  df['SynthSC'] = df.SourceCode.str[:10] + df.LastChar + '0'
  df['SourceCode'] = np.where(
    df.SourceCode.str.endswith('0'), 
    df.SourceCode, 
    df.SynthSC
  )
  df.CampaignCode = df.CampaignCode.astype(str)
  df = df.dropna(axis=0, subset=['SourceCode']) 
  return df.drop(cols, axis=1)



# COMMAND ----------

# DBTITLE 1,CHOA
def _CHOA_ChannelFilter(df):
  # For example in the Appeal ID it would look something like 
  # If it starts with DM and ends in an R or E = Renewal
  # If it starts with DM and ends in an L = Lapsed
  # If it starts with DM and ends in an A = ACQ
  # If it starts with SO and ends in an S,R or W = Leadership
  # If it starts with CR and end in T or F = CAT/Aflac
  # If it starts with CRD = CAT/Aflac
  df['Channel'] = None
  mask = ((df.CampaignCode.str.startswith('DM')) & (df.CampaignCode.str.endswith(('R','E')))).fillna(value=False)
  df.loc[mask, 'Channel'] = 'Renewal'
  mask = ((df.CampaignCode.str.startswith('DM')) & (df.CampaignCode.str.endswith('L'))).fillna(value=False)
  df.loc[mask, 'Channel'] = 'Lapsed'
  mask = ((df.CampaignCode.str.startswith('DM')) & (df.CampaignCode.str.endswith('A'))).fillna(value=False)
  df.loc[mask, 'Channel'] = 'ACQ'
  mask = ((df.CampaignCode.str.startswith('SO')) & (df.CampaignCode.str.endswith(('S','R','W')))).fillna(value=False)
  df.loc[mask, 'Channel'] = 'Leadership'
  mask = ((df.CampaignCode.str.startswith('CR')) & (df.CampaignCode.str.endswith(('T','F')))).fillna(value=False)
  df.loc[mask, 'Channel'] = 'CAT/Aflac'
  mask = df.CampaignCode.str.startswith('CRD').fillna(value=False)
  df.loc[mask, 'Channel'] = 'CAT/Aflac'
  return df

def CHOA_add_filters(df):
  df = _CHOA_ChannelFilter(df)
  return df

def CHOA_dataset_filters(df):
  df = GiftFiscal(df, 'CHOA')
  return df

def CHOA_update_codes(df): #update_client_codes(df, client)
  print('inside client specific')
  if "GiftAmount" not in df.columns:
    df = df.rename(columns = {"Quantity":"_Quantity"})
    _df = df.groupby('SourceCode')['_Quantity'].sum().to_frame(name='Quantity')
    df = pd.merge(df, _df, on='SourceCode', how='left')
    df = df.drop_duplicates('SourceCode')
     
    # add XXX for blank RFM codes
    m1 = (df.CampaignCode.isna()) | (df.CampaignCode == '') | (df.CampaignCode == ' ')
    m2 = (df.PackageCode.isna()) | (df.PackageCode == '') | (df.PackageCode == ' ')
    mask = (m1) | (m2)
    df = df.loc[~mask]
    _df = df.drop_duplicates(['CampaignCode', 'PackageCode'])
    _df.loc[:, 'RFMCode'] = 'XXX'
    _df.loc[:, 'SourceCode'] = _df.CampaignCode + _df.PackageCode + _df.RFMCode

    #“CR” and are followed by 4 numeric characters are campaigns we are not a part of
    # create exception for codes starting with CR and followed by 4 numbers
    #if df.SourceCode.startswith('CR') and df.SourceCode(2:5).str.contains(int):
    #  pass 
    
    cols = [
      'SourceCode', 'CampaignCode', 'PackageCode', 
      'RFMCode', 'CampaignName', 'PackageName', 
      'PackageCPP', 'MailDate'
      ]
    df = pd.concat([df, _df[cols]])

    # # landing page records
    # #    create a mapping from appeal id -> mail date
    cols = ['CampaignCode', 'CampaignName', 'MailDate']
    _df = df[cols].drop_duplicates('CampaignCode')
    _df['PackageCode'] = 'LAND'
    _df['RFMCode'] = 'XXX'
    _df['SourceCode'] = _df.CampaignCode + _df.PackageCode + _df.RFMCode
    df = pd.concat([df, _df])

    # _codesToDates = dict(zip
    # #    for every appeal id, create a record with
    # #    Package Code = LAND, RFM Code = XXX
    # #    map in mail date and campaign name
    
  if 'GiftAmount' in df.columns:
    print('inside first gift history conditional')
    df.SourceCode = df.SourceCode.str.ljust(14,'X')
    m = {'DM Acquisition List': 'ListCode'}
    df = df.rename(columns=m)
    print('columns: ', sorted(df.columns))
    print('finished renaming column')
    df.SourceCode = np.where(
      df.AppealID.str.endswith('A'), # conditional
      df.AppealID.astype(str).str.ljust(7, 'Y') + df.PackageID.astype(str).str.ljust(4,'X') + df.ListCode.astype(str).str.ljust(3, 'X'), # when true
      df.SourceCode # when false
      )
    print('finished updating acq source codes')
  df['_SourceCode'] = np.where(
    df.CampaignCode.str[-1] == 'A',
    df.SourceCode.str[:11] + '___' + df.SourceCode.str[11:],
    df.SourceCode + '___'
  )
  cols = ['ListCode', 'SourceCode']
  for col in cols:
    df[col] = df[col].astype("string").str.replace(r"\.0+$", "", regex=True)

  df.SourceCode = df.SourceCode.str.upper()
  if 'GiftAmount' in df.columns:
    mask = df.SourceCode.str[7:11] == 'LAND'
    df.PackageID = np.where(mask, 'LAND', df.PackageID)
  else: 
    mask = df.SourceCode.str[7:11] == 'LAND'
    df.PackageCode = np.where(mask, 'LAND', df.PackageCode) 


  print('finished client specific')
  return df


def CHOA_initiate_codes(df):
  df['_SourceCode'] = np.where(
    df.CampaignCode.str[-1] == 'A',
    df.SourceCode.str[:11] + '___' + df.SourceCode.str[11:],
    df.SourceCode + '___'
  )
  return df


# COMMAND ----------

# DBTITLE 1,DAV2
def DAV2_update_codes(df): #update_client_codes(df, client)

  return df

# COMMAND ----------

# DBTITLE 1,FFB
def FFB_update_codes(df): #update_client_codes(df, client)
    print("Applying updated codes")
    # Check if the first character is 'X', and keep 0:3 if true
    df['RFMCode'] = df['SourceCode'].apply(
    lambda x: (x[0:3] if x[0] == 'X' else (x[1:4] if x[0].isalpha() else x[0:3])) if x is not None else None)
    print("RFM Code Added")

    if "MailDate" in df.columns:
        # Identify rows where SourceCode contains "XXX"/ONL and CampaignName/MailDate is missing
        cn_condition = df['SourceCode'].str.startswith(('XXX', 'ONL')) & df['CampaignName'].isna()
        md_condition = df['SourceCode'].str.startswith(('XXX', 'ONL')) & df['MailDate'].isna()

        # Create a mapping of CampaignCode to CampaignName/MailDate (ignoring missing CampaignName/MailDate)
        campaign_name_mapping = df.dropna(subset=['CampaignName']).set_index('CampaignCode')['CampaignName'].to_dict()
        mail_date_mapping = df.dropna(subset=['MailDate']).set_index('CampaignCode')['MailDate'].to_dict()

        # Update CampaignName for rows where SourceCode contains "XXX"/ONL and CampaignName/MailDate is missing
        df.loc[cn_condition, 'CampaignName'] = df.loc[cn_condition, 'CampaignCode'].map(campaign_name_mapping)
        df.loc[md_condition, 'MailDate'] = df.loc[md_condition, 'CampaignCode'].map(mail_date_mapping)

        print("Campaign Name & Mail Date updated for XXX & ONL source codes")
            

    if "GiftAmount" in df.columns:
        #CREATE PROGRAM FILTER
        # Replace 'nan' and 'None' strings with actual NaN values
        df['Media Outlet Code'].replace(['nan', 'None'], np.nan, inplace=True)
        df['Segment Code'].replace(['nan', 'None'], np.nan, inplace=True)
        df['Project Code'].replace(['nan', 'None'], np.nan, inplace=True)
        # Convert 'Media Outlet Code' to string and strip whitespaces
        df['Media Outlet Code'] = df['Media Outlet Code'].astype(str).str.strip()
        # Remove '.0' suffix from 'Media Outlet Code' using regex
        df["Media Outlet Code"] = df["Media Outlet Code"].astype("string").str.replace(r"\.0$", "", regex=True)
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

        # Apply conditions to create the 'Join Program' column
        df['Program'] = np.select([cond_core, cond_vc], ['CORE', 'VC'], default='Other')
        print("Program Added")

        # Sort by 'Donor ID' and 'Gift Date' to ensure the first gift is at the top for each donor
        df = df.sort_values(by=['DonorID', 'GiftDate'])
        # Get the first gift's FFB_Program for each donor and assign it as FFB_JoinProgram
        df['JoinProgram'] = df.groupby('DonorID')['Program'].transform('first')
        print("Join Program Added")

        #Create Channel Column
        conditions = [
        # First priority: Paid Digital
        (df['Original URL'].str.contains("give|givenow|donatenow|givelegacy|giveobituary|givechoice", na=False)) |
        (df['Segment Code'].str.contains("WEBA50|W50|50YEARS|ONLSPECPAIDADS|ONLSPECWELPSA", na=False)),
        # Second priority: DM
        (df['Media Outlet Code'].isin(["9005", "Direct Marketing"])) & 
        (df['Segment Code'].str.contains("AQ|CR", na=False)) & 
        (~df['Segment Code'].str.contains("ONL", na=False)),
        # Third priority: Other (based on Media Outlet Code)
        (df['Media Outlet Code'].isin(["9005", "Direct Marketing"])),
        # Fourth priority: VC
        ((df['Media Outlet Code'] == "9000") & 
            (df['Segment Code'].str.contains("VC|9500|Mid-Level", na=False))) | 
            (df['Project Code'].str.contains("9500", na=False))]
        
        choices = ['Paid Digital', 'DM', 'Other', 'VC']
        df['Channel'] = np.select(conditions, choices, default='Other')
        print("Channel Added")

        # Create the Fuse_Program Column
        condition = (
            (df['Program'].isin(['CORE', 'VC'])) |
            ((df['Program'] == 'Other') & (df['Channel'] == 'Paid Digital'))
        )
        # Apply the conditions to create the 'Fuse Program' column
        df['Fuse_Program'] = np.where(condition, 'Yes', 'No')
        print("Fuse Program Added")

        print("Program & Channel Filters Applied")
        
        #Apply time mask after program and channel filters are applied
        mask = df['GiftDate'] >= datetime(year-3,1,1)
        df = df.loc[mask]
        print('Time mask applied to FFB')

    print("FFB_update_codes complete")

    return df



def FFB_initiate_codes(df):
  # df.PackageCode = df.PackageCode.str.strip().fillna(value='XXXXXX')
  # df.PackageCode = df.PackageCode.str.strip()
  df.PackageName = df.PackageName.str.strip()
  df['SourceCode'] = _pad_sc(df)
  print('end FFB update codes')
  return df

def FFB_campperf_dfc_updates(df):
    # Update Segment Name if blank
    df['Segment Name'] = df.apply(
        lambda row: f"{row['CampaignName']} - {row['RecencyCode']}; {row['FrequencyCode']}; {row['MonetaryCode']}"
        if pd.isna(row['Segment Name']) or row['Segment Name'] == ''
        else row['Segment Name'],
        axis=1
    )
    print("Updated Segment Name")
    
    # Update Segment Code if blank with SourceCode
    df['Segment Code'] = df.apply(
        lambda row: row['SourceCode'] if pd.isna(row['Segment Code']) or row['Segment Code'] == '' else row['Segment Code'],
        axis=1
    )
    print("Updated Segment Code")

    #Replace SourceCodes with no returns in campaigns with Program value of other fields
    # Step 1: Create a mapping of CampaignCode to the most frequent non-null Program in each group

    program_types = ['Program', 'Fuse_Program']
    for program_type in program_types:
        program_map = (
            df.dropna(subset=[program_type])  # Remove rows where Program is null
            .groupby('CampaignCode')[program_type]  # Group by CampaignCode
            .agg(lambda x: x.mode()[0]))  # Get the most frequent Program in each group
        # Step 2: Use this map to fill missing values in the Program column only if CampaignCode is not null
        df[program_type] = df.apply(
            lambda row: program_map.get(row['CampaignCode'], row[program_type]) if pd.notna(row['CampaignCode']) else row[program_type],
            axis=1)
        print(f"{program_type} Added for applicable CampaignCodes with no SourceCode ")





    # Identify rows where SourceCode contains "XXX"/ONL and MailDate is missing
    md_condition = (
        df['SourceCode'].str.lower().str.startswith(('xxx', 'onl')) & 
        df['MailDate'].isna() & 
        df['_CampaignCode'].notna() &  # Ensure _CampaignCode is not blank (NaN)
        df['_CampaignCode'].str.strip().ne('')  # Ensure _CampaignCode is not an empty string
    )

    # Create mappings of _CampaignCode to MailDate and Fiscal (ignoring missing values)
    mail_date_mapping = df.dropna(subset=['MailDate']).set_index('_CampaignCode')['MailDate'].to_dict()
    fiscal_mapping = df.dropna(subset=['Fiscal']).set_index('_CampaignCode')['Fiscal'].to_dict()

    # Update MailDate for rows where SourceCode contains "XXX"/ONL and MailDate is missing
    df.loc[md_condition, 'MailDate'] = df.loc[md_condition, '_CampaignCode'].map(mail_date_mapping)

    # Update Fiscal for rows where SourceCode contains "XXX"/ONL and MailDate is missing
    df.loc[md_condition, 'Fiscal'] = df.loc[md_condition, '_CampaignCode'].map(fiscal_mapping)

    print("Mail Date & Fiscal updated for XXX & ONL source codes")

    # print("MailDate associated with campaigns updated")

    print("FFB_campperf_dfc_updates applied")

    return df


def FFB_Program(df):
    conditions = [
        (df['Media Outlet Code'] == "9005") | (df['Media Outlet Code'] == "Direct Marketing"),
        ((df['Media Outlet Code'] == "9000") & 
         (df['Segment Code'].str.contains("VC|9500|Mid-Level"))) | 
         (df['Project Code'].str.contains("9500"))
    ]
    choices = ['CORE', 'VC']
    
    df['Program'] = np.select(conditions, choices, default='Other')
    
    return df

def FFB_Channel(df):
    conditions = [
        # First priority: Paid Digital
        (df['Original URL'].str.contains("give|givenow|donatenow|givelegacy|giveobituary|givechoice", na=False)) |
        (df['Segment Code'].str.contains("WEBA50|W50|50YEARS|ONLSPECPAIDADS|ONLSPECWELPSA", na=False)),
        
        # Second priority: DM
        (df['Media Outlet Code'].isin(["9005", "Direct Marketing"])) & 
        (df['Segment Code'].str.contains("AQ|CR", na=False)) & 
        (~df['Segment Code'].str.contains("ONL", na=False)),
        
        # Third priority: Other (based on Media Outlet Code)
        (df['Media Outlet Code'].isin(["9005", "Direct Marketing"])),
        
        # Fourth priority: VC
        ((df['Media Outlet Code'] == "9000") & 
         (df['Segment Code'].str.contains("VC|9500|Mid-Level", na=False))) | 
         (df['Project Code'].str.contains("9500", na=False))
    ]
    
    choices = ['Paid Digital', 'DM', 'Other', 'VC']
    
    df['Channel'] = np.select(conditions, choices, default='Other')
    
    return df

def FFB_dataset_filters(df):
  print("FFB dataset filters function run")
  return df

# COMMAND ----------

# DBTITLE 1,FS
def FS_update_codes(df): #update_client_codes(df, client)
  print('inside update codes')

  #Replace SourceCodes that contain ZZZZ with blanks(Added 11/25/25)
  print('ZZZZ replace started')
  df['SourceCode'] = df['SourceCode'].str.replace('ZZZZ', '', regex=False)
  print('ZZZZ replace complete')


  print('returning df')
  return df


def FS_add_raw_sc_updates(df, client):
    print(f"🔎 Entering MC_add_raw_sc_updates | client={client}")

    if client != "FS":
        print("⏭️  Client is not FS — skipping unsourced SC logic")
        return df

    df = df.copy()
    print(f"📊 Starting rows: {len(df):,}")

    # FY as clean string (no .0)
    fy = (
        df["FY (SC)"]
        .dropna()
        .astype("Int64")
        .astype(str)
        .reindex(df.index, fill_value="")
    )

    prog = df["Program (SC)"].fillna("").astype(str).str.strip()

    # Build unsourced SourceCode
    df["_unsourced_sc"] = fy + prog + "Unsourced"
    print("🔧 Built _unsourced_sc values")

    # Build unique candidates (NO Campaign fields)
    candidates = (
        df[["Client", "FY (SC)", "Program (SC)", "_unsourced_sc"]]
        .drop_duplicates(
            subset=["Client", "FY (SC)", "Program (SC)"],
            keep="first",
        )
    )

    print(f"📦 Unique candidate combinations: {len(candidates):,}")

    existing_sc = set(df["SourceCode"].fillna("").astype(str).str.strip())
    print(f"🧮 Existing SourceCodes: {len(existing_sc):,}")

    to_add = candidates[~candidates["_unsourced_sc"].isin(existing_sc)].copy()
    print(f"➕ Candidates missing SourceCode: {len(to_add):,}")

    if to_add.empty:
        print("⚠️ No unsourced SourceCodes to add — exiting")
        df.drop(columns="_unsourced_sc", inplace=True)
        return df

    print("🚀 Creating unsourced SourceCode rows")

    to_add["SourceCode"] = to_add["_unsourced_sc"]
    to_add.drop(columns="_unsourced_sc", inplace=True)

    df.drop(columns="_unsourced_sc", inplace=True)

    final_df = pd.concat([df, to_add], ignore_index=True)

    print(f"✅ Finished MC_add_raw_sc_updates | final rows: {len(final_df):,}")

    return final_df






# COMMAND ----------

# DBTITLE 1,FWW
def FWW_update_codes(df): #update_client_codes(df, client)

  
  return df

# COMMAND ----------

# DBTITLE 1,HKI
def HKI_update_codes(df): #update_client_codes(df, client)
  print('inside build client codes')
  mask = df.CampaignCode.str.lower().str.contains('matcher').fillna(value=True)
  df = df.loc[~mask]
  print('Rows with CampaignCodes containing "matcher" removed')

  m = {
    'CampaignCode': 7, # 1-7
    'RFMCode': 2, # 8-9
    'PackageCode': 2, # 10-11
    'ListCode': 3 # 12-14
  }
  for k,v in m.items():
    if k not in df.columns:
      df[k] = ''
      print('added k')
    df[k] = [x.ljust(v,'_') for x in df[k].fillna(value='')]
  print('finished padding')

  cols = ['CampaignCode', 'RFMCode', 'PackageCode', 'ListCode']
  df['_SourceCode'] = np.add.reduce(df[cols], axis=1)
  df.SourceCode = df.SourceCode.str.replace(' ', '')  
  cond = df.CampaignCode.str.startswith('W').fillna(value=False)
  #df.CampaignCode = np.where(cond, df.CampaignCode, df.CampaignCode.str.replace('_','').str[:5])
  df.CampaignCode = np.where(
    cond, 
    df.CampaignCode, 
    np.where(
        (df.CampaignCode.str.replace('_', '').str.len() == 6) & 
        (df.CampaignCode.str.replace('_', '').str[-1].str.isdigit()), 
        df.CampaignCode.str.replace('_', ''), 
        df.CampaignCode.str.replace('_', '').str[:5]
    )
)


  print("Updated CampaignCodes not starting with W to first 5 characters (or 6 characters if 6th character is numeric)")

  # cond = df.CampaignCode.str.endswith('_').fillna(value=False)
  # df.CampaignCode = np.where(cond, df.CampaignCode.str[:-1], df.CampaignCode)


  if 'GiftAmount' in df.columns:
    if 'Constituent Specific Attributes Major Donor Description' in df.columns:
      d = {'Constituent Specific Attributes Major Donor Description': 'CSAMDD'}
      df = df.rename(columns=d)
      print("Column renamed successfully.")
    elif 'CSAMDD' not in df.columns:
      df['CSAMDD'] = ''
      print("CSAMDD column added as it was not present.")
    else:
      print("CSAMDD column already exists.")


  if 'MailDate' in df.columns:
    # get fiscal year of mail date
    df['Fiscal'] = fiscal_from_column(df, 'MailDate', schema['firstMonthFiscalYear'])
    #Update RFM code with List Code for Acq campaigns where FY is 2025 or greater and RFM Code is "___"
    df.loc[(df['Fiscal'] >= 2025) & (df['RFMCode'] == "__"), 'RFMCode'] = df['ListCode']
    print("Updated RFM codes 2025 and greater")


  #Add rows to identify Whitemail transactions later in transaction file
  if "MailDate" in df.columns:
    # Identify CampaignCodes not present in SourceCode
    missing_campaigns = df.loc[~df['CampaignCode'].isin(df['SourceCode']), 'CampaignCode'].unique()

    # Create new rows for missing CampaignCodes
    new_rows = []
    for campaign in missing_campaigns:
        # Get CampaignName and MailDate for the campaign
        campaign_data = df[df['CampaignCode'] == campaign].iloc[0]
        new_rows.append({
            'SourceCode': campaign,
            'CampaignCode': campaign,
            #'Quantity': 0,
            'CampaignName': campaign_data['CampaignName'],
            'PackageName': "Whitemail",
            'PackageCode': "Whitemail",
            'MailDate': campaign_data['MailDate']
        })

    # Convert new rows to a DataFrame
    new_rows_df = pd.DataFrame(new_rows)

    # Append the new rows to the original DataFrame
    df = pd.concat([df, new_rows_df], ignore_index=True)
    print("Campaign codes added for Whitemail")

  print('finished HKI_build_codes')
  return df


def HKI_create_names(df):
  df['_month'] = df.CampaignCode.str[1:3]
  df._month = pd.to_numeric(df._month, errors='coerce').fillna(value=0).astype(int)
  def _lambda_calendar(x):
    try:
      return calendar.month_abbr[x]
    except:
      return ''
  df['_month_name'] = df['_month'].apply(lambda x: _lambda_calendar(x))
  df['_year'] = df.CampaignCode.str[3:5]
  df._year = pd.to_numeric(df._year, errors='coerce').astype('Int32')
  df._year = np.where(df._month > 6, df._year + 1, df._year)
  df['_name'] = 'FY' + df._year.astype(str) + '_' + df._month_name + '_' + dfc.CampaignCode.str[0]
  df.CampaignName = np.where(df.CampaignName.isna(), df['_name'], df.CampaignName)
  cols = ['_month', '_month_name', '_year', '_name']
  return df.drop(cols, axis=1)  


def HKI_initiate_codes(df):
  print('inside HKI_update_codes')
  df = df.dropna(axis=0, subset=['CampaignCode'])
  df['_CampaignCode'] = df.CampaignCode
  
  df['PackageCode'] = df['PackageCode'].str.replace('`','').fillna(value='')
  df['PackageCode'] = df.apply(lambda row : row['PackageCode'].replace(row['CampaignCode'], ''), axis=1)
  df['SourceCode'] = df['CampaignCode'] + df['PackageCode']
  df.SourceCode = df.SourceCode.str.upper().str.replace(' ','')

  cond = df['Appeal Category'].str.upper() == 'ACQUISITION'
  df['_List'] = np.where(cond, df.PackageCode.str[:-1], '')
  df['_Package'] = np.where(cond, df.PackageCode.str[-1], 1)

  df['_RFM'] = np.where(cond, '', df.PackageCode.str[:2])
  df._Package = np.where(cond, df._Package, df.PackageCode.str[2])

  cond = df._List.str.len()>3
  df._List = np.where(cond,  df.PackageCode.str[:-2], df._List)
  df._Package = np.where(cond, df.PackageCode.str[-2:], df._Package) 

  m = {
    '_CampaignCode': 7, # 1-7
    '_RFM': 2, # 8-9
    '_Package': 2, # 10-11
    '_List': 3 # 12-14
  }
  for k,v in m.items():
    df[k] = [x.ljust(v,'_') for x in df[k].fillna(value='')]
    
  cols = ['_CampaignCode', '_RFM', '_Package', '_List']
  df['_SourceCode'] = np.add.reduce(df[cols], axis=1)
  
  df['_CLCode'] = df._CampaignCode + df._List  
  mask = df._List == '___'
  _df = df.loc[~mask]

  cols = ['_CLCode', 'ListName']
  _df = _df[cols].drop_duplicates()
  mapper = dict(zip(_df._CLCode, _df.ListName))
  del _df
  
  print('before: ', df.ListName.nunique())
  df.ListName = df._CLCode.map(mapper)
  print('after: ', df.ListName.nunique())
  df = df.drop('_CLCode', axis=1)
  
  return df.drop_duplicates('SourceCode', keep='last')




# COMMAND ----------

# DBTITLE 1,MC
def MC_update_codes(df): #update_client_codes(df, client)
  print('inside update codes')

  #Replace SourceCodes that contain ZZZZ with blanks(Added 11/25/25)
  print('ZZZZ replace started')
  df['SourceCode'] = df['SourceCode'].str.replace('ZZZZ', '', regex=False)
  print('ZZZZ replace complete')


  print('returning df')
  return df


def MC_add_raw_sc_updates(df, client):
    print(f"🔎 Entering MC_add_raw_sc_updates | client={client}")

    if client != "MC":
        print("⏭️  Client is not MC — skipping unsourced SC logic")
        return df

    df = df.copy()
    print(f"📊 Starting rows: {len(df):,}")

    # FY as clean string (no .0)
    fy = (
        df["FY (SC)"]
        .dropna()
        .astype("Int64")
        .astype(str)
        .reindex(df.index, fill_value="")
    )

    prog = df["Program (SC)"].fillna("").astype(str).str.strip()

    # Build unsourced SourceCode
    df["_unsourced_sc"] = fy + prog + "Unsourced"
    print("🔧 Built _unsourced_sc values")

    # Build unique candidates (NO Campaign fields)
    candidates = (
        df[["Client", "FY (SC)", "Program (SC)", "_unsourced_sc"]]
        .drop_duplicates(
            subset=["Client", "FY (SC)", "Program (SC)"],
            keep="first",
        )
    )

    print(f"📦 Unique candidate combinations: {len(candidates):,}")

    existing_sc = set(df["SourceCode"].fillna("").astype(str).str.strip())
    print(f"🧮 Existing SourceCodes: {len(existing_sc):,}")

    to_add = candidates[~candidates["_unsourced_sc"].isin(existing_sc)].copy()
    print(f"➕ Candidates missing SourceCode: {len(to_add):,}")

    if to_add.empty:
        print("⚠️ No unsourced SourceCodes to add — exiting")
        df.drop(columns="_unsourced_sc", inplace=True)
        return df

    print("🚀 Creating unsourced SourceCode rows")

    to_add["SourceCode"] = to_add["_unsourced_sc"]
    to_add.drop(columns="_unsourced_sc", inplace=True)

    df.drop(columns="_unsourced_sc", inplace=True)

    final_df = pd.concat([df, to_add], ignore_index=True)

    print(f"✅ Finished MC_add_raw_sc_updates | final rows: {len(final_df):,}")

    return final_df






# COMMAND ----------

# DBTITLE 1,NJH
def NJH_create_helper_code(df):
    # if first character is d and second character is not m, acquistion
    cond = (df.SourceCode.str[0] == 'D') & (df.SourceCode.str[0] != 'M')

    # make a new temp column that gets the fourth fifth and sixth characters from the source code
    df["_TempColumn"] = np.where(cond, df.SourceCode.str[3:6], "___")

    # when it is acquisition we need to overwrite the end of the string so we dont pick up unwanted rfm stuff
    df["_SourceCode"] = np.where(cond, df.SourceCode.str[:3] + "___", df.SourceCode)

    # create _sourcecode column which is source code + temp column above 
    df._SourceCode = df._SourceCode + df._TempColumn

    # get package from appeals 
    df._SourceCode = np.where(cond, df._SourceCode, df._SourceCode.str[:-1] + df.SourceCode.str[-1])
    df['CampaignCode'] = df['SourceCode'].str[:3]

    cond = df.SourceCode.str.startswith("DM")
    df["_SourceCode"] = np.where(cond, "_________", df._SourceCode)

    return df


def NJH_initiate_codes(df):
  df.SourceCode = df.SourceCode.str.upper().str.replace(" ","")
  df.MailDate = pd.to_datetime(df.MailDate)
  mask = df.MailDate.dt.year > 2019
  df = df.loc[mask]

  df = NJH_create_helper_code(df)

  mask = df.ListName.str.upper().str.contains("SEED").fillna(value = False)
  return df#.loc[~mask] #5/19 removed this condition as per Derek's suggestion


def NJH_update_codes(df): #update_client_codes(df, client)
  if 'GiftAmount' in df.columns:
    df = df.rename(columns={'Gf_Appeal': 'CampaignCode'})
    df['SourceCode'] = df.CampaignCode + df.Gf_Package
  else:
    df = NJH_create_helper_code(df)
  print('SourceCodes updated')
  
  
  return df

def NJH_dataset_filters(df):
    print("Running NJH dataset filters")
    
    # Determine if Campaign is tracked by Fuse and add column fuse_tracked_campaign
    Fuse_Campaign_Lookup(df, client)
    print("fuse_tracked_campaign column added")

    # Add Additional Revenue Campaigns
    additional_revenue_condition = (
        df['Gf_Campaign'].isin(['Direct Mail FY2020', 'Direct Mail FY2021', 'Direct Mail FY2022', 
                                'Direct Mail FY2023', 'Direct FY2024', 'Direct FY2025', 'Direct FY2026'])
        & df['CampaignName'].isna()
    )
    df.loc[additional_revenue_condition, 'CampaignName'] = 'Additional Revenue'
    print("Additional Revenue added to CampaignName where applicable")

    # Update SourceCode to 'REC' if it starts with 'REC' and meets the additional_revenue_condition
    rec_condition = additional_revenue_condition & df['SourceCode'].str.startswith('REC', na=False)
    df.loc[rec_condition, 'SourceCode'] = 'REC'
    print("SourceCode updated to 'REC' where applicable")

    # Define mappings for SourceCode to PackageName and PackageCode
    mappings = {
        'Acknowledgments': {
            'source_codes': ['DM'],
            'package_code': 'Acknowledgments'
        },
        'Misc/Unsolicited': {
            'source_codes': [
                'Unsolicited', 'DM Unsolicited', 'DM24WTGMADNAV', 
                'DM24MADR', 'DM24EDPMALAP', 'DW2500D2W', 'AN2312SSF',
                'AF Unsolicited', 'AF Solicitation', 'DW2400D2W'
            ],
            'package_code': 'Misc/Unsolicited'
        },
        'Monthly Giving': {
            'source_codes': ['REC', 'RECUR1', 'RECUR2'], # RECUR1 & RECUR2 should be removed above (happen when package code is UR1 or UR2)
            'package_code': 'Monthly Giving'
        },
        'New Directions': {
            'source_codes': ['DM2409NDF', 'AN2306NDS'],
            'package_code': 'New Directions'
        },
        'Tribute': {
            'source_codes': ['DM2200TRIB', 'Tribute', 'DM24MWTDTRIBB', 'DM24WTGTRIB', 'DM24MWTDTRIBMEGA','DM24TRIBJC','DM24TRIBR', 'DM24TRIB' ],
            'package_code': 'Tribute'
        }
    }

    # Apply mappings
    for package_name, details in mappings.items():
        condition = (
            df['SourceCode'].isin(details['source_codes']) &
            (df['CampaignName'] == 'Additional Revenue')
        )
        df.loc[condition, 'PackageName'] = package_name
        df.loc[condition, 'PackageCode'] = details['package_code']

    print("PackageName and PackageCode updated for Tribute, Misc/Unsolicited, Monthly Giving, New Directions")

    # Handle blank PackageName for Additional Revenue
    carry_in_condition = (
        (df['CampaignName'] == 'Additional Revenue') &
        (df['PackageName'].isna())
    )
    df.loc[carry_in_condition, 'PackageName'] = 'Carry-In'
    df.loc[carry_in_condition, 'PackageCode'] = 'Carry-In'
    print("PackageName and PackageCode updated to 'Carry-In' where applicable")

    print("NJH dataset filters applied")
    return df


  

# COMMAND ----------

# DBTITLE 1,RADY
def RADY_update_codes(df):  # update_client_codes(df, client)
    print('RADY update_codes begun')

    # Ensure 'SourceCode' exists to avoid KeyErrors
    if 'SourceCode' not in df.columns:
        df['SourceCode'] = ''



    #Add rows to identify Whitemail transactions later in transaction file
    if "MailDate" in df.columns:
        # Identify CampaignCodes not present in SourceCode
        missing_campaigns = df.loc[~df['CampaignCode'].isin(df['SourceCode']), 'CampaignCode'].unique()

        # Create new rows for missing CampaignCodes
        new_rows = []
        for campaign in missing_campaigns:
            # Get CampaignName and MailDate for the campaign
            campaign_data = df[df['CampaignCode'] == campaign].iloc[0]
            new_rows.append({
                'SourceCode': campaign,
                'CampaignCode': campaign,
                #'Quantity': 0,
                'CampaignName': campaign_data['CampaignName'],
                'PackageName': "Whitemail",
                'MailDate': campaign_data['MailDate']
            })

        # Convert new rows to a DataFrame
        new_rows_df = pd.DataFrame(new_rows)

        # Append the new rows to the original DataFrame
        df = pd.concat([df, new_rows_df], ignore_index=True)
        print("Campaign codes added for Whitemail")

    print("RADY update_codes complete")
    return df

def RADY_campperf_dfc_updates(df):
    print("RADY campperf_dfc_updates begun")

    # Fill missing MailDate values with the most recent (max) valid MailDate for the same CampaignCode 
    print(f"Missing MailDate count after: {dfc['MailDate'].isna().sum()}")
    df.loc[df['_CampaignCode'].notna(), 'MailDate'] = (
        df.groupby('_CampaignCode')['MailDate'].transform('max')
    )
    #Add Fiscal Year MailDate
    df['Fiscal'] = fiscal_from_column(df, 'MailDate', schema['firstMonthFiscalYear'])
    print(f"Missing MailDate count after: {dfc['MailDate'].isna().sum()}")

    print("RADY campperf_dfc_updates complete")
        



# COMMAND ----------

# DBTITLE 1,TCI
def TCI_update_codes(df):
    # --- Standardize SourceCode ---
    df.SourceCode = df.SourceCode.str.strip().str.upper().str.replace(' ', '')

    mask = df.SourceCode.str.startswith('11Q')
    df['_SourceCode'] = np.where(mask, df.SourceCode.str[2:], df.SourceCode)

    mask = df.SourceCode.str.startswith('1Q')
    df['_SourceCode'] = np.where(mask, df.SourceCode.str[1:], df._SourceCode)

    # --- Load SourceCode reference ---
    sc_tci = load_sc('TCI').sort_values('MailDate', ascending=False)

    df = df.merge(
        sc_tci[['SourceCode', 'CampaignCode', 'CampaignName']],
        how='left',
        on='SourceCode',
        suffixes=('', '_sc')
    )

    # --- Fill CampaignCode ---
    missing_campaigncode_before = df['CampaignCode'].isna().sum() if 'CampaignCode' in df.columns else len(df)

    if 'CampaignCode_sc' in df.columns:
        if 'CampaignCode' not in df.columns:
            df['CampaignCode'] = df['CampaignCode_sc']
        else:
            df['CampaignCode'] = df['CampaignCode'].fillna(df['CampaignCode_sc'])
    else:
        df['CampaignCode'] = df.get('CampaignCode', np.nan)

    # Fallback fill from SourceCode prefix
    df['CampaignCode'] = df['CampaignCode'].fillna(df['SourceCode'].str[:5])
    missing_campaigncode_after = df['CampaignCode'].isna().sum()
    print(f"Filled {missing_campaigncode_before - missing_campaigncode_after} missing CampaignCode values.")

    # --- Fill CampaignName ---
    missing_campaignname_before = df['CampaignName'].isna().sum() if 'CampaignName' in df.columns else len(df)

    if 'CampaignName_sc' in df.columns:
        if 'CampaignName' not in df.columns:
            df['CampaignName'] = df['CampaignName_sc']
        else:
            df['CampaignName'] = df['CampaignName'].fillna(df['CampaignName_sc'])
    else:
        df['CampaignName'] = df.get('CampaignName', np.nan)

    missing_campaignname_after = df['CampaignName'].isna().sum()
    print(f"Filled {missing_campaignname_before - missing_campaignname_after} missing CampaignName values.")

    # --- Clean up temporary merge columns ---
    df.drop(columns=['CampaignCode_sc', 'CampaignName_sc'], errors='ignore', inplace=True)

    return df




def TCI_initiate_codes(df): #fix_client_codes(df, client)
  df.SourceCode = df.SourceCode.str.strip().str.upper().str.replace(' ','')
  
  mask = df.SourceCode.str.len() > 7
  df = df.loc[mask]
  
  mask = df.SourceCode.str.startswith('11Q')
  df['_SourceCode'] = np.where(mask, df.SourceCode.str[2:], df.SourceCode)
  
  mask = df.SourceCode.str.startswith('1Q')
  df['_SourceCode'] = np.where(mask, df.SourceCode.str[1:], df._SourceCode)
  
  return df 

def TCI_dataset_filters(df):
  #Determine if Campaign is tracked by Fuse and add column fuse_tracked_campaign
  Fuse_Campaign_Lookup(df,client)
   
  print("TCI dataset filters applied")
  return df

# COMMAND ----------

# DBTITLE 1,TLF
def TLF_update_codes(df): #update_client_codes(df, client)

  #Add rows to identify Whitemail transactions later in transaction file
  if "MailDate" in df.columns:
    # Identify CampaignCodes not present in SourceCode
    missing_campaigns = df.loc[~df['CampaignCode'].isin(df['SourceCode']), 'CampaignCode'].unique()

    # Create new rows for missing CampaignCodes
    new_rows = []
    for campaign in missing_campaigns:
        # Get CampaignName and MailDate for the campaign
        campaign_data = df[df['CampaignCode'] == campaign].iloc[0]
        new_rows.append({
            'SourceCode': campaign,
            'CampaignCode': campaign,
            #'Quantity': 0,
            'CampaignName': campaign_data['CampaignName'],
            'PackageName': "Whitemail",
            'MailDate': campaign_data['MailDate']
        })

    # Convert new rows to a DataFrame
    new_rows_df = pd.DataFrame(new_rows)

    # Append the new rows to the original DataFrame
    df = pd.concat([df, new_rows_df], ignore_index=True)
    print("Campaign codes added for Whitemail")
  return df

# COMMAND ----------

# DBTITLE 1,U4U
def U4U_update_codes(df): #update_client_codes(df, client)
  print("Initiate update client codes")
  df.SourceCode = df.SourceCode.str.strip().str.upper().str.replace(' ','') 
  
  #If acquisition campaign make the RFMCode equal to the ListCode
  if "MailDate" in df.columns:
    df.loc[df['SourceCode'].str.startswith('A'), 'RFMCode'] = df['ListCode'] 
    print("Mapped ListCode to RFMCode for Acquisition Campaigns")


  #Fill in Campaign Code with AppealID if CampaignCode is blank
  if "GiftAmount" in df.columns:
    df['CampaignCode'] = np.where(df['CampaignCode'].isna() | (df['CampaignCode'] == ''), df['Appeal ID'], df['CampaignCode'])
    print("Mapped AppealIDs to blank CampaignCodes")

  print("Update client codes complete")
  return df  

# COMMAND ----------

# DBTITLE 1,WFP

def WFP_add_filters(df):

  return df




def WFP_update_codes(df): #update_client_codes(df, client)
  if 'GiftAmount' in df.columns:
    df.CampaignCode = np.where(
      df.CampaignCode.isna(),
      df.SourceCode.str[:4],
      df.CampaignCode
      )
  return df.rename(columns={'Campaign Name': 'CampaignName'})


def WFP_initiate_codes(df):
  df.SourceCode = df.SourceCode.str.strip().str.replace('-','').str.upper()
  mask = df.SourceCode.str.contains('SEED')
  df = df.loc[~mask]

  mask = df.SourceCode.duplicated()
  codes = df.loc[mask]['SourceCode'].unique()

  mask = df.SourceCode.isin(codes)
  _df = df.loc[mask].groupby('SourceCode')['Quantity'].sum().to_frame(name='NewQuantity')
  df = pd.merge(df, _df, on='SourceCode', how='left')

  df.Quantity = np.where(mask, df.NewQuantity, df.Quantity)
  df = df.drop(['NewQuantity'], axis=1).drop_duplicates('SourceCode')



def WFP_dataset_filters(df):
  #Determine if Campaign is tracked by Fuse
  Fuse_Campaign_Lookup(df,client)
   
  print("WFP dataset filters applied")
  return df


# COMMAND ----------

# MAGIC %md
# MAGIC # Helpers

# COMMAND ----------

# DBTITLE 1,Helpers
# helpers
def GiftFiscal(df, client):
  schema = get_schema(client)
  df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', schema['firstMonthFiscalYear']).astype('Int32')
  return df

def align_cols(df, f):
  schema = get_schema_details(f.SCHEMA)
  cols = make_client_cols(schema)
  print('columns: ', cols)
  for col in cols:
    if col not in df.columns:
      df[col] = None
  
  cols_to_drop = []
  for col in df.columns:
    if col not in cols:
      cols_to_drop.append(col)
  return df.drop(cols_to_drop, axis=1)


def assert_cols(df, new):
  cols = set(df.columns)
  new = new[cols]
  new_cols = set(new.columns)
  assert len(cols^new_cols) == 0


def assert_one_sheet(path, f):
  # number of sheets in files
  xl = pd.ExcelFile(os.path.join(path, f))
  xl.close()
  assert len(xl.sheet_names) == 1


def cast(vector, dtype):
  if dtype == 'str':
    return vector.astype(str)
  elif dtype == 'int':
    return format_int(vector)
  elif dtype == 'float':
    return format_float(vector)
  elif dtype == 'date':
    return format_date(vector)


def check_col(col, pattern):
  # if len(col) == 0:
  #   return np.array([], dtype=bool)
                        
  r = re.compile(pattern)
  regmatch = np.vectorize(lambda x: bool(r.match(x)))
  return regmatch(col)


def check_regex(df, _df, regex):
  for k,v in regex.items():
    df[k] = df[k].str.upper().str.strip()
    mask = check_col(df[k], v)
    # print(k, df.shape[0] - mask.sum())
    _df = pd.concat([_df, df.loc[~mask]])
  return _df


def clear_dir(path):
  print('before: ', os.listdir(path))
  for f in os.listdir(path):
    if os.path.isfile(os.path.join(path, f)):
      os.remove(os.path.join(path, f))
  print('after: ', os.listdir(path))


def collect_sc_docs(f):
  schema = get_schema_details(f.SCHEMA)
  _dfs = []
  path = os.path.join(f.RAW, 'SourceCodeDocs')
  files = os.listdir(path)
  for f in files:
    if os.path.isfile(os.path.join(path, f)):
      if '.csv' in f:
        _dfs.append(pd.read_csv(os.path.join(path, f)))      
      else:
        _dfs.append(pd.read_excel(os.path.join(path, f)))
  df = pd.concat(_dfs).rename(columns=schema['CPMapper'])
  if 'SourceCode' in df.columns:
    return df.dropna(axis=0, subset=['SourceCode']).drop_duplicates('SourceCode')
  return df


def decouple_costs(df):
  if 'ListCPP' not in df.columns:
    return df

  _df = df.copy()
  try:
  
    cols = list(df.columns)

    m = {'PackageCPP': 'OriginalPackageCPP'}
    df = df.rename(columns=m)

    df['ExpandedListCost'] = (df.Quantity * df.ListCPP).fillna(value=0)

    _df = df.groupby('PackageName')['ExpandedListCost'].sum().to_frame(name='TotalListCostPerPackage')
    df = pd.merge(df, _df, on='PackageName', how='left')

    df['ExpandedPackageCost'] = df.OriginalPackageCPP * df.Quantity

    _df = df.groupby('PackageName')['ExpandedPackageCost'].sum().to_frame(name='TotalPackageCost')
    df = pd.merge(df, _df, on='PackageName', how='left')

    df['AdjustedPackageCost'] = df.TotalPackageCost - df.TotalListCostPerPackage

    _df = df.groupby('PackageName')['Quantity'].sum().to_frame(name='TotalMailedByPackage')
    df = pd.merge(df, _df, on='PackageName', how='left')

    df['PackageCPP'] = np.round_((df.AdjustedPackageCost / df.TotalMailedByPackage), 2)

    return df[cols]
  except Exception as e:
    print('exception caught (%s), returning original dataframe' %e)
    return _df


def evaluate_regex(df, f):
  schema = get_schema_details(f.SCHEMA)
  regex = schema['SourceCodeRegex']

  for k,v in regex.items():
    mask = (check_col(df[k].astype(str), v)) | (df[k]=='')
    errors = df.shape[0] - mask.sum()
    print('Errors in %s: %d' %(k, errors))
    assert errors == 0
  return None


def expand_cols_from_codes(df, f):
    schema = get_schema_details(f.SCHEMA)
    col = schema['SCMapKey']
    sc_map = schema['SCMapper']
    sc_type = schema.get('SCMapType')
    print(f"🔍 expand_cols_from_codes | SCHEMA={f.SCHEMA}")
    print(f"   ➜ SCMapType = {sc_type}")
    print(f"   ➜ SCMapKey  = {col}")
    if sc_type == 'header':
        print("✅ Using expand_header_type")
        return expand_header_type(df, sc_map, col)
    elif sc_type == 'year':
        print("✅ Using expand_year_type")
        return expand_year_type(df, sc_map, col)
    elif sc_type == 'mcyear':
        print("✅ Using expand_mcyear_type")
        return expand_mcyear_type(df, sc_map, col)
    elif sc_type == 'override':
        print("✅ Using expand_override_type")
        return expand_override_type(df, sc_map, col, schema)
    elif sc_type == 'conditional':
        print("✅ Using expand_conditional_type")
        return expand_conditional_type(df, sc_map, col)
    elif sc_type == 'fiscal_year':
        fym = schema.get('firstMonthFiscalYear')
        print(f"✅ Using expand_fiscal_type | firstMonthFiscalYear={fym}")
        return expand_fiscal_type(df, sc_map, col, fym)
    else:
        print("⚠️ No matching SCMapType — returning df unchanged")
        return df
 

def expand_conditional_type(df, sc_map, col):
  df = expand_header_type(df, sc_map, col)
  
  rfm = ['RecencyCode', 'FrequencyCode', 'MonetaryCode']
  cond = df['_SourceCode'].str.startswith('Q')
  for col in rfm:
    df.loc[cond, col] = None
  df.loc[~cond, 'ListCode'] = None
  
  return df
 

def expand_fiscal_type(df, sc_map, col, fym):  
  
  df['MailDate'] = pd.to_datetime(df['MailDate'])
  df['MailFiscal'] = fiscal_from_column(df, 'MailDate', fym)
  
  year = max([int(x) for x in sc_map.keys()])
  mask = (df.MailFiscal < year) | (df.MailFiscal.isna())
  
  # map values and combine dataframes
  df_pre = expand_header_type(df.loc[mask], sc_map[str(year-1)], col) 
  df_post = expand_header_type(df.loc[~mask], sc_map[str(year)], col) 
  return pd.concat([df_pre, df_post])


def expand_header_type(df, sc_map, col):
  for k,v in sc_map.items():
    if not v['Position']:
      df['_'+k] = None
    elif isinstance(v['Position'], list):
      a,b = get_ab(v['Position'])
      df['_'+k] = df[col].str[a:b]
    else:
      df['_'+k] = df[col].str[v['Position']-1]
  for k,v in sc_map.items():
    if v['Map']:
      df[k] = df['_'+k].map(v['Map']).fillna(value=df['_'+k])
    else:
      df[k] = df['_'+k]
  return df


def expand_override_type(df, sc_map, col, schema):
  df[col] = df[col].astype(str)
  df = expand_header_type(df, sc_map, col)
  ovrd = 'RFM_Override'
  if ovrd in schema.keys():
    df['_RFM'] = df[col].map(schema[ovrd])
    cond = df.ListCode.isin(schema[ovrd].keys())
    df.MonetaryCode = np.where(cond, df['_RFM'], df.MonetaryCode)
    df.RecencyCode = np.where(cond, df['_RFM'], df.RecencyCode)
  return df


def expand_year_type(df, sc_map, col):
  
  df['MailDate'] = pd.to_datetime(df['MailDate'])
  
  year = max([int(x) for x in sc_map.keys()])
  mask = (df.MailDate.dt.year < year) | (df.MailDate.isna())
  
  # map values and combine dataframes
  df_pre = expand_header_type(df.loc[mask], sc_map[str(year-1)], col) 
  df_post = expand_header_type(df.loc[~mask], sc_map[str(year)], col) 
  return pd.concat([df_pre, df_post])

def RADY_conditional(df, sc_map, col):
  
  df['MailDate'] = pd.to_datetime(df['MailDate'])
  
  year = max([int(x) for x in sc_map.keys()])
  mask = (df.MailDate.dt.year < year) | (df.MailDate.isna())
  
  # map values and combine dataframes
  df_pre = expand_header_type(df.loc[mask], sc_map[str(year-1)], col) 
  df_post = expand_header_type(df.loc[~mask], sc_map[str(year)], col) 
  return pd.concat([df_pre, df_post])


def expand_mcyear_type(df, sc_map, col):
  
  df['MailDate'] = pd.to_datetime(df['MailDate'])
  
  year = max([int(x) for x in sc_map.keys()])
  m1 = (df.MailDate.dt.year < year) | (df.MailDate.isna())
  m2 = (~m1) & (~df.CampaignCode.str.startswith('QQ'))

  # map values and combine dataframes
  df_pre = expand_header_type(df.loc[m1], sc_map[str(year-1)], col) 
  df_post = expand_header_type(df.loc[m2], sc_map[str(year)], col) 
  df_qq = df.loc[(~m1)&(~m2)] 
  return pd.concat([df_pre, df_post, df_qq])


def format_date(v):
  v = pd.to_datetime(v).dt.date
  return pd.to_datetime(v)


def format_float(v):
  v = v.astype(str)
  v = v.str.strip()
  v = v.str.replace(',','').str.replace('$','')
  return v.astype(float)


def format_int(v):
  v = v.astype(str)
  v = v.str.strip()
  v = v.str.replace(',','')
  return v.astype(float).astype('Int64') 
 

def get_ab(l):  
  return l[0]-1, l[-1]


def make_client_cols(schema):
  MASTER_COLUMNS = [
    'CampaignCode', 'ListCode', 'ListName', 'ListCPP', 'SourceCode',
    'PackageCode', 'RecencyCode', 'FrequencyCode', 'MonetaryCode',
    'Quantity', 'CampaignName', 'MailDate', 'PackageName', 'PackageCPP', 'RFMCode', 'CountsFileName'
  ]
  for r in schema['CPUnique']['remove']:
    MASTER_COLUMNS.pop(MASTER_COLUMNS.index(r))
  return MASTER_COLUMNS + schema['CPUnique']['add']


def record_errors(_df, errors, f, path):
  kws = ['FY22', 'FY23']
  pattern = '|'.join(kws)
  mask = _df['CampaignName'].str.contains(pattern)
  num_invalid = _df.loc[mask].shape[0]
  if num_invalid > 0:
    errors += num_invalid
    _f = f.split('.')[0] + '.csv'
    print('errors in %s' %_f)
    _df.loc[mask].to_csv(os.path.join(path, 'errors', _f), index=False)
    # send email to team with invalid
    # psuedo code: send_email(_df.loc[mask])
  return errors


def repair_source_codes(df, sc_col, aid='AppealID', seg='SegmentCode', pid='PackageID'):
  # check source code length
  condition = df[sc_col].str.len() == 14
  df[sc_col] = np.where(condition, df[sc_col], df[aid] + df[seg] + df[pid])
  return df


def validate(df, f, path, errors, dtypes, regex, sc_col='SourceCodeID'):
  '''TODO'''  
  # assert only one sheet
  assert_one_sheet(path, f)
  
  # read in new source code doc
  new = pd.read_excel(os.path.join(path, f))
  new[sc_col] = new[sc_col].astype(str).str.strip()

  # assert columns match
  assert_cols(df, new)

  # combine records
  df = pd.concat([df, new]).drop_duplicates(sc_col, keep='last')
  
  # repair source codes where length is not 14 characters
  df = repair_source_codes(df, sc_col)

  # check / cast columns  
  for k,v in dtypes.items():
    df[k] = cast(df[k],v)     

  # check patterns of critical fields
  _df = pd.DataFrame()
  _df = check_regex(df, _df, regex)
  errors = record_errors(_df, errors, f, path)
    
  # drop null source codes
  mask = (df[sc_col]=='NAN') | (df[sc_col].isna())
  return df.loc[~mask], errors    

# COMMAND ----------

# DBTITLE 1,Sharepoint Helpers
def write_to_sp(df):
  # write to SharePoint
  # write temp file to cluster
  file_name = "/tmp/MC_SourceCode.xlsx"
  df.to_excel(file_name, index=False)

  # establish share point context
  relative_url = 'Shared Documents/PowerBI/MercyCorps/Azure/SupportDocs'
  libraryRoot = client_context.web.get_folder_by_server_relative_url(relative_url)
  libraryRoot

  # create and load file object
  info = FileCreationInformation()    
  with open(file_name, 'rb') as content_file:
      info.content = content_file.read()

  info.url = "MC_SourceCode.xlsx"
  info.overwrite = True
  upload_file = libraryRoot.files.add(info)

  # transfer file to share point
  client_context.execute_query()


# COMMAND ----------

# DBTITLE 1,Moved from CP
def add_synth_sc(df, schema):
  m = schema['SynthSC']
  if m:
    cols = []

    for k in sorted(m.keys()):
      v = m[k]
      cols.append(v['column'])
      df['_'+v['column']] = df[v['column']].fillna(v['fill_value'])

    new_cols = ['_'+x for x in cols]
    df['SynthSC'] = np.add.reduce(df[new_cols], axis=1)
    df['SourceCode'] = np.where(df['SourceCode'].isna(), df['SynthSC'], df['SourceCode'])

    return df.drop(new_cols+['SynthSC'], axis=1)
  else:
    df['SourceCode'] = df['SourceCode'].fillna(value='')
    return df