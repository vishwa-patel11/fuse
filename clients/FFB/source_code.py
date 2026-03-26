"""FFB source code validation logic."""
import pandas as pd
import numpy as np
from datetime import datetime


def _pad_sc(df):
    """Build SourceCode from CampaignCode+PackageCode+Segment Code etc with padding."""
    code_cols = ['CampaignCode', 'PackageCode', 'Segment Code', 'RecencyCode', 'FrequencyCode', 'MonetaryCode']
    parts = []
    for col in code_cols:
        if col in df.columns:
            parts.append(df[col].fillna('').astype(str).str.strip())
        else:
            parts.append(pd.Series([''] * len(df), index=df.index))
    if not parts:
        return pd.Series([''] * len(df), index=df.index)
    result = parts[0]
    for p in parts[1:]:
        result = result + p
    return result


def initiate_codes(df, client):
    df.PackageName = df.PackageName.str.strip()
    df['SourceCode'] = _pad_sc(df)
    print('end FFB update codes')
    return df


def update_codes(df, client):
    df['RFMCode'] = df['SourceCode'].apply(
        lambda x: (x[0:3] if x[0] == 'X' else (x[1:4] if x[0].isalpha() else x[0:3])) if x is not None else None)
    if "MailDate" in df.columns:
        cn_condition = df['SourceCode'].str.startswith(('XXX', 'ONL')) & df['CampaignName'].isna()
        md_condition = df['SourceCode'].str.startswith(('XXX', 'ONL')) & df['MailDate'].isna()
        campaign_name_mapping = df.dropna(subset=['CampaignName']).set_index('CampaignCode')['CampaignName'].to_dict()
        mail_date_mapping = df.dropna(subset=['MailDate']).set_index('CampaignCode')['MailDate'].to_dict()
        df.loc[cn_condition, 'CampaignName'] = df.loc[cn_condition, 'CampaignCode'].map(campaign_name_mapping)
        df.loc[md_condition, 'MailDate'] = df.loc[md_condition, 'CampaignCode'].map(mail_date_mapping)
    if "GiftAmount" in df.columns:
        if 'Media Outlet Code' in df.columns:
            df['Media Outlet Code'].replace(['nan', 'None'], np.nan, inplace=True)
        if 'Segment Code' in df.columns:
            df['Segment Code'].replace(['nan', 'None'], np.nan, inplace=True)
        if 'Project Code' in df.columns:
            df['Project Code'].replace(['nan', 'None'], np.nan, inplace=True)
        if 'Media Outlet Code' in df.columns:
            df['Media Outlet Code'] = df['Media Outlet Code'].astype(str).str.strip()
            df["Media Outlet Code"] = df["Media Outlet Code"].astype("string").str.replace(r"\.0$", "", regex=True)
        if 'Segment Code' in df.columns:
            df['Segment Code'] = df['Segment Code'].astype(str).str.strip()
        if 'Project Code' in df.columns:
            df['Project Code'] = df['Project Code'].astype(str).str.strip()
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
        df['Program'] = np.select([cond_core, cond_vc], ['CORE', 'VC'], default='Other')
        df = df.sort_values(by=['DonorID', 'GiftDate'])
        df['JoinProgram'] = df.groupby('DonorID')['Program'].transform('first')
        conditions = [
            (df['Original URL'].str.contains("give|givenow|donatenow|givelegacy|giveobituary|givechoice", na=False)) |
            (df['Segment Code'].str.contains("WEBA50|W50|50YEARS|ONLSPECPAIDADS|ONLSPECWELPSA", na=False)),
            (df['Media Outlet Code'].isin(["9005", "Direct Marketing"])) &
            (df['Segment Code'].str.contains("AQ|CR", na=False)) &
            (~df['Segment Code'].str.contains("ONL", na=False)),
            (df['Media Outlet Code'].isin(["9005", "Direct Marketing"])),
            ((df['Media Outlet Code'] == "9000") &
             (df['Segment Code'].str.contains("VC|9500|Mid-Level", na=False))) |
            (df['Project Code'].str.contains("9500", na=False))
        ]
        choices = ['Paid Digital', 'DM', 'Other', 'VC']
        df['Channel'] = np.select(conditions, choices, default='Other')
        condition = (
            (df['Program'].isin(['CORE', 'VC'])) |
            ((df['Program'] == 'Other') & (df['Channel'] == 'Paid Digital'))
        )
        df['Fuse_Program'] = np.where(condition, 'Yes', 'No')
        year = datetime.now().year
        mask = df['GiftDate'] >= datetime(year - 3, 1, 1)
        df = df.loc[mask]
    return df


def campperf_dfc_updates(df, client):
    df['Segment Name'] = df.apply(
        lambda row: f"{row['CampaignName']} - {row['RecencyCode']}; {row['FrequencyCode']}; {row['MonetaryCode']}"
        if pd.isna(row['Segment Name']) or row['Segment Name'] == ''
        else row['Segment Name'],
        axis=1
    )
    df['Segment Code'] = df.apply(
        lambda row: row['SourceCode'] if pd.isna(row['Segment Code']) or row['Segment Code'] == '' else row['Segment Code'],
        axis=1
    )
    program_types = ['Program', 'Fuse_Program']
    for program_type in program_types:
        if program_type in df.columns:
            program_map = (
                df.dropna(subset=[program_type])
                .groupby('CampaignCode')[program_type]
                .agg(lambda x: x.mode()[0]))
            df[program_type] = df.apply(
                lambda row: program_map.get(row['CampaignCode'], row[program_type]) if pd.notna(row['CampaignCode']) else row[program_type],
                axis=1)
    md_condition = (
        df['SourceCode'].str.lower().str.startswith(('xxx', 'onl')) &
        df['MailDate'].isna() &
        df['_CampaignCode'].notna() &
        df['_CampaignCode'].str.strip().ne('')
    )
    if '_CampaignCode' in df.columns:
        mail_date_mapping = df.dropna(subset=['MailDate']).set_index('_CampaignCode')['MailDate'].to_dict()
        fiscal_mapping = df.dropna(subset=['Fiscal']).set_index('_CampaignCode')['Fiscal'].to_dict()
        df.loc[md_condition, 'MailDate'] = df.loc[md_condition, '_CampaignCode'].map(mail_date_mapping)
        df.loc[md_condition, 'Fiscal'] = df.loc[md_condition, '_CampaignCode'].map(fiscal_mapping)
    return df


def dataset_filters(df, client):
    return df
