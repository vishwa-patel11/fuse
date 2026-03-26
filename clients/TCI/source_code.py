"""TCI source code validation logic."""
import pandas as pd
import numpy as np


def initiate_codes(df, client):
    df.SourceCode = df.SourceCode.str.strip().str.upper().str.replace(' ', '')
    mask = df.SourceCode.str.len() > 7
    df = df.loc[mask]
    mask = df.SourceCode.str.startswith('11Q')
    df['_SourceCode'] = np.where(mask, df.SourceCode.str[2:], df.SourceCode)
    mask = df.SourceCode.str.startswith('1Q')
    df['_SourceCode'] = np.where(mask, df.SourceCode.str[1:], df._SourceCode)
    return df


def update_codes(df, client):
    from common.utilities import load_sc

    df.SourceCode = df.SourceCode.str.strip().str.upper().str.replace(' ', '')
    mask = df.SourceCode.str.startswith('11Q')
    df['_SourceCode'] = np.where(mask, df.SourceCode.str[2:], df.SourceCode)
    mask = df.SourceCode.str.startswith('1Q')
    df['_SourceCode'] = np.where(mask, df.SourceCode.str[1:], df['_SourceCode'])
    sc_tci = load_sc('TCI').sort_values('MailDate', ascending=False)
    df = df.merge(
        sc_tci[['SourceCode', 'CampaignCode', 'CampaignName']],
        how='left', on='SourceCode', suffixes=('', '_sc')
    )
    if 'CampaignCode_sc' in df.columns:
        if 'CampaignCode' not in df.columns:
            df['CampaignCode'] = df['CampaignCode_sc']
        else:
            df['CampaignCode'] = df['CampaignCode'].fillna(df['CampaignCode_sc'])
    else:
        df['CampaignCode'] = df.get('CampaignCode', np.nan)
    df['CampaignCode'] = df['CampaignCode'].fillna(df['SourceCode'].str[:5])
    if 'CampaignName_sc' in df.columns:
        if 'CampaignName' not in df.columns:
            df['CampaignName'] = df['CampaignName_sc']
        else:
            df['CampaignName'] = df['CampaignName'].fillna(df['CampaignName_sc'])
    else:
        df['CampaignName'] = df.get('CampaignName', np.nan)
    df.drop(columns=['CampaignCode_sc', 'CampaignName_sc'], errors='ignore', inplace=True)
    return df


def dataset_filters(df, client):
    from common.source_code_validation import Fuse_Campaign_Lookup

    Fuse_Campaign_Lookup(df, client)
    return df
