"""AH source code validation logic."""
import pandas as pd
import numpy as np


def _helper(df):
    col_name = 'SourceCode'
    if col_name in df.columns:
        df = df.rename(columns={col_name: 'PackageID'})
    df.PackageID = df.PackageID.str.strip().str.upper()
    cond = df.PackageID.str.len() > 4
    df['LastChar'] = df.PackageID.str[-1]
    df['_PackageID'] = np.where(cond, '___' + df.LastChar, df.PackageID)
    df['ListCode'] = np.where(cond, df.PackageID.str[1:4], None)
    df.CampaignCode = df.CampaignCode.str.strip()
    df.PackageID = df.PackageID.str.strip()
    df['_SourceCode'] = df.CampaignCode.fillna(value='X'*10) + df.PackageID.fillna(value='Y'*5)
    cond = (df.CampaignCode == df.PackageID)
    df['SourceCode'] = np.where(cond, df.CampaignCode + 'YYYY', df._SourceCode)
    df = df.drop_duplicates('SourceCode', keep='last')
    return df


def initiate_codes(df, client):
    return _helper(df)


def update_codes(df, client):
    mask = df.CampaignCode == ''
    return _helper(df.loc[~mask])
