"""CHOA source code validation logic."""
import pandas as pd
import numpy as np


def initiate_codes(df, client):
    df['_SourceCode'] = np.where(
        df.CampaignCode.str[-1] == 'A',
        df.SourceCode.str[:11] + '___' + df.SourceCode.str[11:],
        df.SourceCode + '___'
    )
    return df


def update_codes(df, client):
    if "GiftAmount" not in df.columns:
        df = df.rename(columns={"Quantity": "_Quantity"})
        _df = df.groupby('SourceCode')['_Quantity'].sum().to_frame(name='Quantity')
        df = pd.merge(df, _df, on='SourceCode', how='left')
        df = df.drop_duplicates('SourceCode')
        m1 = (df.CampaignCode.isna()) | (df.CampaignCode == '') | (df.CampaignCode == ' ')
        m2 = (df.PackageCode.isna()) | (df.PackageCode == '') | (df.PackageCode == ' ')
        mask = (m1) | (m2)
        df = df.loc[~mask]
        _df = df.drop_duplicates(['CampaignCode', 'PackageCode'])
        _df.loc[:, 'RFMCode'] = 'XXX'
        _df.loc[:, 'SourceCode'] = _df.CampaignCode + _df.PackageCode + _df.RFMCode
        cols = ['SourceCode', 'CampaignCode', 'PackageCode', 'RFMCode', 'CampaignName', 'PackageName',
                'PackageCPP', 'MailDate']
        df = pd.concat([df, _df[cols]])
        cols = ['CampaignCode', 'CampaignName', 'MailDate']
        _df = df[cols].drop_duplicates('CampaignCode')
        _df['PackageCode'] = 'LAND'
        _df['RFMCode'] = 'XXX'
        _df['SourceCode'] = _df.CampaignCode + _df.PackageCode + _df.RFMCode
        df = pd.concat([df, _df])
    if 'GiftAmount' in df.columns:
        df.SourceCode = df.SourceCode.str.ljust(14, 'X')
        m = {'DM Acquisition List': 'ListCode'}
        df = df.rename(columns=m)
        df.SourceCode = np.where(
            df.AppealID.str.endswith('A'),
            df.AppealID.astype(str).str.ljust(7, 'Y') + df.PackageID.astype(str).str.ljust(4, 'X') + df.ListCode.astype(str).str.ljust(3, 'X'),
            df.SourceCode
        )
    df['_SourceCode'] = np.where(
        df.CampaignCode.str[-1] == 'A',
        df.SourceCode.str[:11] + '___' + df.SourceCode.str[11:],
        df.SourceCode + '___'
    )
    cols = ['ListCode', 'SourceCode']
    for col in cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.replace(r"\.0+$", "", regex=True)
    df.SourceCode = df.SourceCode.str.upper()
    if 'GiftAmount' in df.columns:
        mask = df.SourceCode.str[7:11] == 'LAND'
        df.PackageID = np.where(mask, 'LAND', df.PackageID)
    else:
        mask = df.SourceCode.str[7:11] == 'LAND'
        df.PackageCode = np.where(mask, 'LAND', df.PackageCode)
    return df


def add_filters(df, client):
    df['Channel'] = None
    mask = ((df.CampaignCode.str.startswith('DM')) & (df.CampaignCode.str.endswith(('R', 'E')))).fillna(value=False)
    df.loc[mask, 'Channel'] = 'Renewal'
    mask = ((df.CampaignCode.str.startswith('DM')) & (df.CampaignCode.str.endswith('L'))).fillna(value=False)
    df.loc[mask, 'Channel'] = 'Lapsed'
    mask = ((df.CampaignCode.str.startswith('DM')) & (df.CampaignCode.str.endswith('A'))).fillna(value=False)
    df.loc[mask, 'Channel'] = 'ACQ'
    mask = ((df.CampaignCode.str.startswith('SO')) & (df.CampaignCode.str.endswith(('S', 'R', 'W')))).fillna(value=False)
    df.loc[mask, 'Channel'] = 'Leadership'
    mask = ((df.CampaignCode.str.startswith('CR')) & (df.CampaignCode.str.endswith(('T', 'F')))).fillna(value=False)
    df.loc[mask, 'Channel'] = 'CAT/Aflac'
    mask = df.CampaignCode.str.startswith('CRD').fillna(value=False)
    df.loc[mask, 'Channel'] = 'CAT/Aflac'
    return df


def dataset_filters(df, client):
    from common.source_code_validation import GiftFiscal
    return GiftFiscal(df, 'CHOA')
