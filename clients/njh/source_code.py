"""NJH source code validation logic."""
import pandas as pd
import numpy as np


def _create_helper_code(df):
    cond = (df.SourceCode.str[0] == 'D') & (df.SourceCode.str[0] != 'M')
    df["_TempColumn"] = np.where(cond, df.SourceCode.str[3:6], "___")
    df["_SourceCode"] = np.where(cond, df.SourceCode.str[:3] + "___", df.SourceCode)
    df._SourceCode = df._SourceCode + df._TempColumn
    df._SourceCode = np.where(cond, df._SourceCode, df._SourceCode.str[:-1] + df.SourceCode.str[-1])
    df['CampaignCode'] = df['SourceCode'].str[:3]
    cond = df.SourceCode.str.startswith("DM")
    df["_SourceCode"] = np.where(cond, "_________", df._SourceCode)
    return df


def initiate_codes(df, client):
    df.SourceCode = df.SourceCode.str.upper().str.replace(" ", "")
    df.MailDate = pd.to_datetime(df.MailDate)
    mask = df.MailDate.dt.year > 2019
    df = df.loc[mask]
    df = _create_helper_code(df)
    return df


def update_codes(df, client):
    if 'GiftAmount' in df.columns:
        df = df.rename(columns={'Gf_Appeal': 'CampaignCode'})
        df['SourceCode'] = df.CampaignCode + df.Gf_Package
    else:
        df = _create_helper_code(df)
    return df


def dataset_filters(df, client):
    from common.source_code_validation import Fuse_Campaign_Lookup

    Fuse_Campaign_Lookup(df, client)
    additional_revenue_condition = (
        df['Gf_Campaign'].isin(['Direct Mail FY2020', 'Direct Mail FY2021', 'Direct Mail FY2022',
                                'Direct Mail FY2023', 'Direct FY2024', 'Direct FY2025', 'Direct FY2026'])
        & df['CampaignName'].isna()
    )
    df.loc[additional_revenue_condition, 'CampaignName'] = 'Additional Revenue'
    rec_condition = additional_revenue_condition & df['SourceCode'].str.startswith('REC', na=False)
    df.loc[rec_condition, 'SourceCode'] = 'REC'
    mappings = {
        'Acknowledgments': {'source_codes': ['DM'], 'package_code': 'Acknowledgments'},
        'Misc/Unsolicited': {
            'source_codes': ['Unsolicited', 'DM Unsolicited', 'DM24WTGMADNAV', 'DM24MADR', 'DM24EDPMALAP',
                             'DW2500D2W', 'AN2312SSF', 'AF Unsolicited', 'AF Solicitation', 'DW2400D2W'],
            'package_code': 'Misc/Unsolicited'
        },
        'Monthly Giving': {'source_codes': ['REC', 'RECUR1', 'RECUR2'], 'package_code': 'Monthly Giving'},
        'New Directions': {'source_codes': ['DM2409NDF', 'AN2306NDS'], 'package_code': 'New Directions'},
        'Tribute': {
            'source_codes': ['DM2200TRIB', 'Tribute', 'DM24MWTDTRIBB', 'DM24WTGTRIB', 'DM24MWTDTRIBMEGA',
                             'DM24TRIBJC', 'DM24TRIBR', 'DM24TRIB'],
            'package_code': 'Tribute'
        }
    }
    for package_name, details in mappings.items():
        condition = (
            df['SourceCode'].isin(details['source_codes']) &
            (df['CampaignName'] == 'Additional Revenue')
        )
        df.loc[condition, 'PackageName'] = package_name
        df.loc[condition, 'PackageCode'] = details['package_code']
    carry_in_condition = (
        (df['CampaignName'] == 'Additional Revenue') &
        (df['PackageName'].isna())
    )
    df.loc[carry_in_condition, 'PackageName'] = 'Carry-In'
    df.loc[carry_in_condition, 'PackageCode'] = 'Carry-In'
    return df
