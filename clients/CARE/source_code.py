"""CARE source code validation logic."""
import pandas as pd
import numpy as np


def initiate_codes(df, client):
    df.SourceCode = df.SourceCode.str.replace(' ', '')
    mask = df.SourceCode.str.contains('nan').fillna(value=False)
    df = df.loc[~mask]
    df['LastChar'] = df.SourceCode.str[-1]
    df['SynthSC'] = df.SourceCode.str[:10] + df.LastChar + '0'
    df['SourceCode'] = np.where(
        df.SourceCode.str.endswith('0'),
        df.SourceCode,
        df.SynthSC
    )
    df.CampaignCode = df.CampaignCode.astype(str)
    df = df.dropna(axis=0, subset=['SourceCode'])
    return df.drop(['SynthSC', 'LastChar'], axis=1)


def update_codes(df, client):
    mask = df.SourceCode.notna()
    df = df.loc[mask]
    df.SourceCode = df.SourceCode.astype(str)
    df.SourceCode = df.SourceCode.str.replace(' ', '')
    mask = df.SourceCode.str.contains('nan').fillna(value=False)
    df = df.loc[~mask]
    df['CampaignCode'] = df.SourceCode.str[:6]
    if 'RFMCode' in df.columns:
        df['RFMCode'] = df['RFMCode'].fillna(value='').astype(str)
    df.SourceCode = df.SourceCode.str.replace(' ', '')
    df['LastChar'] = df.SourceCode.str[-1]
    conditions = [
        df['CampaignCode'].str.startswith('1924M'),
        df['CampaignCode'].str.startswith('1324M')
    ]
    choices = [
        df.SourceCode.str[:7] + df.LastChar + '0',
        df.SourceCode.str[:7] + df.LastChar + '0'
    ]
    default_choice = df.SourceCode.str[:10] + df.LastChar + '0'
    df['SynthSC'] = np.select(conditions, choices, default=default_choice)
    df['SourceCode'] = np.where(
        df.SourceCode.str.endswith('0'),
        df.SourceCode,
        df.SynthSC
    )
    df = df.dropna(axis=0, subset=['SourceCode'])
    df = df.drop(['SynthSC', 'LastChar'], axis=1)
    return df


def add_filters(df, client):
    old_d = {'1': 'DM Only', '2': 'Digital Only', '3': 'DM and Digital', '4': 'Other'}
    new_d = {'1': 'DM Only', '2': 'Digital Only', '3': 'DM and Other', '4': 'Other',
             '5': 'TM Only', '6': 'Digital and Other (no DM)'}
    df['SC_Channel_Behavior'] = np.where(
        df.SourceCode.str[2:4].astype(int) < 24,
        df.SourceCode.str[6].map(old_d),
        df.SourceCode.str[6].map(new_d)
    )
    if "PackageName" in df.columns:
        feature = "segment"
        package_col = 'PackageName'
        df[feature] = 'Other'
        segment_mapping = {
            'Sustainer': 'Sustainer', 'High Value': 'High Value', 'Maximize': 'Maximize',
            'Retain': 'Retain', 'No Gift': 'No Gift', 'Yes Gift': 'Yes Gift',
            'Landing Page': 'Landing Page', 'Whitemail': 'Whitemail'
        }
        for segment, keyword in segment_mapping.items():
            df.loc[df[package_col].str.contains(keyword, case=False, na=False), feature] = segment
    return df


def dataset_filters(df, client):
    df['Program'] = None
    mask = df.CampaignCode.str.startswith(('1224', '1924', '1323', '0823AK'))
    df.loc[mask, 'Program'] = 'PC'
    mask = df.CampaignCode.str.startswith(('1124', '1324'))
    df.loc[mask, 'Program'] = 'IC'
    if "PackageName" in df.columns:
        feature = "CARE_Segment"
        package_col = 'PackageName'
        df[feature] = 'Other'
        segment_mapping = {
            'Sustainer': 'Sustainer', 'High Value': 'High Value', 'Maximize': 'Maximize',
            'Retain': 'Retain', 'No Gift': 'No Gift', 'Yes Gift': 'Yes Gift',
            'Landing Page': 'Landing Page', 'Whitemail': 'Whitemail'
        }
        for segment, keyword in segment_mapping.items():
            df.loc[df[package_col].str.contains(keyword, case=False, na=False), feature] = segment
    return df
