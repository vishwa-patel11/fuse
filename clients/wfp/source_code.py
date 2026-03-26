"""WFP source code validation logic."""
import pandas as pd
import numpy as np


def add_filters(df, client):
    return df


def initiate_codes(df, client):
    df.SourceCode = df.SourceCode.str.strip().str.replace('-', '').str.upper()
    mask = df.SourceCode.str.contains('SEED')
    df = df.loc[~mask]
    mask = df.SourceCode.duplicated()
    codes = df.loc[mask]['SourceCode'].unique()
    mask = df.SourceCode.isin(codes)
    _df = df.loc[mask].groupby('SourceCode')['Quantity'].sum().to_frame(name='NewQuantity')
    df = pd.merge(df, _df, on='SourceCode', how='left')
    df.Quantity = np.where(mask, df.NewQuantity, df.Quantity)
    df = df.drop(['NewQuantity'], axis=1).drop_duplicates('SourceCode')
    return df


def update_codes(df, client):
    if 'GiftAmount' in df.columns:
        df.CampaignCode = np.where(
            df.CampaignCode.isna(),
            df.SourceCode.str[:4],
            df.CampaignCode
        )
    return df.rename(columns={'Campaign Name': 'CampaignName'})


def dataset_filters(df, client):
    from common.source_code_validation import Fuse_Campaign_Lookup

    Fuse_Campaign_Lookup(df, client)
    return df
