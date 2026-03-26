"""FWW data.parquet transform."""
import os
import pandas as pd
import numpy as np


def parse(df, client):
    from common.utilities import Filemap, format_currency

    print('inside FWW client specific')
    filemap = Filemap(client)
    df.columns = [x.replace(' ', '') for x in df.columns]
    cols = [x for x in df.columns if 'Unnamed' not in x]
    df = df[cols]
    f = 'segment codes for migrated recurring gifts.csv'
    _df = pd.read_csv(os.path.join(filemap.RAW, 'Historical', f))
    col_map = {'Recurring Donation: Segment Code': 'SegmentCode', 'Opportunity ID': 'GiftID'}
    cols = ['GiftID', 'SegmentCode']
    _df = _df.rename(columns=col_map)[cols]
    df = pd.merge(df, _df, on='GiftID', how='left')
    del _df
    condition = df['SourceCode'].isna()
    df['SourceCode'] = np.where(condition, df['SegmentCode'], df['SourceCode'])
    df = df.drop('SegmentCode', axis=1)
    file = 'Adding contact ID.xlsx'
    sheet = 'FWW_TransactionHistoryWithNullC'
    cols = ['Primary Contact: 18 Digit Contact ID', 'Opportunity ID']
    _df = pd.read_excel(os.path.join(filemap.RAW, 'Historical', file), sheet_name=sheet, usecols=cols).dropna()
    col_map = {'Primary Contact: 18 Digit Contact ID': 'NewContactID', 'Opportunity ID': 'GiftID'}
    _df = _df.rename(columns=col_map)
    df = pd.merge(df, _df, on='GiftID', how='left')
    del _df
    condition = df['DonorID'].isna()
    df['DonorID'] = np.where(condition, df['NewContactID'], df['DonorID'])
    df = df.drop('NewContactID', axis=1)
    file = 'Partial Soft Credit Transactions fuse-2022-08-05-09-22-36.csv'
    _df = pd.read_csv(os.path.join(filemap.RAW, 'Historical', file))
    col_map = {'Opportunity: Opportunity ID': 'GiftID', '18 Digit Contact ID': 'NewContactID', 'Amount': 'GiftAmount'}
    cols = ['GiftID', 'Partial Soft Credit: ID', 'GiftAmount', 'NewContactID']
    _df = _df.rename(columns=col_map)[cols]
    soft_credits = _df['GiftID'].unique()
    cols = ['GiftAmount', 'DonorID']
    _df = pd.merge(_df, df.drop(cols, axis=1), on='GiftID', how='left')
    _df = _df.drop('GiftID', axis=1)
    col_map = {'Partial Soft Credit: ID': 'GiftID', 'NewContactID': 'DonorID'}
    _df = _df.rename(columns=col_map)
    mask = df['GiftID'].isin(soft_credits)
    df = df.loc[~mask]
    df = pd.concat([df, _df])
    del _df
    def _extract_campaign_codes(s):
        try:
            return '-'.join(s.split('-')[:2])
        except Exception:
            return None
    df['CampaignCode'] = [_extract_campaign_codes(s) for s in df['SourceCode']]
    df.columns = [x.replace(' ', '') for x in df.columns]
    df['GiftAmount'] = format_currency(df['GiftAmount'])
    mask = df['GiftAmount'] > 0
    df = df.loc[mask]
    file = 'FWW_Members_Category_Code_Substitutions.csv'
    FWW_subs_df = pd.read_csv(os.path.join(filemap.RAW, 'Historical', file))
    mapping = FWW_subs_df.set_index('Opportunity ID')['Substitute Members Code Category']
    df['MembersCodeCategory'] = df['GiftID'].map(mapping).combine_first(df['MembersCodeCategory'])
    print('Completed client specific in parser')
    return df
