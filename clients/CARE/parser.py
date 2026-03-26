"""CARE data.parquet transform."""
import pandas as pd
import numpy as np


def _convert_sn(s):
    try:
        return str(int("{:.0f}".format(float(s))))
    except Exception:
        return s


def parse(df, client):
    print('inside client specific')
    df.SourceCode = [_convert_sn(s) for s in df.SourceCode]
    mask = df.GiftAmount.notna()
    df = df.loc[mask]
    print("Source Code updates complete")

    df['SourceCode'] = df['SourceCode'].fillna(df['CampaignCode'])
    df['DonorID'] = df['DonorID'].astype(str).str.strip()
    df['DonorID'] = df['DonorID'].str.replace(r'\.0$', '', regex=True)
    print('DonorID updated to string')

    if 'Legacy SF ID' in df.columns:
        df['GiftID'] = df['GiftID'].fillna(df['Legacy SF ID'].astype(str) + '_legacy')
    print('Blank GiftIDs filled with Legacy SF IDs if applicable')

    print('Client Specific Functions Completed')
    return df
