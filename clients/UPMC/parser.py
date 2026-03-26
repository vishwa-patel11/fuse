"""UPMC data.parquet transform."""
import pandas as pd


def parse(df, client):
    print('inside client specific')
    df['DonorID'] = df['DonorID'].astype(str).str.strip()
    df['DonorID'] = df['DonorID'].str.replace(r'\.0$', '', regex=True)
    df['GiftID'] = df['GiftID'].astype(str).str.strip()
    df['GiftID'] = df['GiftID'].str.replace(r'\.0$', '', regex=True)
    print('Client Specific Functions Completed')
    return df
