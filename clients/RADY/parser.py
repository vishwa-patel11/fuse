"""RADY data.parquet transform."""
import pandas as pd


def parse(df, client):
    print("RADY client-specific columns processing started")
    if 'Reference' in df.columns:
        df['Reference'] = df['Reference'].astype(str).fillna('none')
    if 'ZipCode' in df.columns:
        df['ZipCode'] = df['ZipCode'].astype(str)
    if 'CampaignCode' in df.columns and 'PackageCode' in df.columns:
        mask = (
            df['CampaignCode'].str.startswith('DMA', na=False) &
            (df['PackageCode'].isna() | (df['PackageCode'].str.strip() == '')))
        df.loc[mask, 'PackageCode'] = df.loc[mask, 'CampaignName']
    if 'CampaignCode' in df.columns and 'PackageCode' in df.columns:
        df['SourceCode'] = df.apply(
            lambda row: f"{row['CampaignCode']}-{row['PackageCode']}"
            if str(row['CampaignCode']).startswith(('DM', 'DR')) and pd.notna(row['PackageCode']) and row['PackageCode'] != ''
            else row['CampaignCode'] if str(row['CampaignCode']).startswith(('DM', 'DR'))
            else '', axis=1)
    if 'AppealID' in df.columns and 'CampaignCode' in df.columns:
        df['AppealID'] = df['AppealID'].fillna(df['CampaignCode'])
    cols = ['GiftDate', 'GiftAmount', 'DonorID']
    df = df.dropna(subset=[col for col in cols if col in df.columns])
    print("RADY client-specific columns successfully applied")
    return df
