"""FFB data.parquet transform."""
import re
import pandas as pd
import numpy as np


def parse(df, client):
    df['DonorID'] = df['DonorID'].astype(str).str.strip()
    df['DonorID'] = df['DonorID'].str.replace(r'\.0$', '', regex=True)
    df = df.drop(columns=df.filter(like='¿"Contact Id"').columns, errors='ignore')
    df['SourceCode'] = df['Segment Code']
    print("SourceCode built from Segment Code")

    pattern = re.compile(r'^[A-Z]{2,3}[A-Z]{2}\d{3}$')
    df['CampaignCode'] = df.apply(
        lambda row: row['SourceCode'][3:10].upper()
        if isinstance(row['SourceCode'], str) and len(row['SourceCode']) >= 10 and pattern.match(row['SourceCode'][3:10].upper())
        else '',
        axis=1)
    print("CampaignCode updated based on SourceCode matching the updated pattern")

    for column in ['GiftID', 'Media Outlet Code']:
        if column in df.columns:
            df[column] = df[column].astype(str).replace(r'\.0$', '', regex=True)
            print(f"{column} is now a string and '.0' has been stripped where applicable")

    print('Completed FFB Client Specific')
    df['online'] = df['Original URL']
    df['program_type'] = df['Media Outlet Code']
    return df
