"""U4U source code validation logic."""
import pandas as pd
import numpy as np


def update_codes(df, client):
    df.SourceCode = df.SourceCode.str.strip().str.upper().str.replace(' ', '')
    if "MailDate" in df.columns:
        df.loc[df['SourceCode'].str.startswith('A'), 'RFMCode'] = df['ListCode']
    if "GiftAmount" in df.columns:
        df['CampaignCode'] = np.where(
            df['CampaignCode'].isna() | (df['CampaignCode'] == ''),
            df['Appeal ID'], df['CampaignCode']
        )
    return df
