"""CHOA data.parquet transform."""
import pandas as pd
import numpy as np


def parse(df, client):
    df['CampaignID'] = df['AppealID']
    df['SourceCode'] = df.AppealID + df.PackageID.astype(str).str[:4].replace('None', '') + df['RFM Code'].astype(str).str.replace('None', '')
    df["SourceCode"] = df["SourceCode"].str.replace(r"\.0$", "", regex=True)
    cond = df.SourceCode.str.startswith(('DM', 'SO', 'CR')).fillna(value=False)
    df['CampaignCode'] = np.where(cond, df.SourceCode.str[:7], None)
    cond = (df.AppealID == 'CR0722') | (df.AppealID == 'SO0822')
    df.CampaignCode = np.where(cond, df.AppealID, df.CampaignCode)
    df.CampaignCode = np.where(df.CampaignCode.isna(), df.AppealID, df.CampaignCode)
    mask = df['Gf_SCMGFlag'].isin(["Soft Credit Recipient"])
    return df.loc[~mask]
