"""AH data.parquet transform."""
import pandas as pd
import numpy as np


def parse(df, client):
    df['SourceCode'] = df.CampaignCode.fillna(value='X'*10) + df.PackageID.fillna(value='Y'*5)
    return df
