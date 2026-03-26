"""WFP data.parquet transform."""
import pandas as pd


def parse(df, client):
    df.SourceCode = df.SourceCode.str.strip().str.replace('-', '')
    mask = df.GiftAmount.notna()
    return df.loc[mask]
