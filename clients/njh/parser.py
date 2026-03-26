"""NJH data.parquet transform."""
import pandas as pd


def parse(df, client):
    cols = ["Gf_Appeal", "Gf_Package"]
    for c in cols:
        df[c] = df[c].fillna(value="")
    df['SourceCode'] = df.Gf_Appeal + df.Gf_Package
    print("NJH client specific columns successfully applied")
    return df
