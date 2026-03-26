"""FS source code validation logic."""
import pandas as pd
import numpy as np


def update_codes(df, client):
    df['SourceCode'] = df['SourceCode'].str.replace('ZZZZ', '', regex=False)
    return df


def add_raw_sc_updates(df, client):
    if client != "FS":
        return df
    df = df.copy()
    fy = (
        df["FY (SC)"]
        .dropna()
        .astype("Int64")
        .astype(str)
        .reindex(df.index, fill_value="")
    )
    prog = df["Program (SC)"].fillna("").astype(str).str.strip()
    df["_unsourced_sc"] = fy + prog + "Unsourced"
    candidates = (
        df[["Client", "FY (SC)", "Program (SC)", "_unsourced_sc"]]
        .drop_duplicates(subset=["Client", "FY (SC)", "Program (SC)"], keep="first")
    )
    existing_sc = set(df["SourceCode"].fillna("").astype(str).str.strip())
    to_add = candidates[~candidates["_unsourced_sc"].isin(existing_sc)].copy()
    if to_add.empty:
        df.drop(columns="_unsourced_sc", errors='ignore', inplace=True)
        return df
    to_add["SourceCode"] = to_add["_unsourced_sc"]
    to_add.drop(columns="_unsourced_sc", inplace=True)
    df.drop(columns="_unsourced_sc", inplace=True)
    return pd.concat([df, to_add], ignore_index=True)
