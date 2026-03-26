"""HKI data.parquet transform."""
import re
import pandas as pd
import numpy as np


def _check_col(col, pattern):
    r = re.compile(pattern)
    regmatch = np.vectorize(lambda x: bool(r.match(x)))
    return regmatch(col)


def parse(df, client):
    print('inside client specific')
    OLD_ID = 198024
    NEW_ID = 54545
    df.DonorID = np.where(df.DonorID == OLD_ID, NEW_ID, df.DonorID)
    df['AppealCategory'] = df['AppealCategory'].fillna(value='Blank')
    mask = (df.AppealID.isin(['H0222NS'])) & (df.PackageID.str.endswith('B'))
    df.PackageID = np.where(mask, df.PackageID.str[:-1] + 'N', df.PackageID)
    cols = ['AppealID', 'PackageID']
    for col in cols:
        df[col] = df[col].str.replace('NS', '')
        df[col] = df[col].str.upper().str.replace(" ", "", regex=False).fillna("")
    pattern = '^[A-Z][0-9]{4}[A-Z]{0,2}$'
    c1 = _check_col(df['AppealID'], pattern)
    pattern = '^[W][A-Z0-9]{4,7}$'
    c2 = _check_col(df['AppealID'], pattern)
    cond1 = (c1) | (c2)
    df['_aid'] = np.where(cond1, df['AppealID'], '')
    cond2 = df.apply(lambda x: x['PackageID'].startswith(x['_aid']), axis=1)
    df['SourceCode'] = np.where(cond2, df['PackageID'], df['_aid'] + df['PackageID'])
    df.SourceCode = np.where(cond1, df.SourceCode, '')
    df = df.drop('_aid', axis=1)
    df['DonorID'] = df['DonorID'].apply(lambda x: str(int(x)) if pd.notnull(x) else np.nan)
    df['Soft Credit Constituent ID'] = df['Soft Credit Constituent ID'].apply(lambda x: str(int(x)) if pd.notnull(x) else np.nan)
    df['DonorID'] = df.apply(
        lambda row: row['Soft Credit Constituent ID'] if pd.notnull(row['Soft Credit Constituent ID']) else row['DonorID'],
        axis=1)
    df["GiftAmount"] = df["GiftAmount"].astype("string").str.replace(r"[\$,]", "", regex=True).astype("float64")
    for column in ['Luminate Online Gift ID', 'GiftID', 'GiftIDDescription']:
        if column in df.columns:
            df[column] = df[column].astype(str).replace(r'\.0$', '', regex=True)
    df['SourceCode'] = df['SourceCode'].where(df['SourceCode'].notna() & (df['SourceCode'] != ''), df['PackageID'])
    print("Client Specific for HKI Completed")
    return df
