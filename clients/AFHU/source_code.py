"""AFHU source code validation logic."""
import pandas as pd


def initiate_codes(df):
    df.SourceCode = df.SourceCode.str.replace(' ', '').str.upper()
    df['CampaignCode'] = df.SourceCode.str[:5]
    df['RFMCode'] = df.SourceCode.str[5:9]
    return df


def update_codes(df, client):
    from common.utilities import load_sc
    df.SourceCode = df.SourceCode.str.replace(' ', '').str.upper()
    print("SourceCode spaces removed and uppercased")

    if 'CampaignCode' not in df.columns:
        df['CampaignCode'] = None
    df.loc[df['CampaignCode'].isna() | (df['CampaignCode'] == ''), 'CampaignCode'] = df['SourceCode'].str[:5]
    print("Missing CampaignCodes Updated with first 5 digits of SourceCode")

    if "PackageID" in df.columns:
        df["PackageID"] = (
            df["PackageID"]
            .astype("string")
            .str.replace(r"\.0$", "", regex=True)
        )
        print("✅ .0 removed from end of PackageID")

    if "PackageCode" in df.columns:
        df["PackageCode"] = (
            df["PackageCode"]
            .astype("string")
            .str.replace(r"\.0$", "", regex=True)
        )
        print("✅ .0 removed from end of PackageCode")

    if 'GiftAmount' in df.columns:
        sc_check = load_sc('AFHU')
        df['SourceCode_Original'] = df['SourceCode']
        df['SourceCode'] = df.apply(
            lambda row: row['PackageID']
            if ((pd.isna(row['SourceCode']) or row['SourceCode'].strip() == '') or row['SourceCode'] not in sc_check['SourceCode'].values)
            and pd.notna(row['PackageID']) and row['PackageID'] != ''
            else row['SourceCode'],
            axis=1
        )
        print("WHITEMAIL SOURCECODE updated")

    return df


def dataset_filters(df, client):
    from common.source_code_validation import Fuse_Campaign_Lookup
    Fuse_Campaign_Lookup(df, client)
    print("Fuse Campaign Lookup applied")

    df.loc[(df['fuse_tracked_campaign'] == True) & (df['AppealCategory'].isnull() | (df['AppealCategory'] == "")), 'AppealCategory'] = "Direct Mail"
    print("Direct Mail added to AppealCategory for Fuse campaigns with no returns")

    print("AFHU dataset filters applied")
    return df
