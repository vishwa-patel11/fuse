"""TCI data.parquet transform."""
import os
import pandas as pd
import numpy as np


def parse(df, client):
    from common.utilities import Filemap, get_schema, fiscal_from_column

    filemap = Filemap(client)
    cols = [x for x in df.columns if 'unnamed' in x.lower()]
    df = df.drop(cols, axis=1)
    df.GiftID = df.GiftID.astype(str)
    df['GiftDate'] = pd.to_datetime(df['GiftDate'])
    mask = df['GiftDate'] >= '2019-07-01'
    cols = ['DonorID', 'GiftAmount', 'GiftDate']
    _df = df[mask].drop_duplicates(cols, keep='last')
    df = pd.concat([df[~mask], _df], ignore_index=True)

    ref = pd.read_excel(os.path.join(filemap.RAW, 'DonorID_Mapping', 'TCI All Records SF and RE ID.xlsx'))
    mask = ref['RE Constituent ID'].notna()
    ref = ref.loc[mask]
    cols = ['RE Constituent ID', 'SF Account ID']
    ref[cols] = ref[cols].astype(str)
    donor_map = dict(zip(ref['RE Constituent ID'], ref['SF Account ID']))
    df.DonorID = df.DonorID.astype(str)
    df['TempDonorID'] = df.DonorID.map(donor_map)
    df.DonorID = np.where(df.TempDonorID.isna(), df.DonorID, df.TempDonorID)
    df = df.drop('TempDonorID', axis=1)

    schema = get_schema('TCI')
    fy_month = schema["firstMonthFiscalYear"]
    df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', fy_month)
    df['SourceCode'] = df['Package']
    df['SC Determination'] = 'TRX File Raw'

    mask = (
        df['GiftSource'].eq('Fundraise Up') &
        (df['SourceCode'].isna() | (df['SourceCode'] == '') |
         (df['SourceCode'].astype(str).str.lower() == 'none')))
    df.loc[mask, 'SourceCode'] = (
        (df['GiftFiscal'].fillna('').astype(str) + df['AppealID'].fillna('') + df['GiftSource'].fillna(''))
        .str.upper().str.replace(' ', '', regex=False))
    df.loc[mask, 'SC Determination'] = 'Fundraise Up Logic'

    mask_mid = (
        (df['SourceCode'].isna() | (df['SourceCode'] == '') |
         (df['SourceCode'].astype(str).str.lower() == 'none')) &
        (~df['GiftSource'].eq('Fundraise Up')))
    df.loc[mask_mid, 'SourceCode'] = (
        (df['GiftFiscal'].fillna('').astype(str) + df['AppealID'].fillna(''))
        .str.upper().str.replace(' ', '', regex=False))
    df.loc[mask_mid, 'SC Determination'] = 'Blank SourceCode(Package) Logic'

    mask2 = (
        df['SourceCode'].notnull() &
        (df['SourceCode'].str.len() < 9) &
        (df['SourceCode'].str.startswith(('1Q', '1A'))))
    df.loc[mask2, 'SourceCode'] = (
        (df['GiftFiscal'].fillna('').astype(str).str.strip() +
         df['AppealID'].fillna('').astype(str).str.strip())
        .str.upper().str.replace(' ', '', regex=False))
    df.loc[mask2, 'SC Determination'] = 'Short SourceCode(Package) Logic'

    mask3 = (
        df['SourceCode'].notna() &
        (df['SourceCode'].str.len() == 9) &
        (df['SourceCode'].str.startswith(('1Q', '1A'))))
    df.loc[mask3, 'SC Determination'] = 'Fuse Campaign'

    mask_fuse_campaign = df['SC Determination'].eq('Fuse Campaign')
    if mask_fuse_campaign.any():
        df.loc[mask_fuse_campaign, 'CampaignCode'] = df.loc[mask_fuse_campaign, 'SourceCode'].str[:5]
    df["State"] = df["Preferred State Legacy"].fillna(df["Mailing State/Province"])
    df["City"] = df["Preferred City Legacy"].fillna(df["Mailing City"])
    df['AppealCategory'] = df['AppealID']
    print('TCI client specific complete')
    return df
