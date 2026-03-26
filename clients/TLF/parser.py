"""TLF data.parquet transform."""
import pandas as pd
import numpy as np


def parse(df, client):
    print('inside TLF client specific')
    if 'GiftAmount' in df.columns:
        df['Segment Code'] = df['Segment Code'].astype(str)
        df['DAF'] = df['DAF'].astype(str)
        df['IRA'] = df['IRA'].astype(str)
        df['GiftID'] = df['GiftID'].apply(lambda x: str(int(x)) if pd.notnull(x) else '')
        if 'Captured Scanline' in df.columns and 'Segment Code' in df.columns:
            df['Captured Scanline'] = df['Captured Scanline'].astype(str)
            extracted_scanline = df['Captured Scanline'].str[11:21]
            df['SourceCode'] = np.where(
                (df['Captured Scanline'].notna()) &
                (df['Captured Scanline'].str.strip() != '') &
                (extracted_scanline.str.len() == 10),
                extracted_scanline,
                df['Segment Code'])
        df.SourceCode = df.SourceCode.astype(str).str.upper()
        df['CampaignCode'] = df.SourceCode.astype(str).fillna(value='').str[:6]
        print('Completed TLF Client Specific')
    return df
