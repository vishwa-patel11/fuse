"""HKI source code validation logic."""
import calendar
import pandas as pd
import numpy as np


def update_codes(df, client):
    from common.utilities import get_schema, fiscal_from_column

    mask = df.CampaignCode.str.lower().str.contains('matcher').fillna(value=True)
    df = df.loc[~mask]
    m = {'CampaignCode': 7, 'RFMCode': 2, 'PackageCode': 2, 'ListCode': 3}
    for k, v in m.items():
        if k not in df.columns:
            df[k] = ''
        df[k] = [x.ljust(v, '_') for x in df[k].fillna(value='')]
    cols = ['CampaignCode', 'RFMCode', 'PackageCode', 'ListCode']
    if all(c in df.columns for c in cols):
        df['_SourceCode'] = df[cols].fillna('').astype(str).sum(axis=1)
    elif 'SourceCode' in df.columns:
        df['_SourceCode'] = df['SourceCode']
    df.SourceCode = df.SourceCode.str.replace(' ', '')
    cond = df.CampaignCode.str.startswith('W').fillna(value=False)
    df.CampaignCode = np.where(
        cond,
        df.CampaignCode,
        np.where(
            (df.CampaignCode.str.replace('_', '').str.len() == 6) &
            (df.CampaignCode.str.replace('_', '').str[-1].str.isdigit()),
            df.CampaignCode.str.replace('_', ''),
            df.CampaignCode.str.replace('_', '').str[:5]
        )
    )
    if 'GiftAmount' in df.columns:
        if 'Constituent Specific Attributes Major Donor Description' in df.columns:
            df = df.rename(columns={'Constituent Specific Attributes Major Donor Description': 'CSAMDD'})
        elif 'CSAMDD' not in df.columns:
            df['CSAMDD'] = ''
    if 'MailDate' in df.columns:
        schema = get_schema(client)
        df['Fiscal'] = fiscal_from_column(df, 'MailDate', schema['firstMonthFiscalYear'])
        df.loc[(df['Fiscal'] >= 2025) & (df['RFMCode'] == "__"), 'RFMCode'] = df['ListCode']
    if "MailDate" in df.columns:
        missing_campaigns = df.loc[~df['CampaignCode'].isin(df['SourceCode']), 'CampaignCode'].unique()
        new_rows = []
        for campaign in missing_campaigns:
            campaign_data = df[df['CampaignCode'] == campaign].iloc[0]
            new_rows.append({
                'SourceCode': campaign, 'CampaignCode': campaign,
                'CampaignName': campaign_data['CampaignName'],
                'PackageName': "Whitemail", 'PackageCode': "Whitemail",
                'MailDate': campaign_data['MailDate']
            })
        new_rows_df = pd.DataFrame(new_rows)
        df = pd.concat([df, new_rows_df], ignore_index=True)
    return df


def create_names(df, client):
    import calendar
    df['_month'] = df.CampaignCode.str[1:3]
    df._month = pd.to_numeric(df._month, errors='coerce').fillna(value=0).astype(int)
    def _lambda_calendar(x):
        try:
            return calendar.month_abbr[x]
        except Exception:
            return ''
    df['_month_name'] = df['_month'].apply(_lambda_calendar)
    df['_year'] = df.CampaignCode.str[3:5]
    df._year = pd.to_numeric(df._year, errors='coerce').astype('Int32')
    df._year = np.where(df._month > 6, df._year + 1, df._year)
    df['_name'] = 'FY' + df._year.astype(str) + '_' + df._month_name + '_' + df.CampaignCode.str[0]
    df.CampaignName = np.where(df.CampaignName.isna(), df['_name'], df.CampaignName)
    return df.drop(['_month', '_month_name', '_year', '_name'], axis=1)


def initiate_codes(df, client):
    df = df.dropna(axis=0, subset=['CampaignCode'])
    df['_CampaignCode'] = df.CampaignCode
    df['PackageCode'] = df['PackageCode'].str.replace('`', '').fillna(value='')
    df['PackageCode'] = df.apply(lambda row: row['PackageCode'].replace(row['CampaignCode'], ''), axis=1)
    df['SourceCode'] = df['CampaignCode'] + df['PackageCode']
    df.SourceCode = df.SourceCode.str.upper().str.replace(' ', '')
    cond = df['Appeal Category'].str.upper() == 'ACQUISITION'
    df['_List'] = np.where(cond, df.PackageCode.str[:-1], '')
    df['_Package'] = np.where(cond, df.PackageCode.str[-1], 1)
    df['_RFM'] = np.where(cond, '', df.PackageCode.str[:2])
    df._Package = np.where(cond, df._Package, df.PackageCode.str[2])
    cond = df._List.str.len() > 3
    df._List = np.where(cond, df.PackageCode.str[:-2], df._List)
    df._Package = np.where(cond, df.PackageCode.str[-2:], df._Package)
    m = {'_CampaignCode': 7, '_RFM': 2, '_Package': 2, '_List': 3}
    for k, v in m.items():
        df[k] = [x.ljust(v, '_') for x in df[k].fillna(value='')]
    cols = ['_CampaignCode', '_RFM', '_Package', '_List']
    df['_SourceCode'] = df[cols].fillna('').astype(str).sum(axis=1)
    df['_CLCode'] = df._CampaignCode + df._List
    mask = df._List == '___'
    _df = df.loc[~mask]
    cols = ['_CLCode', 'ListName']
    _df = _df[cols].drop_duplicates()
    mapper = dict(zip(_df._CLCode, _df.ListName))
    df.ListName = df._CLCode.map(mapper)
    df = df.drop('_CLCode', axis=1)
    return df.drop_duplicates('SourceCode', keep='last')
