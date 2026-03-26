"""RADY source code validation logic."""
import pandas as pd


def update_codes(df, client):
    if 'SourceCode' not in df.columns:
        df['SourceCode'] = ''
    if "MailDate" in df.columns:
        missing_campaigns = df.loc[~df['CampaignCode'].isin(df['SourceCode']), 'CampaignCode'].unique()
        new_rows = []
        for campaign in missing_campaigns:
            campaign_data = df[df['CampaignCode'] == campaign].iloc[0]
            new_rows.append({
                'SourceCode': campaign, 'CampaignCode': campaign,
                'CampaignName': campaign_data['CampaignName'],
                'PackageName': "Whitemail", 'MailDate': campaign_data['MailDate']
            })
        new_rows_df = pd.DataFrame(new_rows)
        df = pd.concat([df, new_rows_df], ignore_index=True)
    return df


def campperf_dfc_updates(df, client):
    from common.utilities import get_schema, fiscal_from_column

    schema = get_schema(client)
    if '_CampaignCode' not in df.columns:
        return df
    df.loc[df['_CampaignCode'].notna(), 'MailDate'] = (
        df.groupby('_CampaignCode')['MailDate'].transform('max')
    )
    df['Fiscal'] = fiscal_from_column(df, 'MailDate', schema['firstMonthFiscalYear'])
    return df
