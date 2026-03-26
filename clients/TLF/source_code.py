"""TLF source code validation logic."""
import pandas as pd


def update_codes(df, client):
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
