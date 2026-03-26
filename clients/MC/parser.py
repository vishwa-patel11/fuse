"""MC data.parquet transform."""
import pandas as pd
import numpy as np


def parse(df, client):
    print("=== Starting MC Client Specific Functions ===")
    from common.utilities import load_sc, get_schema, fiscal_from_column

    df['SustainerGiftFlag'] = np.where(df['CampaignID'].astype(str).isin(['147']), 'Sustainer', '1x')
    df['RecurringGift'] = np.where(df['CampaignID'].astype(str).isin(['147']), True, False)

    def _numeric_donor(_df, col='DonorID'):
        _df[col] = pd.to_numeric(_df[col], errors='coerce')
        _df = _df.dropna(subset=[col])
        _df[col] = _df[col].astype(str).str.replace(r'\.0', '', regex=True)
        return _df

    df = _numeric_donor(df)
    df['SourceCode'] = (
        df['CampaignCode'].fillna('').replace('None', '') +
        df['SegmentCode'].fillna('').replace('None', '') +
        df['PackageCode'].fillna('').replace('None', ''))
    df['SourceCode_OG'] = df['SourceCode']
    sc = load_sc('MC').sort_values('MailDate', ascending=False)
    prefix_map_mc = {
        "AH": "Appeal", "AB": "Appeal", "AG": "Appeal", "AN": "Appeal",
        "MA": "Appeal", "QQ": "Cold", "MQ": "Cold", "AW": "Welcome"
    }
    valid_sc_codes = set(sc['SourceCode'].astype(str))
    mask = (
        df['SourceCode'].str.startswith(tuple(prefix_map_mc.keys())) &
        df['CampaignID'].astype(str).isin(['143', '145']) &
        ~df['SourceCode'].isin(valid_sc_codes)
    )
    df.loc[mask, 'SourceType'] = df.loc[mask, 'SourceCode'].str[:2].map(prefix_map_mc)
    schema = get_schema('MC')
    fy_month = schema["firstMonthFiscalYear"]
    df['GiftFiscal'] = fiscal_from_column(df, 'GiftDate', fy_month)
    has_campaign_code = mask & df['CampaignCode'].notna()
    df.loc[has_campaign_code, 'SourceCode'] = df.loc[has_campaign_code, 'CampaignCode'].astype(str)
    fallback_mask = mask & df['CampaignCode'].isna()
    df.loc[fallback_mask, 'SourceCode'] = (
        df.loc[fallback_mask, 'GiftFiscal'].astype(str) +
        df.loc[fallback_mask, 'SourceType'] + "Unsourced")
    df['GiftDate'] = pd.to_datetime(df['GiftDate'])
    max_gift_date = df['GiftDate'].max()
    last_complete_month = max_gift_date.to_period('M') - 1
    if 'Sustainer Status' not in df.columns:
        df['Sustainer Status'] = "N/A"
    rg = df[df['RecurringGift'] == 1].copy()
    rg['gift_month'] = rg['GiftDate'].dt.to_period('M')
    rg = rg[rg['gift_month'] <= last_complete_month]
    rg['months_ago'] = (
        (last_complete_month.year - rg['gift_month'].dt.year) * 12 +
        (last_complete_month.month - rg['gift_month'].dt.month))
    recent_rg = rg.groupby('DonorID')['months_ago'].min().reset_index().rename(columns={'months_ago': 'months_since_last_recurring'})
    def classify_sustainer(m):
        if pd.isna(m):
            return "N/A"
        m = int(m)
        if m == 0:
            return "Active"
        elif 1 <= m <= 5:
            return "Lapsing- Short"
        elif 6 <= m <= 11:
            return "Lapsing-Long"
        return "Lapsed"
    recent_rg['Sustainer Status'] = recent_rg['months_since_last_recurring'].apply(classify_sustainer)
    status_map = recent_rg.set_index('DonorID')['Sustainer Status']
    df['Sustainer Status'] = df['DonorID'].map(status_map).fillna("N/A")
    df["CampaignID"] = df["CampaignID"].astype(str).str.replace(".0", "", regex=False).str.strip()
    DIGITAL_IDS = {"132", 132, "133", 133, "134", 134, "135", 135, "136", 136, "137", 137, "139", 139}
    DIRECT_MAIL_IDS = {"143", 143, "145", 145, "146", 146, "160", 160}
    DAF_STOCK_IRA_IDS = {"157", 157}
    SUSTAINER_IDS = {"147", 147}
    df["GiftChannel"] = "Other"
    df.loc[df["CampaignID"].isin(DIGITAL_IDS), "GiftChannel"] = "Digital"
    df.loc[df["CampaignID"].isin(DIRECT_MAIL_IDS), "GiftChannel"] = "Direct Mail"
    df.loc[df["CampaignID"].isin(DAF_STOCK_IRA_IDS), "GiftChannel"] = "DAF, Stock, IRA"
    df.loc[df["CampaignID"].isin(SUSTAINER_IDS), "GiftChannel"] = "Sustainer"
    SUBTYPE_MAP = {
        "132": "Email Receipts", "133": "Email Renewal", "134": "Email Prospect Conversion",
        "135": "Digital Marketing Paid Search", "136": "Digital Marketing Renewal",
        "137": "Digital Marketing Acquisition", "139": "Organic Website Visits",
        "143": "Direct Mail Renewal", "145": "Direct Mail Acquisition", "146": "Direct Mail Receipts",
        "147": "Sustainer", "148": "Telemarketing", "149": "Emergency Fundraising",
        "150": "Community Fundraising", "156": "Affinity Partnerships",
        "157": "DAFs, Stocks, IRAs", "159": "Texting", "160": "Organic Direct Mail"
    }
    df["GiftChannelSubtype"] = df["CampaignID"].map(SUBTYPE_MAP).fillna("Other")
    print("=== MC Client Specific Functions completed successfully ===")
    return df
