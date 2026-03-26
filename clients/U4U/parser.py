"""U4U data.parquet transform."""
import os
import pandas as pd
import numpy as np


def parse(df, client):
    from common.utilities import Filemap

    df['SourceCode'] = df['Package ID']
    df['CampaignCode'] = ''
    sc_counts = df.groupby('SourceCode')['Appeal ID'].nunique()
    multi_appeal_sc = sc_counts[sc_counts > 1].index
    df_fixed = df.copy()
    for sc in multi_appeal_sc:
        mask = df_fixed['SourceCode'] == sc
        appeal_ids = df_fixed[mask]['Appeal ID'].unique()
        for i, appeal_id in enumerate(appeal_ids):
            if i == 0:
                continue
            appeal_mask = mask & (df_fixed['Appeal ID'] == appeal_id)
            df_fixed.loc[appeal_mask, 'SourceCode'] = f"{sc}_{i+1}"
    df = df_fixed.copy()

    cid = df["Campaign ID"].astype(str).str.lower().str.replace(r"[^a-z0-9]+", "", regex=True)
    direct_mail_keys = ["directmail", "mailacquisition", "mailretention"]
    digital_keys = ["digitalacquisition", "digitalads", "digitalretention", "email", "website"]
    other_keys = ["event", "hightouch", "sustainerexisting", "whitemail", "otherretention",
                  "majorgiving", "boardgiveget", "legacygiving", "thirdparty"]
    direct_pat = "|".join(direct_mail_keys)
    digital_pat = "|".join(digital_keys)
    other_pat = "|".join(other_keys)

    try:
        from common.transfer_files import get_sharepoint_context_app_only, get_folder_files
        SHAREPOINT_URL_U4U = "https://mindsetdirect.sharepoint.com/sites/ClientFiles"
        filemap_sp = Filemap('Fuse')
        import json
        with open(os.path.join(filemap_sp.SCHEMA, 'schema.json'), 'r') as f:
            USER = json.load(f)['SharePointUID']
        client_context_U4U = get_sharepoint_context_app_only(SHAREPOINT_URL_U4U)
        files = get_folder_files(client_context_U4U, 'U4U/Reporting 2.0')
        from office365.sharepoint.files.file import File
        dm_appeal_file = next(f for f in files if os.path.basename(f).startswith("DM Appeal IDs") and f.lower().endswith(('.xlsx', '.xls')))
        response = File.open_binary(client_context_U4U, dm_appeal_file)
        import io
        bytes_file = io.BytesIO(response.content)
        dm_appeal_df = pd.read_excel(bytes_file)
        direct_mail_appeals = set(dm_appeal_df.iloc[:, 0].dropna())
        appeal_is_direct = df["Appeal ID"].isin(direct_mail_appeals)
    except Exception:
        appeal_is_direct = df["Appeal ID"].isin([])

    df["GiftChannel"] = np.select(
        [appeal_is_direct,
         cid.str.contains(direct_pat, na=False),
         cid.str.contains(digital_pat, na=False),
         cid.str.contains(other_pat, na=False)],
        ["Direct Mail", "Direct Mail", "Digital", "Other"],
        default="N/A")
    df['gift_channel_subtype'] = df['Campaign ID']
    df['gift_channel_detail'] = df['Appeal ID']
    print('U4U client specific function complete')
    return df
