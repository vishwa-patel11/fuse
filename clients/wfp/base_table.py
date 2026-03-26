"""WFP base table logic."""
import pandas as pd
import numpy as np


def _wfp_truncate_to_bytes(s, max_bytes=510):
    s = str(s)
    return s[:255] if len(s) > 255 else s


def source_code_missing_transform(missing, client):
    for col in missing.select_dtypes(include=['object', 'string']).columns:
        missing[col] = missing[col].astype(str).apply(lambda x: _wfp_truncate_to_bytes(x, 510))
    missing = missing.replace({'None': None, 'nan': None, 'NaT': None})
    date_cols = ['mail_date']
    for col in date_cols:
        if col in missing.columns:
            missing[col] = pd.to_datetime(missing[col], errors='coerce')
    text_cols = ['campaign_name', 'source_code', 'campaign_code']
    for col in text_cols:
        if col in missing.columns:
            missing[col] = missing[col].astype(str).str.strip()
    missing = missing.replace({'None': None, 'nan': None})
    if 'fy' in missing.columns:
        missing['fy'] = missing['fy'].replace({'<NA>': None}).astype('Int64')
    if 'mail_date' in missing.columns:
        missing['mail_date'] = pd.to_datetime(missing['mail_date'], errors='coerce')
    if 'data_processed_at' in missing.columns:
        missing['data_processed_at'] = pd.to_datetime(missing['data_processed_at'], errors='coerce')
    if 'data_processed_at_og_sc' in missing.columns:
        missing['data_processed_at_og_sc'] = pd.to_datetime(missing['data_processed_at_og_sc'], errors='coerce')
    if 'quantity' in missing.columns:
        missing['quantity'] = missing['quantity'].fillna(np.nan).astype('Int64')
    return missing
