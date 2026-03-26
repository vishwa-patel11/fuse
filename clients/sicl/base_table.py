"""SICL base table logic."""
import pandas as pd


def donor_post_process(donor, trx, client):
    m = trx['gift_type'].astype(str).str.contains('Membership', case=False, na=False)
    ct = pd.crosstab(trx['donor_key'], m).reindex(columns=[False, True], fill_value=0)
    labels = pd.Series('Member/Non-Member', index=ct.index)
    labels[(ct[True] > 0) & (ct[False] == 0)] = 'Membership Only'
    labels[(ct[True] == 0) & (ct[False] > 0)] = 'Non-Membership Only'
    if 'donor_group' not in donor.columns:
        donor['donor_group'] = pd.NA
    donor['donor_group'] = donor['donor_key'].map(labels).fillna(donor['donor_group'])
    return donor
