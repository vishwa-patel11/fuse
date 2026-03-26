"""NJH base table logic."""
import pandas as pd
import numpy as np


def donor_csl(donor, client):
    null_cols = [col for col in donor.columns if donor[col].isnull().any()]
    donor[null_cols] = donor[null_cols].fillna('null')
    donor = donor.groupby(
        [col for col in donor.columns if col != 'patient'],
        as_index=False
    )['patient'].max()
    donor[null_cols] = donor[null_cols].replace('null', np.nan)
    return donor
