"""FFB base table logic."""
import pandas as pd


def source_code_transform(sc, client):
    cols = [x for x in sc.columns if 'code' in x.lower()]
    sc[cols] = sc[cols].astype(str)
    return sc
