"""MC base table logic."""
import pandas as pd


def after_gift_before_qa(gift, client):
    # from common.utilities import Filemap
    # filemap = Filemap(client)
    # gift.to_parquet(filemap.MASTER + 'gift.parquet')
    pass


def after_publish_bi_tables(client):
    from common.sql import get_curated_table_path, sql_exec_only
    donor_tbl = get_curated_table_path('mc_donor')
    sql_exec_only(
        f"UPDATE {donor_tbl} SET mc_donor984126 = CASE WHEN donor_id = '984126' THEN 1 ELSE 0 END"
    )
