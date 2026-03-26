"""AFHU data.parquet transform."""
import os
import pandas as pd
import numpy as np


def _to_dbfs_local(p: str) -> str:
    if isinstance(p, str) and p.startswith("dbfs:/"):
        return p.replace("dbfs:/", "/dbfs/", 1)
    return p


def _read_csv_with_fallback(path: str, **kwargs) -> pd.DataFrame:
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except UnicodeDecodeError as e:
            last_err = e
    raise last_err


def parse(df, client):
    from common.utilities import Filemap
    filemap = Filemap(client)
    alumni_dir = _to_dbfs_local(os.path.join(filemap.MASTER, "Alumni"))

    # --- AppealCategory update ---
    condition = (df["AppealCategory"] == "Solicitation") & (df["Online Donations - Form Name"].notna())
    df["AppealCategory"] = np.where(condition, "Online", df["AppealCategory"])

    df["GiftID"] = df["GiftID"].astype("string")
    print("Appeal Category and Gift ID columns created")

    # --- Alumni column ---
    alumni_ids = set()
    if os.path.isdir(alumni_dir):
        for filename in os.listdir(alumni_dir):
            if not filename.lower().endswith(".csv"):
                continue
            file_path = _to_dbfs_local(os.path.join(alumni_dir, filename))
            try:
                alumni_df = _read_csv_with_fallback(
                    file_path,
                    usecols=lambda c: c.strip() == "ConsID" or c == "ConsID",
                    dtype={"ConsID": "string"},
                    low_memory=False,
                    on_bad_lines="skip",
                )
            except ValueError:
                continue
            if "ConsID" in alumni_df.columns:
                alumni_ids.update(alumni_df["ConsID"].dropna().astype(str).tolist())
    else:
        print(f"⚠️ Alumni directory not found or not accessible: {alumni_dir}")

    donor_ids = df["DonorID"].astype("string")
    df["alumni"] = np.where(donor_ids.isin(alumni_ids), "Yes", "No")
    print("Alumni column created")

    df["PaymentMethod"] = df["PaymentMethod"].astype("string")
    return df
