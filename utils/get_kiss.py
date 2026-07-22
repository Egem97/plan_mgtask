import os
import time
import pandas as pd
import requests
from utils.get_token import load_config

config = load_config()

SUBDOMAIN = config['kissflow']['subdomain']
PAGE_SIZE = 1000
MAX_PAGES = 10_000
TIMEOUT = 30
RETRY_ON_429 = 3
ACCOUNT_ID = config['kissflow']['account_id']
ACCESS_KEY_ID = config['kissflow']['access_key_id']
ACCESS_KEY_SECRET = config['kissflow']['access_key_secret']

def fetch_page(page_number: int, page_size: int, dataset: str) -> tuple[list[dict], dict]:
    """Devuelve (rows, meta) para una página. Reintenta en 429."""
    params = {"page_number": page_number, "page_size": page_size}
    url = f"https://{SUBDOMAIN}.kissflow.com/dataset/2/{ACCOUNT_ID}/{dataset}/list"
    headers = {"Accept": "application/json","X-Access-Key-Id": ACCESS_KEY_ID,"X-Access-Key-Secret": ACCESS_KEY_SECRET}
    for intento in range(RETRY_ON_429 + 1):
        r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
        if r.status_code == 429:
            espera = 2 ** intento
            print(f"  429 rate-limit en page {page_number}; reintento en {espera}s")
            time.sleep(espera)
            continue
        break
    r.raise_for_status()
    payload = r.json()
    if isinstance(payload, dict):
        rows = payload.get("Data") or payload.get("data") or []
        meta = {k: v for k, v in payload.items() if k.lower() != "data"}
    else:
        rows = payload
        meta = {}
    return rows, meta

def fetch_all_kissflow(dataset: str) -> pd.DataFrame:
    all_rows: list[dict] = []
    for page in range(1, MAX_PAGES + 1):
        rows, meta = fetch_page(page, PAGE_SIZE, dataset)
        all_rows.extend(rows)
        if len(rows) < PAGE_SIZE:
            break
    else:
        print(f"Alcanzado MAX_PAGES={MAX_PAGES}; posible truncamiento.")

    if not all_rows:
        return pd.DataFrame()

    df = pd.json_normalize(all_rows, max_level=0)

    for col in df.columns:
        sample = df[col].dropna().head(1)
        if not sample.empty and isinstance(sample.iloc[0], dict):
            df[col] = df[col].apply(
                lambda v: v.get("Name") if isinstance(v, dict) else v
            )

    if "_id" in df.columns:
        df = df[["_id", *[c for c in df.columns if c != "_id"]]]

    cols_a_revisar = [c for c in df.columns if c != "_id"]
    
    if cols_a_revisar:
        as_str = df[cols_a_revisar].astype(str)
        mask_dummy = as_str.apply(
            lambda col: col.str.contains(r"value\d*|Text data", case=False, regex=True, na=False)
        ).any(axis=1)
        df = df.loc[~mask_dummy].reset_index(drop=True)
    df = df.drop(columns=["_id","Name"])
    return df