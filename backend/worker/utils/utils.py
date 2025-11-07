
import os, re, json, shutil, unicodedata
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
import hashlib
from sqlalchemy import text
from backend.app.db.session import engine

def read_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv", ".txt"]:
        return pd.read_csv(path)
    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    else:
        raise ValueError("Unsupported input format. Use CSV or XLSX.")

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def norm_text_val(x: Optional[str]) -> Optional[str]:
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    s = str(x).strip().lower()
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    s = re.sub(r'\s+', ' ', s)
    return s

def norm_df_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(norm_text_val)
    return df

def to_date_col(df: pd.DataFrame, col: str, dayfirst: bool = True):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], dayfirst=dayfirst, errors="coerce")

def load_config(path: str) -> Dict[str, Any]:
    # Supports JSON; YAML if pyyaml present
    if path.lower().endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    elif path.lower().endswith((".yaml",".yml")):
        try:
            import yaml
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise RuntimeError(f"YAML requested but PyYAML not installed or parse error: {e}")
    else:
        raise ValueError("Config must be .json or .yaml")

def archive_raw(input_path: str, raw_dir: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.basename(input_path)
    dest = os.path.join(raw_dir, f"{ts}__{base}")
    shutil.copy2(input_path, dest)
    return dest

def write_log(log_dir: str, name: str, payload: str):
    ensure_dir(log_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(os.path.join(log_dir, f"{ts}__{name}.log"), "w", encoding="utf-8") as f:
        f.write(payload)

def upsert_csv(table_name, df, schema="warehouse", conflict_cols=["facility_id"]):
    date_cols = [
        "ngay_cap_gcn_attp", "ngay_cap_lan_2", "ngay_cap_dau_tien",
        "ngay_cap_dkkd", "thoi_han_gcn_attp"
    ]

    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
            df[c] = df[c].where(df[c].notna(), None)

    df = df.replace({np.nan: None, pd.NaT: None})

    cols = df.columns
    colnames = ", ".join(cols)
    placeholders = ", ".join([f":{c}" for c in cols])
    conflict = ", ".join(conflict_cols)

    

    sql = f"""
        INSERT INTO {schema}.{table_name} ({colnames})
        VALUES ({placeholders})
        ON CONFLICT ({conflict}) DO NOTHING;
    """

    # Insert từng batch
    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict(orient="records"))

def build_facility_id(row: pd.Series) -> str:
    # Prefer dkkd / attp; fallback to hash of normalized text parts
    for k in ["so_gcn_dkkd","so_gcn_attp"]:
        if k in row and pd.notna(row[k]) and str(row[k]).strip()!='':
            return f"fac::{str(row[k]).strip()}"
    t = str(row.get("ten_co_so", "")) + "|" + str(row.get("dia_chi","")) + "|" + str(row.get("quan_huyen",""))
    return f"fac::soft::{abs(hash(t))}"

def fsum(path, algo = "sha256") -> str:
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""): h.update(chunk)
    return h.hexdigest()