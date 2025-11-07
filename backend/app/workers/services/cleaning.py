import io, json, pandas as pd, datetime as dt, re
from urllib.parse import urlparse
from app.utils.minio_client import make_minio, put_bytes, ensure_bucket
from app.services.log_service import log_ingest
from app.db.session import SessionLocal

def normalize_columns(cols):
    def norm(s):
        s = str(s).strip().lower()
        s = re.sub(r"\s+", "_", s).replace("đ","d")
        return s
    return [norm(c) for c in cols]

def clean_data_service(raw_uri: str, *, source: str, cfg: dict | None, cfg_uri: str | None) -> str:
    client = make_minio()
    p = urlparse(raw_uri)
    obj = client.get_object(p.netloc, p.path.lstrip("/"))
    raw = obj.read(); obj.close(); obj.release_conn()

    # load config (ưu tiên cfg đã truyền)
    if cfg is None and cfg_uri:
        cp = urlparse(cfg_uri)
        cobj = client.get_object(cp.netloc, cp.path.lstrip("/"))
        cfg = json.loads(cobj.read().decode("utf-8"))
        cobj.close(); cobj.release_conn()
    cfg = cfg or {}

    file_cfg = cfg.get("file", {})
    fmt = (file_cfg.get("format") or "csv").lower()
    if fmt == "csv":
        df = pd.read_csv(io.BytesIO(raw))
    elif fmt in ("xlsx","excel"): df = pd.read_excel(io.BytesIO(raw), header=file_cfg.get("header_row", 0))
    else: df = pd.read_json(io.BytesIO(raw))

    # map + defaults + types + transforms (rút gọn)
    df.columns = normalize_columns(df.columns)
    mapping = {k: v for k, v in (cfg.get("column_map") or {}).items()}
    df = df.rename(columns=mapping)

    # defaults
    defaults = cfg.get("defaults", {})
    out = df.copy()
    for col, val in defaults.items():
      fill_value = None
      if isinstance(val, str) and val.startswith("@now:"):
        fmt = val.split(":", 1)[1]
        fill_value = dt.datetime.now().strftime(fmt)
      else:
        fill_value = val
      if col not in out.columns:
        out[col] = fill_value
      else:
         out[col] = out[col].fillna(fill_value)
    # types (ví dụ)
    for col, ts in (cfg.get("types") or {}).items():
        if col in df:
            if ts.startswith("date:"):
                fmt = ts.split(":",1)[1]
                df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce").dt.date.astype("string")
            elif ts == "bool": df[col] = df[col].astype(str).str.lower().isin(["1","true","yes","x","co","có"])
            elif ts == "int": df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif ts == "str": df[col] = df[col].astype("string")

    # transforms
    for tr in (cfg.get("transforms") or []):
        op = tr.get("op")
        if op in ("strip","lower","upper"):
            for c in tr.get("cols", []):
                if c in df:
                    s = df[c].astype(str)
                    if op == "strip": df[c] = s.str.strip()
                    elif op == "lower": df[c] = s.str.lower()
                    else: df[c] = s.str.upper()
        elif op == "replace" and tr.get("col") in df:
            df[tr["col"]] = df[tr["col"]].replace(tr.get("map", {}))
    # save staging

    db = SessionLocal()
    try:
        log_ingest(db, source_key="Cleaning", log=f"Cleaned data from {raw_uri} with config {cfg_uri or 'inline'}")
    finally:
        db.close()

    buf = io.BytesIO(); df.to_csv(buf, index=False, encoding="utf-8")
    bucket = "pmnm"; ensure_bucket(client, bucket)
    key = f"staging/{source}/cleaned_{p.path.split('/')[-1]}"
    return put_bytes(client, bucket, key, buf.getvalue())
