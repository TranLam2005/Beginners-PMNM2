import argparse, os, json
from backend.app.db.session import engine
import pandas as pd
import numpy as np
from ..utils.utils import ensure_dir, norm_df_cols, to_date_col, load_config, archive_raw, write_log, build_facility_id, upsert_csv, read_any
from app.services.s3 import s3
from sqlalchemy import text




def rename_and_cast(df: pd.DataFrame, colmap: dict) -> pd.DataFrame:
    # Only rename keys that exist
    rename_map = {k: v for k, v in colmap.items() if k in df.columns}
    return df.rename(columns=rename_map)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--input", required=True)
    ap.add_argument("--source-id", required=True, help="file1|file2|... (used for staging partition)")
    ap.add_argument("--base", default="worker")
    args = ap.parse_args()

    cfg = load_config(args.config)
    dayfirst = cfg.get("date_dayfirst", True)
    colmap = cfg.get("column_map", {})
    date_cols = cfg.get("date_cols", [])
    text_norm = cfg.get("text_normalize", [])
    enums = cfg.get("enums", {})
    add_cols = cfg.get("add_columns", {})
    source_name = cfg.get("source_name", args.source_id)

    # IO dirs
    raw_dir = os.path.join(args.base, "raw")
    staging_dir = os.path.join(args.base, "staging", source_name)
    warehouse_dir = os.path.join(args.base, "warehouse")
    logs_dir = os.path.join(args.base, "logs")

    ensure_dir(raw_dir); ensure_dir(staging_dir); ensure_dir(warehouse_dir); ensure_dir(logs_dir)

    archived = archive_raw(args.input, raw_dir)

    df = read_any(args.input)
    df = rename_and_cast(df, colmap)

    # Add constant/derived columns from config
    for k,v in add_cols.items():
        df[k] = v

    # Parse dates
    for c in date_cols:
        to_date_col(df, c, dayfirst=dayfirst)

    # Normalize text
    df = norm_df_cols(df, text_norm)

    # Normalize enums (simple map)
    for col, spec in enums.items():
        if col in df.columns:
            m = spec.get("map", {})
            df[col] = df[col].map(lambda x: m.get(str(x).strip().lower(), x) if pd.notna(x) else x)

    if 'ngay_tiep_nhan' not in df.columns:
        df['ngay_cap_gcn_attp'] = pd.to_datetime(df['ngay_cap_gcn_attp'], errors='coerce')
        df['ngay_tiep_nhan'] = df['ngay_cap_gcn_attp'] - pd.Timedelta(days=60)

    # Ensure thoi_han_gcn_attp exists
    if 'thoi_han_gcn_attp' not in df.columns:
        df['thoi_han_gcn_attp'] = pd.to_datetime(df['ngay_cap_gcn_attp'], errors='coerce') + pd.Timedelta(days=1095)
    
    # Derive processing_days/on_time if applicable
    if "ngay_tiep_nhan" in df.columns:
        base_tra = None
        # If we have explicit ngay_cap_gcn_attp use it, else use ngay_cap_* as proxy
        if "ngay_cap_gcn_attp" in df.columns:
            base_tra = df["ngay_cap_gcn_attp"]
        elif "ngay_cap_moi_nhat" in df.columns:
            base_tra = df["ngay_cap_moi_nhat"]
        elif "ngay_cap_dau_tien" in df.columns:
            base_tra = df["ngay_cap_dau_tien"]
        if base_tra is not None:
            base_tra = pd.to_datetime(base_tra, errors='coerce')
            df["processing_days"] = (base_tra - df["ngay_tiep_nhan"]).dt.days
    if "han_tra" in df.columns and "ngay_tra" in df.columns:
        df["on_time"] = df["ngay_tra"] <= df["han_tra"]

    # Derive attp_valid if have expiry
    if "thoi_han_gcn_attp" in df.columns:
        today = pd.Timestamp.today().normalize()
        df["attp_valid"] = today <= df["thoi_han_gcn_attp"]

    # Write staging
    part = pd.Timestamp.today().strftime("%Y-%m-%d")
    out_stage = os.path.join(staging_dir, f"dt={part}.csv")
    df.to_csv(out_stage, index=False, date_format="%Y-%m-%d")
    s3.upload_file(out_stage, "pmnm", f"staging/{source_name}_{part}.csv")

    # Split into warehouse entities when columns available
    # Facility
    fac_cols = ["ten_co_so","ten_chu_co_so","ten_dai_dien","loai_hinh_co_so","dien_thoai",
                "dia_chi","phuong_xa","quan_huyen","tinh_thanh","so_gcn_dkkd","ngay_cap_dkkd"]
    fac = df[[c for c in fac_cols if c in df.columns]].copy()
    if not fac.empty:
        need_cols = ["ten_co_so", "dia_chi", "quan_huyen", "so_gcn_dkkd", "so_gcn_attp"]
        tmp = df[[c for c in need_cols if c in df.columns]].copy()
        fid = tmp.apply(build_facility_id, axis=1)
        fac["facility_id"] = fid
        fac = fac.drop_duplicates(subset=["facility_id"])
        upsert_csv("fact_facility", fac, "warehouse")

    # Certificates
    cert_cols = cfg.get("date_issue_cols", [])
    if any(c in df.columns for c in cert_cols):
        cert = df[[c for c in cert_cols if c in df.columns]].copy()
        if "facility_id" not in cert.columns:
            # Attach via derived facility_id using same logic (needs context columns)
            need_cols = ["ten_co_so","dia_chi","quan_huyen","so_gcn_dkkd","so_gcn_attp"]
            tmp = df[[c for c in need_cols if c in df.columns]].copy()
            fid = tmp.apply(build_facility_id, axis=1)
            cert["facility_id"] = fid
        cert['attp_valid'] = df['attp_valid']
        cert = cert.dropna(subset=["facility_id"])
        subset_cols = [col for col in ["facility_id", "so_gcn_attp", "ngay_cap_gcn_attp"] if col in cert.columns]
        cert = cert.drop_duplicates(subset=subset_cols)
        existing_facilities = pd.read_sql(
            text("SELECT facility_id FROM warehouse.fact_facility"),
            engine
        )["facility_id"].astype(str).tolist()

        cert_valid = cert[cert["facility_id"].isin(existing_facilities)].copy()
        upsert_csv("fact_attp_certificate", cert_valid, "warehouse", ["facility_id", "so_gcn_attp"])

    # Case processing (administrative service)
    case_cols = ["so_bien_nhan","ngay_tiep_nhan","han_tra","ngay_tra","ket_qua","processing_days","on_time","linh_vuc","chuyen_vien_thu_ly"]
    if any(c in df.columns for c in case_cols):
        case = df[[c for c in case_cols if c in df.columns]].copy()
        need_cols = ["ten_co_so","dia_chi","quan_huyen","so_gcn_dkkd","so_gcn_attp"]
        tmp = df[[c for c in need_cols if c in df.columns]].copy()
        case["facility_id"] = tmp.apply(build_facility_id, axis=1)
        case = case.dropna(subset=["facility_id"])
        case = case.drop_duplicates(subset=["facility_id","so_bien_nhan"] if "so_bien_nhan" in case.columns else ["facility_id","ngay_tiep_nhan"] if "ngay_tiep_nhan" in case.columns else ["facility_id"])

        existing_facilities = pd.read_sql(
            text("select facility_id from warehouse.fact_facility"),
            engine
        )["facility_id"].astype(str).tolist()
        case_valid = case[case["facility_id"].isin(existing_facilities)].copy()
        upsert_csv("fact_case_processing", case_valid, "warehouse")

    write_log(os.path.join(args.base,"logs"), "ingest", f"OK ingest {args.input} with config {args.config}")

if __name__ == "__main__":
    main()
