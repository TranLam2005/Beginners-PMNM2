
import argparse, os
import pandas as pd
import numpy as np
from backend.app.db.session import engine
from sqlalchemy import text
from typing import Sequence, Mapping, Any


def to_month(d: pd.Series):
    return pd.to_datetime(d, errors="coerce").dt.to_period("M").astype(str)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="worker")
    ap.add_argument("--as-of", default=None, help="YYYY-MM (optional filter)")
    args = ap.parse_args()

    wh = os.path.join(args.base, "warehouse")
    out_dir = os.path.join(args.base, "features")
    os.makedirs(out_dir, exist_ok=True)
    # Load warehouse tables if exist
    query_fac = "select * from warehouse.fact_facility"
    query_cert = "select * from warehouse.fact_attp_certificate"
    query_case = "select * from warehouse.fact_case_processing"

    fac = pd.read_sql(query_fac, engine)
    cert = pd.read_sql(query_cert, engine)
    case = pd.read_sql(query_case, engine)

    # Derive time dims
    if "ngay_cap_gcn_attp" in cert.columns:
        cert["period_month"] = to_month(cert["ngay_cap_gcn_attp"])
    if "ngay_tiep_nhan" in case.columns:
        case["period_month"] = to_month(case["ngay_tiep_nhan"])

    # City (tinh_thanh) inference: use fac.tinh_thanh if exists; else from phuong_xa (future enhancement)
    fac["phuong_xa"] = fac.get("phuong_xa", pd.Series([None]*len(fac)))

    # Facility base per district
    fac_district = fac.groupby("phuong_xa", dropna=False).agg(
        facility_count=("facility_id","nunique")
    ).reset_index()

    # Certificates per city-month
    if not cert.empty and "period_month" in cert.columns:
        cert_city_month = cert.merge(fac[["facility_id","phuong_xa"]], on="facility_id", how="left")
        cm = cert_city_month.groupby(["phuong_xa","period_month"], dropna=False).agg(
            attp_cert_issued_count=("ngay_cap_gcn_attp","nunique"),
            certified_facility_count=("facility_id","nunique"),
            attp_valid_count=("attp_valid","sum")
        ).reset_index()
    else:
        cm = pd.DataFrame(columns=["phuong_xa","period_month","attp_cert_issued_count","certified_facility_count","attp_valid_count"])
    # Case processing metrics
    if not case.empty and "period_month" in case.columns:
        case_city_month = case.merge(fac[["facility_id","phuong_xa"]], on="facility_id", how="left")
        pm = case_city_month.groupby(["phuong_xa","period_month"], dropna=False).agg(
            processing_time_p50=("processing_days", lambda s: np.nanpercentile(s.dropna(), 50) if len(s.dropna()) else np.nan),
            processing_time_p90=("processing_days", lambda s: np.nanpercentile(s.dropna(), 90) if len(s.dropna()) else np.nan ),
        ).reset_index()
    else:
        pm = pd.DataFrame(columns=["phuong_xa","period_month","processing_time_p50","processing_time_p90"])

    # Combine
    # Outer join over (city, month)
    features = pd.merge(cm, pm, on=["phuong_xa","period_month"], how="outer")
    # Add facility_count static (per city) for reference
    features = features.merge(fac_district, on="phuong_xa", how="left")

    # Certified facility rate approximation (needs denominator): attp_valid_count / facility_count
    if "attp_valid_count" in features.columns:
        features["certified_facility_rate"] = features["attp_valid_count"] / features["facility_count"]

    if args["as-of"] if isinstance(args, dict) else args.as_of:
        asof = args.as_of if not isinstance(args, dict) else args["as-of"]
        features = features[features["period_month"] == asof]

    cols = features.columns
    colnames = ", ".join(cols)
    placeholders = ", ".join([f":{c}" for c in cols])
    sql = f"""
        INSERT INTO warehouse.features ({colnames})
        VALUES ({placeholders})
    """

    records: Sequence[Mapping[str, Any]] = (
        features.replace({np.nan: None}).to_dict(orient="records")  # type: ignore[assignment]
    )

    with engine.begin() as conn:
        conn.execute(
            text(sql),
            records
        )

if __name__ == "__main__":
    main()
