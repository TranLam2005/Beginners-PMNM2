import pandas as pd

def build_facility_id(row: pd.Series) -> str:
    for k in ["so_gcn_dkkd","so_gcn_attp"]:
        if k in row and pd.notna(row[k]) and str(row[k]).strip()!='':
            return f"fac::{str(row[k]).strip()}"
    t = str(row.get("ten_co_so", "")) + "|" + str(row.get("dia_chi","")) + "|" + str(row.get("quan_huyen",""))
    return f"fac::soft::{abs(hash(t))}"