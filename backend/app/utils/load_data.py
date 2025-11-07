import pandas as pd
import os

def load_data(path: str) -> pd.DataFrame:
  ext = os.path.splitext(path)[1].lower()
  if ext in [".csv", ".txt"]:
    return pd.read_csv(path)
  elif ext in [".xlsx", ".xls"]:
    return pd.read_excel(path)
  else:
    raise ValueError("Unsupported input format. Use CSV or XLSX.")

