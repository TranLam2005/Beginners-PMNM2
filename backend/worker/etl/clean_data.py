import pandas as pd, re, os
import unicodedata
from app.services.log_raw_file import init_schema, log_raw_file
from ..utils.utils import read_any
from worker.utils.utils import fsum

def clean_columns(df):
  df.columns = (
    df.columns.str.strip()
    .str.lower()
    .map(
      lambda x: ''.join( c for c in unicodedata.normalize('NFD', x) if unicodedata.category(c) != 'Mn')
    )
    .str.replace(r'\s+', '_', regex=True)
    .str.replace(r'[^\w_]', '', regex=True)
  )
  return df

def clean_data(input_path, output_path):
  df = read_any(input_path)
  df = clean_columns(df)
  df['tinh_thanh'] = 'Da Nang'
  for c in df.columns:
    if "ngày" in c or "date" in c or "ngay" in c or "thoi" in c or 'ncap' in c:
      df[c] = pd.to_datetime(df[c], errors='coerce', format='%m/%d/%Y')
  
  for c in df.columns:
    if re.search(r'so_|gia|tong|soluon|value|amount', c):
      df[c] = (
        df[c].astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
      )
      df[c] = pd.to_numeric(df[c], errors='coerce')
  df = df.drop_duplicates()
  os.makedirs(os.path.dirname(output_path), exist_ok=True)
  df.to_csv(output_path, index=False, encoding='utf-8-sig')

if __name__ == "__main__":
  init_schema()
  clean_data("data/raw/danh_sach_cac_ho_kinh_doanh_dich_vu_an_uong_co_gia.xls", "data/processed/danh_sach_cac_ho_kinh_doanh_dich_vu_an_uong_co_gia_clean.csv")
  log_raw_file(5, "pmnm", "raw/Quang_Ngai_clean.csv", fsum("data/processed/Quang_Ngai_clean.csv", "sha256"))