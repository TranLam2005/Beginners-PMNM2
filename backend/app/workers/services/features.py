# app/workers/services/features.py
import io, pandas as pd
from urllib.parse import urlparse
from app.utils.minio_client import make_minio, put_bytes, ensure_bucket, today_path
from app.utils.build_facility_id import build_facility_id
import numpy as np
from app.db.session import engine
from sqlalchemy import text
from app.services.features_service import create_bulk_features
from app.services.log_service import log_ingest
from app.db.session import SessionLocal
from app.models.models import Features # <-- 1. Import model Features

def to_month(d: pd.Series):
    return pd.to_datetime(d, errors='coerce').dt.to_period('M').astype(str)

NEED_COL = ["ten_co_so", "dia_chi", "quan_huyen", "so_gcn_dkkd", "so_gcn_attp"]


def build_features_service(staging_uri: str, *, source: str) -> str:
    m = make_minio()
    all_dfs = []
    bucket_name = "pmnm" # Giả sử bucket là 'pmnm'
    prefix = f"staging/{source}/" # Đường dẫn tới tất cả file clean của source

    # 2. Tải TẤT CẢ các file đã clean của source này
    try:
        cleaned_files = m.list_objects(bucket_name, prefix=prefix, recursive=True)
        for file_obj in cleaned_files:
            # Đảm bảo chỉ đọc các file csv (tránh các file/thư mục khác)
            if file_obj.object_name.lower().endswith('.csv'):
                try:
                    obj = m.get_object(bucket_name, file_obj.object_name)
                    df_file = pd.read_csv(io.BytesIO(obj.read()))
                    obj.close(); obj.release_conn()
                    all_dfs.append(df_file)
                except Exception as read_e:
                    print(f"Bỏ qua file {file_obj.object_name}, lỗi: {read_e}")
    except Exception as e:
        print(f"Lỗi khi đọc các file staging cho source {source}: {e}")
        # Nếu không thể list files, quay lại logic cũ (chỉ xử lý file mới)
        if not all_dfs:
            try:
                p = urlparse(staging_uri)
                obj = m.get_object(p.netloc, p.path.lstrip("/"))
                df_file = pd.read_csv(io.BytesIO(obj.read()))
                obj.close(); obj.release_conn()
                all_dfs.append(df_file)
            except Exception as single_e:
                 print(f"Không thể đọc file staging {staging_uri}: {single_e}")
                 return f"Không có dữ liệu để xử lý cho {source}"

    if not all_dfs:
        log_ingest(db, source_key="Features", log=f"Không tìm thấy file staging nào cho source {source} tại prefix {prefix}")
        return f"Không có dữ liệu file staging cho {source}"

    # 3. Gộp tất cả data lại thành 1 DataFrame
    df = pd.concat(all_dfs, ignore_index=True)

    # --- LOGIC TÍNH TOÁN CŨ (GIỮ NGUYÊN) ---
    # (Chạy logic tính toán, facility_id, v.v. trên df tổng)
    
    if "ngay_tiep_nhan" not in df.columns:
        df['ngay_cap_gcn_attp'] = pd.to_datetime(df['ngay_cap_gcn_attp'], errors='coerce')
        df['ngay_tiep_nhan'] = df['ngay_cap_gcn_attp'] - pd.Timedelta(days=60)
    
    if "thoi_han_gcn_attp" not in df.columns:
        df['thoi_han_gcn_attp'] = pd.to_datetime(df['ngay_cap_gcn_attp'], errors='coerce') + pd.Timedelta(days=1095)

    if "ngay_tiep_nhan" in df.columns:
        base_tra = None
        if "ngay_cap_gcn_attp" in df.columns:
            base_tra = df["ngay_cap_gcn_attp"]
        elif "ngay_cap_moi_nhat" in df.columns:
            base_tra = df["ngay_cap_moi_nhat"]
        elif "ngay_cap_dau_tien" in df.columns:
            base_tra = df["ngay_cap_dau_tien"]
        if base_tra is not None:
            df["ngay_tiep_nhan"] = pd.to_datetime(df["ngay_tiep_nhan"], errors='coerce')
            base_tra = pd.to_datetime(base_tra, errors='coerce')
            df["processing_days"] = (base_tra - df["ngay_tiep_nhan"]).dt.days
            
    if "han_tra" in df.columns and "ngay_tra" in df.columns:
        df["on_time"] = df["ngay_tra"] <= df["han_tra"]

    if "thoi_han_gcn_attp" in df.columns:
        today = pd.Timestamp.now().normalize()
        df["thoi_han_gcn_attp"].fillna(pd.to_datetime(df['ngay_cap_gcn_attp'], errors='coerce') + pd.Timedelta(days=1095), inplace=True)
        # Đảm bảo 'thoi_han_gcn_attp' là datetime trước khi so sánh
        df["thoi_han_gcn_attp"] = pd.to_datetime(df["thoi_han_gcn_attp"], errors='coerce')
        df["attp_valid"] = today <= df["thoi_han_gcn_attp"] 

    if "ngay_cap_gcn_attp" in df.columns:
        df["period_month"] = to_month(df["ngay_cap_gcn_attp"])
    
    tmp = df[[c for c in NEED_COL if c in df.columns]].copy()
    fid = tmp.apply(build_facility_id, axis=1)
    df["facility_id"] = fid
    
    # 4. Khử trùng lặp (DE-DUPLICATE) TRÊN TOÀN BỘ DỮ LIỆU
    # Sắp xếp theo ngày tiếp nhận để giữ lại bản ghi mới nhất
    if "ngay_tiep_nhan" in df.columns:
         df["ngay_tiep_nhan"] = pd.to_datetime(df["ngay_tiep_nhan"], errors='coerce')
         df = df.sort_values(by="ngay_tiep_nhan", ascending=False)
           
    df = df.drop_duplicates(subset=["facility_id"], keep='first') # Giữ bản ghi mới nhất

    # 5. Group by và tính toán features (như cũ)
    out = df.groupby("period_month", dropna=False).agg(
        facility_count=("facility_id","nunique"),
        attp_valid_count=("attp_valid","sum"),
        attp_cert_issued_count=("ngay_cap_gcn_attp", "nunique"),
        processing_time_p50=("processing_days", lambda s: np.nanpercentile(s.dropna(), 50) if len(s.dropna()) else np.nan),
        processing_time_p90=("processing_days", lambda s: np.nanpercentile(s.dropna(), 90) if len(s.dropna()) else np.nan ),
    ).reset_index()
    out["certified_facility_rate"] = (out["attp_valid_count"]/out["facility_count"]).fillna(0)
    out["source"] = source
    # Thay thế NaN bằng 0 (hoặc giá trị null thích hợp) thay vì string rỗng
    out.fillna(0, inplace=True) 

    buf = io.BytesIO(); out.to_csv(buf, index=False)
    bucket = "pmnm"; ensure_bucket(m, bucket)
    key = f"features/{source}_{today_path()}.csv" # File này là file tổng hợp cuối cùng

    db = SessionLocal()
    try:
        # 6. Xoá features cũ của source này
        db.query(Features).filter(Features.source == source).delete(synchronize_session=False)
        db.commit()
        
        # 7. Insert features mới đã được tổng hợp
        create_bulk_features(db, out.to_dict('records'))
        
        log_ingest(db, source_key="Features", log=f"Rebuilt features from {len(all_dfs)} files for source {source}. Total unique facilities: {len(df)}")
    except Exception as e:
        db.rollback()
        print(f"Lỗi khi ghi đè features: {e}")
        raise e
    finally:
        db.close()

    # Trả về URI của file features tổng hợp
    return put_bytes(m, bucket, key, buf.getvalue())