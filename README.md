#ATTP Open API

API mở cung cấp dữ liệu về **An toàn thực phẩm** — giúp truy cập, phân tích và tích hợp dữ liệu từ các cơ sở, chứng nhận và thống kê liên quan đến ATTP trên toàn quốc.

## 🌐 Base URL

**GET** `/attp/indicators`

**Mô tả:**  
Trả về danh sách các cơ sở có chứng nhận ATTP của một thành phố.

**Query params:**
| Tên | Kiểu | Mặc định | Mô tả |
|-----|------|-----------|-------|
| `city` | string | - | Tên tỉnh/thành |

**Ví dụ:**
```bash
GET /attp/indicators?city=Hồ Chí Minh
 {
  [
    {
      id: 1, period_month: 2023-1, facility_count: 1, attp_valid_count: 1, attp_cert_issued_count: 1, processing_time_p50: 3.2, processing_time_p90: 3.2, certifided_facility_rate: 0.8, sources: Hồ Chí Minh
    },
  ]
 }

**GET** `/attp/all`

**Mô tả:**  
Trả về danh sách các cơ sở có chứng nhận ATTP.

**Ví dụ:**
```bash
GET /attp/all
 {
  [
    {
      id: 1, period_month: 2023-1, facility_count: 1, attp_valid_count: 1, attp_cert_issued_count: 1, processing_time_p50: 3.2, processing_time_p90: 3.2, certifided_facility_rate: 0.8, sources: Hồ Chí Minh
    },
  ]
 }

 **GET** `/logs/ingest-logs`

**Mô tả:**  
Trả về danh logs khi làm việc với dữ liệu.

**Ví dụ:**
```bash
GET /logs/ingest-logs
 {
  [
    {
      id: 1, stag: Cleaing, log: Cleaned data from s3://pmnm/raw/Hồ Chí Minh/2025-11-06/e7bfdafe6e3ab4663ec784bac4f8d48b_BAN_CONG_BO_DAT_2023_clean.csv with config s3://pmnm/configs/Hồ Chí Minh/BAN_CONG_BO_DAT_2023.json
    },
  ]
 }

 **GET** `/ml/predict`

**Mô tả:**  
Trả về dự đoán tỉ lệ các cơ sở được cấp giấy chứng nhận attp của một thành phố.

**Ví dụ:**
```bash
GET /ml/predict
{
    {
      city: Hồ Chí Minh, y_pred: 0.3, y_proba: 0.2, model_uri: model:/..., model_name: ...
    },
}


**GET** `/sources/all`

**Mô tả:**  
Trả về danh sách các nguồn dữ liệu.

**Ví dụ:**
```bash
GET /sources/all
{
  [
    {
      id: 1, name: ABC, kind: csv, url: https://..., owner: HCM, license: Open data, update_fequency: Quý, created_at: timeStamp
    },
  ]
}

**POST** `/upload/data`

**Mô tả:**  
Trả về danh sách các nguồn dữ liệu.

**Ví dụ:**
```bash
POST /upload/data
{
  [
    {
      name: str, url: str, kind: str, license: str, update_frequency: str, data: File, config: File, source: str
    },
  ]
}