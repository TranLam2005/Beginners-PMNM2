CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;


CREATE TABLE core.sources (
id SERIAL PRIMARY KEY,
name TEXT NOT NULL,
kind TEXT CHECK (kind IN ('csv','xlsx','api','scraper')),
url TEXT,
owner TEXT,
license TEXT,
update_frequency TEXT,
created_at TIMESTAMPTZ DEFAULT now()
);


CREATE TABLE mart.indicators (
id BIGSERIAL PRIMARY KEY,
indicator_key TEXT UNIQUE, -- vd: online_service_rate
name TEXT,
unit TEXT,
definition TEXT,
formula TEXT,
metadata JSONB
);


CREATE TABLE mart.indicator_values (
id BIGSERIAL PRIMARY KEY,
indicator_id BIGINT REFERENCES mart.indicators(id),
province_code TEXT,
period DATE, -- dùng ngày đầu tháng
value NUMERIC,
quality_score NUMERIC, -- 0..1
lineage JSONB, -- vết nguồn & phép biến đổi
UNIQUE(indicator_id, province_code, period)
);

-- schemas đã tạo: core, staging, mart
CREATE TABLE IF NOT EXISTS core.ingest_logs (
  id BIGSERIAL PRIMARY KEY,
  source_key TEXT NOT NULL,             -- vd: "attp_csv", "wb_internet", "open_api_x"
  log text not null
);

create schema if not exists warehouse;

create table warehouse.fact_facility (
	ten_co_so text,
	ten_chu_co_so text,
	dien_thoai text,
	dia_chi text,
	phuong_xa text,
	tinh_thanh text,
	so_gcn_dkkd text,
	ngay_cap_dkkd date,
	facility_id text primary key,
	quan_huyen text,
	loai_hinh_co_so text,
	ten_dai_dien text
);

create table warehouse.fact_attp_certificate (
	id bigserial primary key,
	ngay_cap_gcn_attp date,
	ngay_cap_dkkd date,
	so_gcn_attp text,
	so_gcn_dkkd text,
	facility_id text references warehouse.fact_facility(facility_id) on delete cascade,
	attp_valid boolean,
	ngay_cap_lan_2 date,
	ngay_cap_dau_tien date,
	so_gcn_cap_lan_2 text,
	thoi_han_gcn_attp date,
	unique(facility_id, so_gcn_attp)
);

CREATE INDEX idx_attp_facility ON warehouse.fact_attp_certificate(facility_id);

create table warehouse.fact_case_processing (
	ngay_tiep_nhan date,
	processing_days float,
	facility_id text primary key references warehouse.fact_facility(facility_id) on delete cascade,
	linh_vuc text,
	han_tra date,
	ket_qua text,
	chuyen_vien_thu_ly text,
	so_bien_nhan text,
	unique(facility_id)
);

CREATE INDEX idx_case_facility ON warehouse.fact_case_processing(facility_id);

create table warehouse.features (
	id bigserial primary key,
	period_month text,
	facility_count float,
	attp_valid_count float,
	attp_cert_issued_count float,
	processing_time_p50 float,
	processing_time_p90 float,
	certified_facility_rate float,
	source text
);