from typing import Optional
import datetime
import decimal

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, Date, DateTime, Double, ForeignKeyConstraint, Index, Integer, Numeric, PrimaryKeyConstraint, Table, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass


class IngestLogs(Base):
    __tablename__ = 'ingest_logs'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='ingest_logs_pkey'),
        {'schema': 'core'}
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_key: Mapped[str] = mapped_column(Text, nullable=False)
    log: Mapped[str] = mapped_column(Text, nullable=False)


class Sources(Base):
    __tablename__ = 'sources'
    __table_args__ = (
        CheckConstraint("kind = ANY (ARRAY['csv'::text, 'xlsx'::text, 'api'::text, 'scraper'::text])", name='sources_kind_check'),
        PrimaryKeyConstraint('id', name='sources_pkey'),
        {'schema': 'core'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    kind: Mapped[Optional[str]] = mapped_column(Text)
    url: Mapped[Optional[str]] = mapped_column(Text)
    owner: Mapped[Optional[str]] = mapped_column(Text)
    license: Mapped[Optional[str]] = mapped_column(Text)
    update_frequency: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('now()'))

    datasets: Mapped[list['Datasets']] = relationship('Datasets', back_populates='source')
    raw_files: Mapped[list['RawFiles']] = relationship('RawFiles', back_populates='source')


class Indicators(Base):
    __tablename__ = 'indicators'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='indicators_pkey'),
        UniqueConstraint('indicator_key', name='indicators_indicator_key_key'),
        {'schema': 'mart'}
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    indicator_key: Mapped[Optional[str]] = mapped_column(Text)
    name: Mapped[Optional[str]] = mapped_column(Text)
    unit: Mapped[Optional[str]] = mapped_column(Text)
    definition: Mapped[Optional[str]] = mapped_column(Text)
    formula: Mapped[Optional[str]] = mapped_column(Text)
    metadata_: Mapped[Optional[dict]] = mapped_column('metadata', JSONB)

    indicator_values: Mapped[list['IndicatorValues']] = relationship('IndicatorValues', back_populates='indicator')


t_city_month_features = Table(
    'city_month_features', Base.metadata,
    Column('phuong_xa', Text),
    Column('period_month', Text),
    Column('attp_cert_issued_count', Double(53)),
    Column('certified_facility_count', Double(53)),
    Column('attp_valid_count', Double(53)),
    Column('processing_time_p50', Double(53)),
    Column('processing_time_p90', Double(53)),
    Column('facility_count', BigInteger),
    Column('certified_facility_rate', Double(53)),
    schema='warehouse'
)


class FactFacility(Base):
    __tablename__ = 'fact_facility'
    __table_args__ = (
        PrimaryKeyConstraint('facility_id', name='fact_facility_pkey'),
        {'schema': 'warehouse'}
    )

    facility_id: Mapped[str] = mapped_column(Text, primary_key=True)
    ten_co_so: Mapped[Optional[str]] = mapped_column(Text)
    ten_chu_co_so: Mapped[Optional[str]] = mapped_column(Text)
    dien_thoai: Mapped[Optional[str]] = mapped_column(Text)
    dia_chi: Mapped[Optional[str]] = mapped_column(Text)
    phuong_xa: Mapped[Optional[str]] = mapped_column(Text)
    tinh_thanh: Mapped[Optional[str]] = mapped_column(Text)
    so_gcn_dkkd: Mapped[Optional[str]] = mapped_column(Text)
    ngay_cap_dkkd: Mapped[Optional[datetime.date]] = mapped_column(Date)
    quan_huyen: Mapped[Optional[str]] = mapped_column(Text)
    loai_hinh_co_so: Mapped[Optional[str]] = mapped_column(Text)
    ten_dai_dien: Mapped[Optional[str]] = mapped_column(Text)

    fact_attp_certificate: Mapped[list['FactAttpCertificate']] = relationship('FactAttpCertificate', back_populates='facility')


class Features(Base):
    __tablename__ = 'features'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='features_pkey'),
        {'schema': 'warehouse'}
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    period_month: Mapped[Optional[str]] = mapped_column(Text)
    facility_count: Mapped[Optional[int]] = mapped_column(Integer)
    attp_valid_count: Mapped[Optional[int]] = mapped_column(Integer)
    attp_cert_issued_count: Mapped[Optional[int]] = mapped_column(Integer)
    processing_time_p50: Mapped[Optional[float]] = mapped_column(Double(53))
    processing_time_p90: Mapped[Optional[float]] = mapped_column(Double(53))
    certified_facility_rate: Mapped[Optional[float]] = mapped_column(Double(53))
    source: Mapped[Optional[str]] = mapped_column(Text)


class Datasets(Base):
    __tablename__ = 'datasets'
    __table_args__ = (
        ForeignKeyConstraint(['source_id'], ['core.sources.id'], name='datasets_source_id_fkey'),
        PrimaryKeyConstraint('id', name='datasets_pkey'),
        {'schema': 'core'}
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_id: Mapped[Optional[int]] = mapped_column(Integer)
    name: Mapped[Optional[str]] = mapped_column(Text)
    version: Mapped[Optional[str]] = mapped_column(Text)
    schema_json: Mapped[Optional[dict]] = mapped_column(JSONB)
    valid_from: Mapped[Optional[datetime.date]] = mapped_column(Date)
    valid_to: Mapped[Optional[datetime.date]] = mapped_column(Date)
    created_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('now()'))

    source: Mapped[Optional['Sources']] = relationship('Sources', back_populates='datasets')


class IndicatorValues(Base):
    __tablename__ = 'indicator_values'
    __table_args__ = (
        ForeignKeyConstraint(['indicator_id'], ['mart.indicators.id'], name='indicator_values_indicator_id_fkey'),
        PrimaryKeyConstraint('id', name='indicator_values_pkey'),
        UniqueConstraint('indicator_id', 'province_code', 'period', name='indicator_values_indicator_id_province_code_period_key'),
        {'schema': 'mart'}
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    indicator_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    province_code: Mapped[Optional[str]] = mapped_column(Text)
    period: Mapped[Optional[datetime.date]] = mapped_column(Date)
    value: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    quality_score: Mapped[Optional[decimal.Decimal]] = mapped_column(Numeric)
    lineage: Mapped[Optional[dict]] = mapped_column(JSONB)

    indicator: Mapped[Optional['Indicators']] = relationship('Indicators', back_populates='indicator_values')


class RawFiles(Base):
    __tablename__ = 'raw_files'
    __table_args__ = (
        CheckConstraint("status = ANY (ARRAY['new'::text, 'parsed'::text, 'failed'::text])", name='raw_files_status_check'),
        ForeignKeyConstraint(['source_id'], ['core.sources.id'], name='raw_files_source_id_fkey'),
        PrimaryKeyConstraint('id', name='raw_files_pkey'),
        UniqueConstraint('checksum', name='checksum_unique'),
        {'schema': 'staging'}
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    source_id: Mapped[Optional[int]] = mapped_column(Integer)
    checksum: Mapped[Optional[str]] = mapped_column(Text)
    ingested_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(True), server_default=text('now()'))
    status: Mapped[Optional[str]] = mapped_column(Text)

    source: Mapped[Optional['Sources']] = relationship('Sources', back_populates='raw_files')


class FactAttpCertificate(Base):
    __tablename__ = 'fact_attp_certificate'
    __table_args__ = (
        ForeignKeyConstraint(['facility_id'], ['warehouse.fact_facility.facility_id'], ondelete='CASCADE', name='fact_attp_certificate_facility_id_fkey'),
        PrimaryKeyConstraint('id', name='fact_attp_certificate_pkey'),
        UniqueConstraint('facility_id', 'so_gcn_attp', name='fact_attp_certificate_facility_id_so_gcn_attp_key'),
        Index('idx_attp_facility', 'facility_id'),
        {'schema': 'warehouse'}
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ngay_cap_gcn_attp: Mapped[Optional[datetime.date]] = mapped_column(Date)
    ngay_cap_dkkd: Mapped[Optional[datetime.date]] = mapped_column(Date)
    so_gcn_attp: Mapped[Optional[str]] = mapped_column(Text)
    so_gcn_dkkd: Mapped[Optional[str]] = mapped_column(Text)
    facility_id: Mapped[Optional[str]] = mapped_column(Text)
    attp_valid: Mapped[Optional[bool]] = mapped_column(Boolean)
    ngay_cap_lan_2: Mapped[Optional[datetime.date]] = mapped_column(Date)
    ngay_cap_dau_tien: Mapped[Optional[datetime.date]] = mapped_column(Date)
    so_gcn_cap_lan_2: Mapped[Optional[str]] = mapped_column(Text)
    thoi_han_gcn_attp: Mapped[Optional[datetime.date]] = mapped_column(Date)

    facility: Mapped[Optional['FactFacility']] = relationship('FactFacility', back_populates='fact_attp_certificate')


class FactCaseProcessing(FactFacility):
    __tablename__ = 'fact_case_processing'
    __table_args__ = (
        ForeignKeyConstraint(['facility_id'], ['warehouse.fact_facility.facility_id'], ondelete='CASCADE', name='fact_case_processing_facility_id_fkey'),
        PrimaryKeyConstraint('facility_id', name='fact_case_processing_pkey'),
        Index('idx_case_facility', 'facility_id'),
        {'schema': 'warehouse'}
    )

    facility_id: Mapped[str] = mapped_column(Text, primary_key=True)
    ngay_tiep_nhan: Mapped[Optional[datetime.date]] = mapped_column(Date)
    processing_days: Mapped[Optional[float]] = mapped_column(Double(53))
    linh_vuc: Mapped[Optional[str]] = mapped_column(Text)
    han_tra: Mapped[Optional[datetime.date]] = mapped_column(Date)
    ket_qua: Mapped[Optional[str]] = mapped_column(Text)
    chuyen_vien_thu_ly: Mapped[Optional[str]] = mapped_column(Text)
    so_bien_nhan: Mapped[Optional[str]] = mapped_column(Text)
