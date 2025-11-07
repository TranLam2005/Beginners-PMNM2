from pydantic import BaseModel, ConfigDict

class FeatureOut(BaseModel):
    id: int
    period_month: str        
    facility_count: int
    attp_valid_count: int
    attp_cert_issued_count: int
    processing_time_p50: float | None
    processing_time_p90: float | None
    certified_facility_rate: float | None
    source: str | None

    model_config = ConfigDict(from_attributes=True)