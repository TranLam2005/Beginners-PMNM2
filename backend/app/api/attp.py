from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import JSONResponse, ORJSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session, raiseload
from sqlalchemy import text
from app.db import session
from app.models.models import Features
from app.services.features_service import get_all_features_by_city
from app.schemas.feature import FeatureOut


router = APIRouter()

  
@router.get("/indicators", response_class=ORJSONResponse)
def get_indicators (city: str = Query(..., min_length=1), db_session: Session = Depends(session.get_db)):
  print(city)
  try:
    results = get_all_features_by_city(db_session, city=city)
    return results
  except Exception as e:
    return {"status": "error", "detail": str(e)}


@router.get("/all", response_class=ORJSONResponse)
def get_all_features(db_session: Session = Depends(session.get_db)):
  try:
    rows = (
      db_session.query(Features)
        .options(raiseload("*"))  # không load bất kỳ relationship nào
        .all()
    )
    return rows
  except Exception:
    raise HTTPException(status_code=500, detail="Internal server error")