from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from app.db import session
from app.ml.prediction import make_prediction



router = APIRouter()

@router.get("/predict")
def predict_features(city: str = Query(..., min_length=1), db_session: Session = Depends(session.get_db)):
  try:
    return make_prediction(city, db_session)
  except Exception as e:
    return {"status": "error", "detail": str(e)}