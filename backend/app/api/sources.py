from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from app.db import session
from app.services.sources_services import get_all_sources

router = APIRouter()

@router.get("/all")
def fetch_sources(db: Session = Depends(session.get_db)):
  try:
    return get_all_sources(db)
  except Exception as e:
    return {"status": "error", "detail": str(e)}
