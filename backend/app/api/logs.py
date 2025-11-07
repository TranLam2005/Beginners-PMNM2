from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db import session
from app.services.log_service import get_ingest_logs
from app.services.log_raw_file import get_raw_file

router = APIRouter()

@router.get("/ingest-logs")
def fetch_ingest_logs(db: Session = Depends(session.get_db)):
  try:
    return get_ingest_logs(db)
  except Exception as e:
    return {"status": "error", "detail": str(e)}

# @router.get("/log-raw")
# def fetch_raw_logs(db: Session = Depends(session.get_db)):
#   try:
#     return get_raw_file(db)
#   except Exception as e:
#     return {"status": "error", "detail": str(e)}