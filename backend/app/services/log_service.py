from sqlalchemy.orm import Session
from app.models.models import IngestLogs

def log_ingest(db: Session, source_key: str, log: str):
  try:
    db.add(IngestLogs(source_key=source_key, log=log))
    db.commit()
  except Exception as e:
    print(f"Error logging ingest: {e}")
    db.rollback()
    raise e

def get_ingest_logs(db: Session):
  try:
    return db.query(IngestLogs).all()
  except Exception as e:
    print(f"Error fetching ingest logs: {e}")
    return []