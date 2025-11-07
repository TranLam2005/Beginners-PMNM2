from ..db.session import engine, Base, SessionLocal
from sqlalchemy.dialects.postgresql import insert
from ..models.models import RawFiles
from sqlalchemy.orm import Session

def log_raw_file(db: Session, source_id: int, bucket: str, key: str, checksum: str, status: str = "new"):
  path = f"s3://{bucket}/{key}"
  try:
    db.add(RawFiles(source_id=source_id, path=path, checksum=checksum, status=status))
    db.commit()
  except Exception as e:
    print(f"Error logging raw file: {e}")
    db.rollback()
    raise e
  
def get_raw_file(db: Session):
  try:
    return db.query(RawFiles).all()
  except Exception as e:
    print(f"Error fetching raw files: {e}")
    return []