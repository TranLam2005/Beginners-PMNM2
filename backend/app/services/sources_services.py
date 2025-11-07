from sqlalchemy.orm import Session
from app.models.models import Sources

def get_all_sources(db: Session):
  try:
    results = db.query(Sources).all()
    return results
  except Exception as e:
    print(f"Error fetching sources: {e}")
    return []
  
def add_source(db: Session, name: str, url: str, kind: str, owner: str, license: str, update_frequency: str):
  try:
    db.add(Sources(name=name, url=url, kind=kind, owner=owner, license=license, update_frequency=update_frequency))
    db.commit()
  except Exception as e:
    print(f"Error adding source: {e}")
    db.rollback()
    raise e