from sqlalchemy.orm import Session
from app.models.models import Features
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func, desc


def create_bulk_features(db: Session, features_data):
  if not features_data:
      return
  try:
     db.bulk_insert_mappings(Features, features_data)
     db.commit()
  except Exception as e:
      print(f"Error inserting features: {e}")
      db.rollback()
      raise e
  
def get_all_features_by_city(db: Session, city: str, threshold = 0.3):
    sim = func.similarity(Features.source, city)
    try:
        results = (
            db.query(Features, sim.label("score"))
            .filter(sim >= threshold)
            .order_by(desc("score"))
            .all()
        )
        # results is a list of tuples (Features, score); return JSON-serializable dicts
        return [jsonable_encoder(feature) for feature, score in results]
    except Exception as e:
        print(f"Error fetching features for city {city}: {e}")
        return []
    
def get_all_features(db: Session):
    try:
        results = db.query(Features).all()
        return results
    except Exception as e:
        print(f"Error fetching all features: {e}")
        return []