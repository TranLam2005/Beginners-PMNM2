import warnings, mlflow, pandas as pd
from app.services.features_service import get_all_features_by_city
from sqlalchemy.orm import Session
import numpy as np

FEATURE_COLUMNS = [
    'attp_cert_issued_count',
    'attp_valid_count',
    'processing_time_p50',
    'processing_time_p90',
    'facility_count',
    'certified_facility_rate'
]

LAG_STEPS = [1, 2, 3]

MODEL_NAME="attp_facility_rate_prediction"
MODEL_URI=f"models:/{MODEL_NAME}/1"

try:
  warnings.filterwarnings("ignore")
  MODEL = mlflow.pyfunc.load_model(model_uri=MODEL_URI)
except Exception as e:
  MODEL = None
  raise RuntimeError(f"Failed to load model from {MODEL_URI}: {e}")


def prepare_input_features(city: str, db_connection: Session) -> pd.DataFrame:
  # try:
  #    df = get_all_features_by_city(db_connection, city=city)
  #   #  df = pd.DataFrame([r.__dict__ for r in records])
  #   #  df = df.drop(columns=["_sa_instance_state"], errors="ignore")
  # except Exception as e:
  #     raise RuntimeError(f"Error fetching features for city {city}: {e}")
  raw = get_all_features_by_city(db_connection, city=city)
  # ensure we have a pandas DataFrame (handles list of dicts, ORM objects, or already a DataFrame)
  df: pd.DataFrame = pd.DataFrame(raw)
  # drop SQLAlchemy instance-state if present
  df = df.drop(columns=["_sa_instance_state"], errors="ignore")
  
  df["period_month"] = pd.to_datetime(df["period_month"], errors="coerce")
  df = df.sort_values(["period_month"])

  recent_data = df.tail(len(LAG_STEPS))
    
  if len(recent_data) < len(LAG_STEPS):
      raise ValueError(f"Không đủ dữ liệu lịch sử cho '{city}'. Cần {len(LAG_STEPS)} tháng.")

  input_data = {}
  features_list_ordered = [] 

  t_minus_1_data = recent_data.iloc[-1]
  t_minus_2_data = recent_data.iloc[-2]
  t_minus_3_data = recent_data.iloc[-3]

  for col in FEATURE_COLUMNS:
      for lag in LAG_STEPS:
          feature_name = f"{col}_lag{lag}"
          features_list_ordered.append(feature_name)
            
          if lag == 1:
              input_data[feature_name] = t_minus_1_data[col]
          elif lag == 2:
              input_data[feature_name] = t_minus_2_data[col]
          elif lag == 3:
              input_data[feature_name] = t_minus_3_data[col]
                
  input_df = pd.DataFrame([input_data], columns=features_list_ordered)
    
  return input_df

def make_prediction(city: str, db_connection: Session):
  model = MODEL
  X = prepare_input_features(city, db_connection)

  Y_pred = None
  Y_proba = None

  try:
    y = model.predict(X)

    y = np.asarray(y).reshape(-1)
    y_pred = float(y[0])
  except Exception as e:
    raise RuntimeError(f"Error during prediction for city {city}: {e}")
  
  try:
     if hasattr(model._model_impl, "predict_proba"):
        proba = model._model_impl.predict_proba(X)
        proba = np.asarray(proba)
        Y_proba = float(proba[0, -1]) if proba.ndim == 2 and proba.shape[1] > 1 else float(proba[0])
  except Exception as e:
     pass
    
  return {
     "city": city,
     "y_pred": y_pred,
     "y_proba": Y_proba,
     "model_uri": MODEL_URI,
     "model_name": MODEL_NAME
  }