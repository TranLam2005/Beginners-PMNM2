import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_absolute_error
import mlflow
from mlflow import sklearn as mlflow_sklearn
import warnings
from app.db.session import SessionLocal
from app.services.features_service import get_all_features
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature

warnings.filterwarnings("ignore")

target_column = "certified_facility_rate"

feature_columns = [
  'attp_cert_issued_count',
  'attp_valid_count',
  'processing_time_p50',
  'processing_time_p90',
  'facility_count',
  'certified_facility_rate'
]


db = SessionLocal()
try:
  records = get_all_features(db)
  df = pd.DataFrame([r.__dict__ for r in records])
  df = df.drop(columns=["_sa_instance_state"], errors="ignore")
except Exception as e:
  raise RuntimeError(f"Error fetching features from database: {e}")
finally:
  db.close()



df['period_month'] = pd.to_datetime(df['period_month'], errors='coerce')

df[feature_columns] = df[feature_columns].fillna(0)

df = df.sort_values(['source', 'period_month'])

# Ví dụ tạo đặc trưng lùi thời gian
features = []
lag_steps = [1, 2, 3]
for col in feature_columns:
  for lag in lag_steps:
    feature_name = f"{col}_lag{lag}"
    features.append(feature_name)
    df[feature_name] = df.groupby('source')[col].shift(lag)


target = target_column

df = df.dropna(subset=features + [target])

X = df[features]
Y = df[target]
X = X.astype("float64")
Y = Y.astype("float64")

X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, shuffle=False)

mlflow.set_experiment("attp_facility_rate_prediction")
with mlflow.start_run():
  model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
  model.fit(X_train, Y_train)

  pred = model.predict(X_test)

  r2 = r2_score(Y_test, pred)
  mae = mean_absolute_error(Y_test, pred)

  mlflow.log_params({"model_type": "RandomForestRegressor", "n_estimators": 100, "lags": str(lag_steps)})
  mlflow.log_metric("r2", r2)
  mlflow.log_metric("mae", mae)

  input_example = X_train.iloc[:5]
  signature = infer_signature(X_train, model.predict(X_train))

  mlflow_sklearn.log_model(
    sk_model=model,
    artifact_path="model",
    signature=signature,
    input_example=input_example,
    registered_model_name="attp_facility_rate_prediction",
  )