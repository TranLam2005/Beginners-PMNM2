from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
  DATABASE_URL: str = ""
  model_config = SettingsConfigDict(env_file=".env")
  minio_endpoint: str = Field(default="localhost:9000", alias="MINIO_ENDPOINT")
  minio_access_key: str = Field(default="", alias="MINIO_ACCESS_KEY")
  minio_secret_key: str = Field(default="", alias="MINIO_SECRET_KEY")
  minio_bucket: str = Field(default="pmnm", alias="MINIO_BUCKET")
  minio_secure: bool = Field(default=False, alias="MINIO_SECURE")

  # Celery
  celery_broker_url: str = Field(default="amqp://guest:guest@localhost:5672//", alias="CELERY_BROKER_URL")
  celery_result_backend: str = Field(default="rpc://", alias="CELERY_RESULT_BACKEND")

def get_settings() -> Settings:
  return Settings()

settings = get_settings()