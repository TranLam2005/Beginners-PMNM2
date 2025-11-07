from app.core.celery_app import celery
from app.workers.services.cleaning import clean_data_service

@celery.task(name="clean_data")
def clean_data(raw_uri: str, source: str, config: dict | None = None, config_uri: str | None = None) -> str:
    return clean_data_service(raw_uri, source=source, cfg=config, cfg_uri=config_uri)
