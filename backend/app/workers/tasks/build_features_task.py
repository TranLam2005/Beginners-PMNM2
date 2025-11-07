from app.core.celery_app import celery
from app.workers.services.features import build_features_service

@celery.task(name="build_features")
def build_features(staging_uri: str, source: str) -> str:
    return build_features_service(staging_uri, source=source)
