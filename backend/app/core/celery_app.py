# app/tasks/celery_app.py
import os
from celery import Celery
celery = Celery("pmnm", broker=os.getenv("CELERY_BROKER_URL"), backend=os.getenv("CELERY_RESULT_BACKEND", "rpc://"))

celery.conf.update(
    broker_connection_retry_on_startup=True,  # tránh warning, retry ngay khi khởi động
)

celery.conf.imports = [
  "app.workers.tasks.clean_data_task",
  "app.workers.tasks.build_features_task",
]