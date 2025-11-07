# app/api/upload.py
from fastapi import APIRouter, UploadFile, File, Form, Depends
import json
from app.workers.tasks.pipeline_task import run_pipeline_chain
from app.utils.minio_client import put_bytes, make_minio, ensure_bucket, md5_of_bytes, today_path
from sqlalchemy.orm import Session
from app.db import session
from app.services.sources_services import add_source

router = APIRouter()

@router.post("/data")
async def upload_file(
    name: str = Form(""),
    url: str = Form(""),
    kind: str = Form(""),
    license: str = Form(""),
    update_frequency: str = Form(...),
    data: UploadFile = File(...),
    config: UploadFile | None = File(None),
    source: str = Form("manual"),
    config_id: str | None = Form(None),
    db: Session = Depends(session.get_db)
):

    # save source metadata
    add_source(db, name=name, url=url, kind=kind, owner=source, license=license, update_frequency=update_frequency)

    raw_bytes = await data.read()
    cfg_bytes = None

    # cấu hình: ưu tiên file cấu hình kèm theo; nếu không có thì dùng config_id
    if config:
        cfg_bytes = await config.read()
        cfg = json.loads(cfg_bytes.decode("utf-8"))
        cfg_uri = None
    else:
        cfg = None
        cfg_uri = f"s3://pmnm/configs/{config_id}.json" if config_id else None

    client = make_minio()
    checksum_raw = md5_of_bytes(raw_bytes)
    key = f"raw/{source}/{today_path()}/{checksum_raw}_{data.filename}"
    
    # push file raw lên MinIO (như bạn đã làm), trả về raw_uri
    raw_uri = put_bytes(client, bucket=ensure_bucket(client, "pmnm"), key=key, data=raw_bytes, content_type=data.content_type or "application/octet-stream")

    # nếu có file cấu hình kèm theo thì cũng lưu lại để trace
    if cfg and not cfg_uri and cfg_bytes:
        cfg_uri = put_bytes(client, bucket="pmnm", key=f"configs/{source}/{config.filename}", data=cfg_bytes, content_type="application/json")

    # chạy pipeline, truyền cả raw_uri và config (hoặc config_uri)
    task = run_pipeline_chain(raw_uri, source, cfg, cfg_uri)
    return {"message":"queued", "raw_uri": raw_uri, "config_uri": cfg_uri, "task_id": task.id}
