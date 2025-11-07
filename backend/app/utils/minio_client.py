import hashlib, os, io, datetime as dt
from minio import Minio

def make_minio():
    from os import getenv
    return Minio(
        endpoint=getenv("MINIO_ENDPOINT") or "localhost:9000",
        access_key=getenv("MINIO_ACCESS_KEY") or "admin",
        secret_key=getenv("MINIO_SECRET_KEY") or "12345678",
        secure=getenv("MINIO_SECURE","false").lower()=="true"
    )

def ensure_bucket(client, bucket: str) -> str:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    return bucket

def md5_of_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()

def put_bytes(client, bucket: str, key: str, data: bytes, content_type="application/octet-stream") -> str:
    ensure_bucket(client, bucket)
    client.put_object(bucket, key, io.BytesIO(data), length=len(data), content_type=content_type)
    return f"s3://{bucket}/{key}"

def today_path():
    d = dt.datetime.utcnow()
    return f"{d:%Y-%m-%d}"
