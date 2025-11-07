from sqlalchemy import text
from sqlalchemy.orm import Session

def log_raw_file(session: Session, *, file_id, source, filename, checksum, s3_uri, status="uploaded", meta=None):
    session.execute(text("""
        INSERT INTO staging.raw_files (file_id, source, filename, checksum, s3_uri, status, meta)
        VALUES (:file_id, :source, :filename, :checksum, :s3_uri, :status, :meta)
        ON CONFLICT (file_id) DO UPDATE SET status = EXCLUDED.status, s3_uri=EXCLUDED.s3_uri;
    """), dict(file_id=file_id, source=source, filename=filename, checksum=checksum, s3_uri=s3_uri, status=status, meta=meta))
    session.commit()
