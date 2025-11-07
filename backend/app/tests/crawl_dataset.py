from urllib.parse import urlparse
from app.utils.minio_client import make_minio
import pandas as pd
import io

client = make_minio()
p = urlparse("s3://pmnm/staging/HCM/test_attp/v1/cleaned_e7bfdafe6e3ab4663ec784bac4f8d48b_test_dataset.csv")
obj = client.get_object(p.netloc, p.path.lstrip("/"))
raw = obj.read(); obj.close(); obj.release_conn()

df = pd.read_csv(io.BytesIO(raw))

print(df.head())