import os
import sys
import json
import pandas as pd
import gzip
from google.cloud import storage
from google.oauth2 import service_account
import io


def process_stock_data(year, month, day):
    print(f"Processing data for {year}-{month}-{day}")  # 디버깅 로그 추가
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        print("Using GOOGLE_CREDENTIALS from environment")  # 디버깅 로그
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(creds_json)
        )
        storage_client = storage.Client(credentials=credentials)
    else:
        print("No GOOGLE_CREDENTIALS, using default credentials")  # 디버깅 로그
        storage_client = storage.Client()

    source_bucket_name = "goboolean-452007-raw"
    source_prefix = f"stock/usa/{year}/{month}/"
    target_bucket_name = "goboolean-452007-resampled"
    source_bucket = storage_client.bucket(source_bucket_name)
    blobs = source_bucket.list_blobs(prefix=source_prefix)

    for blob in blobs:
        if blob.name.endswith(".csv.gz"):
            print(f"Found file: {blob.name}")  # 디버깅 로그
            blob_data = blob.download_as_bytes()
            with gzip.open(io.BytesIO(blob_data), 'rt') as f:
                df = pd.read_csv(f)
            df["date"] = pd.to_datetime(df["window_start"], unit='ns')
            df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")
            df = df[df["date_str"] == f"{year}-{month}-{day}"]
            if df.empty:
                print(f"No data found for {year}-{month}-{day}")
                continue
            for ticker, group in df.groupby("ticker"):
                target_path = f"stock/usa/{ticker}/1m/{year}/{month}/{ticker}_{year}-{month}-{day}.csv.gz"
                output_buffer = io.StringIO()
                group.drop(columns=["date", "date_str"]).to_csv(output_buffer, index=False)
                csv_data = output_buffer.getvalue().encode('utf-8')
                gzip_buffer = io.BytesIO()
                with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as gz:
                    gz.write(csv_data)
                compressed_data = gzip_buffer.getvalue()
                target_bucket = storage_client.bucket(target_bucket_name)
                target_blob = target_bucket.blob(target_path)
                target_blob.upload_from_string(compressed_data, content_type="application/gzip")
                print(f"Uploaded: gs://{target_bucket_name}/{target_path}")


if __name__ == "__main__":
    print(f"Arguments received: {sys.argv}")  # 디버깅 로그 추가
    if len(sys.argv) != 4:
        print("Usage: python script.py <year> <month> <day>")
        sys.exit(1)
    year = sys.argv[1]
    month = sys.argv[2]
    day = sys.argv[3]
    process_stock_data(year, month, day)
