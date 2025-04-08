import json
import os
import sys
import pandas as pd
import gzip
from google.cloud import storage
from google.oauth2 import service_account
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import tempfile
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def upload_to_influxdb(year, month, day, ticker, influx_url, influx_token, influx_org, influx_bucket):
    logger.info(f"Uploading data for {year}-{month}-{day}, ticker: {ticker}")
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    storage_client_options = {"api_endpoint": os.environ.get("STORAGE_EMULATOR_HOST")} if os.environ.get(
        "STORAGE_EMULATOR_HOST") else {}

    if creds_json:
        logger.info("Using GOOGLE_CREDENTIALS from environment")
        try:
            credentials = service_account.Credentials.from_service_account_info(json.loads(creds_json))
            storage_client = storage.Client(credentials=credentials, client_options=storage_client_options)
        except Exception as e:
            logger.error(f"Failed to parse GOOGLE_CREDENTIALS: {e}")
            storage_client = storage.Client(client_options=storage_client_options)
    else:
        logger.info("No GOOGLE_CREDENTIALS, using default or emulator client")
        storage_client = storage.Client(client_options=storage_client_options)

    source_bucket_name = "goboolean-452007-resampled"
    # 모든 주기의 데이터를 처리하며, 정규화된 파일은 모두 _norm 접미사를 사용합니다.
    periods = ["1m", "5m", "10m", "15m", "30m", "1h", "4h", "1d"]
    try:
        client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
        write_api = client.write_api(write_options=SYNCHRONOUS)
    except Exception as e:
        logger.error(f"Failed to initialize InfluxDB client: {e}")
        raise
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            for period in periods:
                source_path = f"stock/usa/{ticker}/{period}_norm/{year}/{month}/{ticker}_{year}-{month}-{day}_{period}_norm.csv.gz"
                source_bucket = storage_client.bucket(source_bucket_name)
                blob = source_bucket.blob(source_path)
                if not blob.exists():
                    logger.warning(f"File not found: gs://{source_bucket_name}/{source_path}")
                    continue

                local_file = os.path.join(temp_dir, f"{ticker}_{year}-{month}-{day}_{period}_norm.csv.gz")
                blob.download_to_filename(local_file)

                with gzip.open(local_file, 'rt') as f:
                    df = pd.read_csv(f)
                    df["window_start"] = pd.to_datetime(df["window_start"])
                    points = [
                        Point("stock_price")
                        .tag("ticker", ticker)
                        .tag("period", period)
                        .field("open", float(row["open"]))
                        .field("high", float(row["high"]))
                        .field("low", float(row["low"]))
                        .field("close", float(row["close"]))
                        .field("volume", float(row["volume"]))
                        .time(int(row["window_start"].timestamp() * 1000000000))
                        for _, row in df.iterrows()
                    ]
                    write_api.write(bucket=influx_bucket, record=points)
                    logger.info(f"Uploaded {period} data to InfluxDB: {ticker} for {year}-{month}-{day}")
    finally:
        client.close()
        logger.info(f"Completed uploading all periods for {ticker} on {year}-{month}-{day}")


if __name__ == "__main__":
    if len(sys.argv) != 9:
        logger.error(
            "Usage: python upload_to_influxdb.py <year> <month> <day> <ticker> <influx_url> <influx_token> <influx_org> <influx_bucket>")
        sys.exit(1)
    year, month, day, ticker, influx_url, influx_token, influx_org, influx_bucket = sys.argv[1:9]
    
    # Validate date components
    try:
        year, month, day = int(year), int(month), int(day)
        # Basic date validation
        if not (1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
            logger.error(f"Invalid date: {year}-{month}-{day}")
            sys.exit(1)
    except ValueError:
        logger.error("Year, month, and day must be integers")
        sys.exit(1)
        
    # Validate URL format
    if not influx_url.startswith(("http://", "https://")):
        logger.error(f"Invalid InfluxDB URL format: {influx_url}")
        sys.exit(1)
        
    # Convert back to strings for consistent handling in the function
    year, month, day = str(year), str(month), str(day)
    
    upload_to_influxdb(year, month, day, ticker, influx_url, influx_token, influx_org, influx_bucket)
