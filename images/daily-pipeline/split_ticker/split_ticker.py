import os
import sys
import json
import pandas as pd
import gzip
from google.cloud import storage
from google.oauth2 import service_account
import tempfile
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def upload_ticker_group(ticker, group, temp_dir, target_bucket, year, month, day, upload_counter):
    if ticker is None or (isinstance(ticker, float) and np.isnan(ticker)):
        logger.warning(f"Skipping upload for invalid ticker: {ticker}")
        return None

    target_bucket_name = target_bucket.name
    period = "1m"  # 현재 코드에서는 1분 단위 데이터만 처리
    local_output = os.path.join(temp_dir, f"{ticker}_{year}-{month}-{day}_{period}.csv.gz")
    target_path = f"stock/usa/{ticker}/{period}/{year}/{month}/{ticker}_{year}-{month}-{day}_{period}.csv.gz"

    try:
        group.drop(columns=["date", "date_str"]).to_csv(local_output, compression='gzip', index=False)
        target_blob = target_bucket.blob(target_path)
        target_blob.upload_from_filename(local_output, content_type="application/gzip")
        upload_counter[0] += 1
        if upload_counter[0] % 100 == 0:
            logger.info(f"Uploaded ({upload_counter[0]}th): gs://{target_bucket_name}/{target_path}")
        return ticker
    except Exception as e:
        logger.error(f"Failed to upload ticker {ticker}: {str(e)}")
        return None
    finally:
        if os.path.exists(local_output):
            os.remove(local_output)


def process_chunk(chunk, temp_dir, target_bucket, year, month, day, total_rows, all_tickers, upload_counter):
    chunk["date"] = pd.to_datetime(chunk["window_start"], unit='ns')
    chunk["date_str"] = chunk["date"].dt.strftime("%Y-%m-%d")
    chunk = chunk[chunk["date_str"] == f"{year}-{month}-{day}"]
    if chunk.empty:
        return total_rows, set()

    total_rows += len(chunk)
    logger.info(f"Processed chunk with {len(chunk)} rows, total rows so far: {total_rows}")

    chunk_tickers = set(chunk["ticker"].unique())
    all_tickers.update(chunk_tickers)

    uploaded_tickers = set()
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(upload_ticker_group, ticker, group, temp_dir, target_bucket, year, month, day,
                            upload_counter)
            for ticker, group in chunk.groupby("ticker")
        ]
        for future in as_completed(futures):
            ticker = future.result()
            if ticker:
                uploaded_tickers.add(ticker)

    missing_in_chunk = chunk_tickers - uploaded_tickers
    if missing_in_chunk:
        logger.warning(f"Tickers missing in chunk: {missing_in_chunk}")

    return total_rows, uploaded_tickers


def process_stock_data(year, month, day):
    logger.info(f"Processing data for {year}-{month}-{day}")
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    emulator_host = os.environ.get("STORAGE_EMULATOR_HOST")

    if creds_json:
        logger.info("Using GOOGLE_CREDENTIALS from environment")
        try:
            credentials = service_account.Credentials.from_service_account_info(json.loads(creds_json))
            if emulator_host:
                storage_client = storage.Client(credentials=credentials, client_options={"api_endpoint": emulator_host})
            else:
                storage_client = storage.Client(credentials=credentials)
        except Exception as e:
            logger.error(f"Failed to parse GOOGLE_CREDENTIALS: {e}")
            if emulator_host:
                storage_client = storage.Client(client_options={"api_endpoint": emulator_host})
            else:
                storage_client = storage.Client()
    else:
        logger.info("No GOOGLE_CREDENTIALS, using default or emulator client")
        if emulator_host:
            storage_client = storage.Client(client_options={"api_endpoint": emulator_host})
        else:
            storage_client = storage.Client()

    source_bucket_name = "goboolean-452007-raw"
    source_path = f"stock/usa/{year}/{month}/{year}-{month}-{day}.csv.gz"
    target_bucket_name = "goboolean-452007-resampled"
    source_bucket = storage_client.bucket(source_bucket_name)
    target_bucket = storage_client.bucket(target_bucket_name)

    blob = source_bucket.blob(source_path)
    if not blob.exists():
        logger.error(f"File not found: gs://{source_bucket_name}/{source_path}")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        local_gz_file = os.path.join(temp_dir, f"{year}-{month}-{day}.csv.gz")
        logger.info(f"Downloading to: {local_gz_file}")
        blob.download_to_filename(local_gz_file)

        chunk_size = 10000
        total_rows = 0
        all_tickers = set()
        uploaded_tickers = set()
        upload_counter = [0]

        with gzip.open(local_gz_file, 'rt') as f:
            reader = pd.read_csv(f, chunksize=chunk_size, keep_default_na=False)
            all_tickers = set(pd.concat([chunk['ticker'] for chunk in reader], ignore_index=True).unique())
            logger.info(f"Total unique tickers in source file: {len(all_tickers)}")

            f.seek(0)
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for chunk in pd.read_csv(f, chunksize=chunk_size, keep_default_na=False):
                    futures.append(
                        executor.submit(process_chunk, chunk, temp_dir, target_bucket, year, month, day, total_rows,
                                        all_tickers, upload_counter)
                    )
                    total_rows += len(chunk)

                for future in as_completed(futures):
                    rows, tickers = future.result()
                    total_rows = max(total_rows, rows)
                    uploaded_tickers.update(tickers)

        if uploaded_tickers:
            last_ticker = sorted(uploaded_tickers)[-1]
            period = "1m"  # 주기 추가
            last_path = f"stock/usa/{last_ticker}/{period}/{year}/{month}/{last_ticker}_{year}-{month}-{day}_{period}.csv.gz"
            logger.info(f"Last uploaded ({upload_counter[0]}th): gs://{target_bucket_name}/{last_path}")

        logger.info(f"Execution completed. Total rows processed: {total_rows}")
        logger.info(f"Total unique tickers processed: {len(uploaded_tickers)}")
        if len(uploaded_tickers) != len(all_tickers):
            missing_tickers = all_tickers - uploaded_tickers
            logger.warning(f"Ticker mismatch! Source: {len(all_tickers)}, Uploaded: {len(uploaded_tickers)}")
            logger.warning(f"Missing tickers: {missing_tickers}")


if __name__ == "__main__":
    logger.info(f"Arguments received: {sys.argv}")
    if len(sys.argv) != 4:
        logger.error("Usage: python script.py <year> <month> <day>")
        sys.exit(1)
    year = sys.argv[1]
    month = sys.argv[2]
    day = sys.argv[3]
    process_stock_data(year, month, day)
