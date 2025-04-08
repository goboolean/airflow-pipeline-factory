import json
import os
import sys
import pandas as pd
import gzip
import tempfile
import logging
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core.client_options import ClientOptions

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def resample_data(year, month, day, ticker):
    logger.info(f"Processing data for {year}-{month}-{day}, ticker: {ticker}")
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    emulator_host = os.environ.get("STORAGE_EMULATOR_HOST")

    if creds_json:
        logger.info("Using GOOGLE_CREDENTIALS from environment")
        try:
            credentials = service_account.Credentials.from_service_account_info(json.loads(creds_json))
            if emulator_host:
                client_options = ClientOptions(api_endpoint=emulator_host)
                storage_client = storage.Client(credentials=credentials, client_options=client_options)
            else:
                storage_client = storage.Client(credentials=credentials)
        except Exception as e:
            logger.error(f"Failed to parse GOOGLE_CREDENTIALS: {e}")
            storage_client = storage.Client()
    else:
        logger.info("No GOOGLE_CREDENTIALS, using default or emulator client")
        if emulator_host:
            client_options = ClientOptions(api_endpoint=emulator_host)
            storage_client = storage.Client(client_options=client_options)
        else:
            storage_client = storage.Client()

    # split_ticker에서 생성한 원본 1m 파일 경로
    source_bucket_name = "goboolean-452007-resampled"
    source_path = f"stock/usa/{ticker}/1m/{year}/{month}/{ticker}_{year}-{month}-{day}_1m.csv.gz"

    source_bucket = storage_client.bucket(source_bucket_name)
    blob = source_bucket.blob(source_path)
    if not blob.exists():
        logger.error(f"File not found: gs://{source_bucket_name}/{source_path}")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        local_file = os.path.join(temp_dir, f"{ticker}_{year}-{month}-{day}_1m.csv.gz")
        blob.download_to_filename(local_file)

        # 데이터 로드 및 전처리: 타임스탬프를 인덱스로 사용
        with gzip.open(local_file, 'rt') as f:
            df = pd.read_csv(f)
            df["window_start"] = pd.to_datetime(df["window_start"], unit='ns')
            df.set_index("window_start", inplace=True)

        # 리샘플링할 주기 정의 (모든 주기에 대해 정규화된 결과 파일을 생성)
        periods = {
            "1m": "1min",
            "5m": "5min",
            "10m": "10min",
            "15m": "15min",
            "30m": "30min",
            "1h": "1h",
            "4h": "4h",
            "1d": "1d"
        }

        target_bucket = storage_client.bucket(source_bucket_name)

        for period_name, period_code in periods.items():
            resampled_df = df.resample(period_code).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            })
            # forward filling으로 결측치 보완 (FutureWarning 회피를 위해 .ffill() 사용)
            resampled_df = resampled_df.ffill().reset_index()

            # 모든 주기에 대해 "_norm" 접미사를 붙인다.
            normalized_suffix = f"{period_name}_norm"
            # 해당 폴더(플레이스홀더) 생성: GCS는 디렉터리 개념이 없으므로,
            # 빈 blob을 업로드하여 폴더처럼 보이게 할 수 있습니다.
            norm_folder = f"stock/usa/{ticker}/{normalized_suffix}/"
            dummy_blob = target_bucket.blob(norm_folder)
            dummy_blob.upload_from_string("")

            local_output = os.path.join(temp_dir, f"{ticker}_{year}-{month}-{day}_{normalized_suffix}.csv.gz")
            target_path = f"stock/usa/{ticker}/{normalized_suffix}/{year}/{month}/{ticker}_{year}-{month}-{day}_{normalized_suffix}.csv.gz"

            resampled_df.to_csv(local_output, compression='gzip', index=False)
            target_blob = target_bucket.blob(target_path)
            target_blob.upload_from_filename(local_output)
            logger.info(f"Resampled ({period_name}) and uploaded: gs://{source_bucket_name}/{target_path}")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        logger.error("Usage: python resample_ticker.py <year> <month> <day> <ticker>")
        sys.exit(1)
    year, month, day, ticker = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    resample_data(year, month, day, ticker)
