#!/bin/bash

# YEAR는 인자 또는 환경 변수로 받기
YEAR="${1:-$YEAR}"
if [ -z "$YEAR" ]; then
    echo "Error: YEAR must be provided as an argument or environment variable"
    exit 1
fi
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "Error: AWS_ACCESS_KEY_ID environment variable must be set"
    exit 1
fi
if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "Error: AWS_SECRET_ACCESS_KEY environment variable must be set"
    exit 1
fi

# GCS 인증 설정
if [ -n "$GOOGLE_CREDENTIALS" ]; then
    echo "Using GCS_CREDENTIALS from environment variable"
    echo "$GOOGLE_CREDENTIALS" > /tmp/gcp_credentials.json
    gcloud auth activate-service-account --key-file=/tmp/gcp_credentials.json || { echo "GCS auth failed"; exit 1; }
    export GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp_credentials.json
else
    echo "Error: $GOOGLE_CREDENTIALS environment variable must be set"
    exit 1
fi

# 경로 하드코딩
TEMP_DIR="/tmp/s3_downloads/${YEAR}"

# 성공/실패한 달을 추적하기 위한 배열
declare -a SUCCESS_MONTHS
declare -a FAILED_MONTHS

echo "Configuring AWS CLI with environment variables"
aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"

echo "Checking GCS auth:"
gsutil ls "gs://goboolean-452007-raw/" || echo "GCS auth failed but continuing"

mkdir -p "$TEMP_DIR"

echo "Processing s3://flatfiles/us_stocks_sip/minute_aggs_v1/${YEAR}"
for MONTH in {01..12}; do
    S3_PREFIX="s3://flatfiles/us_stocks_sip/minute_aggs_v1/${YEAR}/${MONTH}/"
    GCS_PREFIX="gs://goboolean-452007-raw/stock/usa/${YEAR}/${MONTH}/"
    LOCAL_MONTH_DIR="${TEMP_DIR}/${MONTH}"

    mkdir -p "$LOCAL_MONTH_DIR"

    echo "Downloading S3 prefix ${S3_PREFIX} to ${LOCAL_MONTH_DIR}..."
    aws s3 cp "${S3_PREFIX}" "$LOCAL_MONTH_DIR" --recursive --endpoint-url https://files.polygon.io/ --no-verify-ssl > "${LOCAL_MONTH_DIR}/s3_download.log" 2>&1
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo "Error: aws s3 cp --recursive failed for month ${MONTH}. Skipping..."
        cat "${LOCAL_MONTH_DIR}/s3_download.log"
        FAILED_MONTHS+=("$MONTH")
        continue
    fi

    if [ -z "$(ls -A "$LOCAL_MONTH_DIR"/*.gz 2>/dev/null)" ]; then
        echo "Warning: No .gz files found in ${LOCAL_MONTH_DIR}. Skipping month ${MONTH}..."
        FAILED_MONTHS+=("$MONTH")
        continue
    fi

    echo "Uploading .gz files to ${GCS_PREFIX} using gsutil -m cp..."
    gsutil -m cp "${LOCAL_MONTH_DIR}/*.gz" "${GCS_PREFIX}" > "${LOCAL_MONTH_DIR}/gsutil_upload.log" 2>&1
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo "Error: gsutil -m cp failed for month ${MONTH}. Skipping..."
        cat "${LOCAL_MONTH_DIR}/gsutil_upload.log"
        FAILED_MONTHS+=("$MONTH")
        continue
    fi

    rm -rf "${LOCAL_MONTH_DIR}"/*.gz
    SUCCESS_MONTHS+=("$MONTH")
    echo "Successfully processed month ${MONTH}"
done

echo -e "\n=== Processing Results ==="
echo "Successful months (${#SUCCESS_MONTHS[@]}): ${SUCCESS_MONTHS[*]:-None}"
echo "Failed months (${#FAILED_MONTHS[@]}): ${FAILED_MONTHS[*]:-None}"

echo "Cleaning up temporary directory..."
rm -rf "$TEMP_DIR"
echo "Script completed"
