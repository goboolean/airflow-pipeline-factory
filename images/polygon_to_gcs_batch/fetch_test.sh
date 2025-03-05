#!/bin/bash
# 필수 환경 변수 확인
if [ -z "$YEAR" ]; then
    echo "Error: YEAR environment variable must be set"
    exit 1
fi
if [ -z "$S3_BUCKET" ] || [ -z "$S3_PATH" ]; then
    echo "Error: S3_BUCKET and S3_PATH environment variables must be set"
    exit 1
fi
if [ -z "$GCS_BUCKET" ] || [ -z "$GCS_PATH" ]; then
    echo "Error: GCS_BUCKET and GCS_PATH environment variables must be set"
    exit 1
fi

S3_PREFIX="s3://${S3_BUCKET}/${S3_PATH}/${YEAR}/"
GCS_PREFIX="gs://${GCS_BUCKET}/${GCS_PATH}/${YEAR}/"
TEMP_DIR="/tmp/s3_downloads/${YEAR}"

# 성공/실패한 달을 추적하기 위한 배열
declare -a SUCCESS_MONTHS
declare -a FAILED_MONTHS

echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
echo "GOOGLE_CREDENTIALS: ${GOOGLE_CREDENTIALS:0:50}..."
echo "Configuring AWS CLI with environment variables"
aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"

if [ -n "$GOOGLE_CREDENTIALS" ]; then
    echo "Using GOOGLE_CREDENTIALS from environment variable"
    echo "$GOOGLE_CREDENTIALS" > /tmp/gcp_credentials.json
    export GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp_credentials.json
else
    echo "Error: GOOGLE_CREDENTIALS environment variable not set"
    exit 1
fi

echo "Checking if GCS credential file exists:"
ls -l "$GOOGLE_APPLICATION_CREDENTIALS" || echo "GCS credential file not generated"
echo "Activating GCS service account:"
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS" || echo "Failed to activate GCS service account"
echo "Checking GCS auth:"
gsutil ls "gs://${GCS_BUCKET}/" || echo "GCS auth failed but continuing"

mkdir -p "$TEMP_DIR"

echo "Processing ${S3_PREFIX}..."
for MONTH in {01..12}; do
    S3_MONTH_PREFIX="${S3_PREFIX}${MONTH}/"
    GCS_MONTH_PREFIX="${GCS_PREFIX}${MONTH}/"
    LOCAL_MONTH_DIR="${TEMP_DIR}/${MONTH}"

    mkdir -p "$LOCAL_MONTH_DIR"

    echo "Downloading S3 prefix ${S3_MONTH_PREFIX} to ${LOCAL_MONTH_DIR}..."
    aws s3 cp "${S3_MONTH_PREFIX}" "$LOCAL_MONTH_DIR" --recursive --endpoint-url https://files.polygon.io/ --no-verify-ssl > "${LOCAL_MONTH_DIR}/s3_download.log" 2>&1
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

    echo "Uploading .gz files to ${GCS_MONTH_PREFIX} using gsutil -m cp..."
    gsutil -m cp "${LOCAL_MONTH_DIR}/*.gz" "${GCS_MONTH_PREFIX}" > "${LOCAL_MONTH_DIR}/gsutil_upload.log" 2>&1
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
