#!/bin/bash

# 인자로 YEAR, MONTH, DAY 받기
YEAR=$1
MONTH=$2
DAY=$3

setup_gcs_auth() {
    if [ "$ENVIRONMENT" = "production" ]; then
        echo "Production mode: Skipping GCS auth (handled by Kubernetes)"
    else
        echo "Local mode: Setting up GCS auth"
        if [ -n "$GOOGLE_CREDENTIALS" ]; then
            echo "$GOOGLE_CREDENTIALS" > "$TMP_DIR/gcp_credentials.json"
            gcloud auth activate-service-account --key-file="$TMP_DIR/gcp_credentials.json" || { echo "GCS auth failed"; exit 1; }
            export GOOGLE_APPLICATION_CREDENTIALS="$TMP_DIR/gcp_credentials.json"
        else
            echo "Error: GOOGLE_CREDENTIALS must be set in local mode"
            exit 1
        fi
    fi

    echo "Checking if GCS credential file exists:"
    ls -l "$GOOGLE_APPLICATION_CREDENTIALS" 2>&1 || echo "Credential file not generated"
    echo "Activating GCS service account:"
    gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS" 2>&1 || echo "Failed to activate GCS service account"
}

setup_aws_auth() {
    if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
        echo "Error: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set"
        exit 1
    fi
    aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
    aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"
}

validate_env_variables() {
    if [ -z "$YEAR" ] || [ -z "$MONTH" ] || [ -z "$DAY" ]; then
        echo "Error: YEAR, MONTH, and DAY must be provided as arguments"
        exit 1
    fi
    # 날짜 형식 검증
    if ! [[ "$YEAR" =~ ^[0-9]{4}$ ]] || ! [[ "$MONTH" =~ ^[0-1][0-9]$ ]] || ! [[ "$DAY" =~ ^[0-3][0-9]$ ]]; then
        echo "Error: YEAR (YYYY), MONTH (MM), and DAY (DD) must be valid numbers"
        exit 1
    fi
}

# 환경변수 확인 (인자로 받은 값 검증)
validate_env_variables
# AWS 인증 설정
setup_aws_auth
# GCS 인증 설정
setup_gcs_auth

# 경로 설정
S3_PREFIX="s3://flatfiles/us_stocks_sip/minute_aggs_v1/${YEAR}/${MONTH}/"
GCS_PREFIX="gs://goboolean-452007-raw/stock/usa/${YEAR}/${MONTH}/"

echo "Checking GCS auth:"
gsutil ls gs://goboolean-452007-raw/ 2>&1 || echo "GCS auth failed but continuing"
echo "Processing ${S3_PREFIX}"

# S3에서 파일 목록 확인
echo "Running aws s3 ls ${S3_PREFIX}"
aws s3 ls "${S3_PREFIX}" --endpoint-url https://files.polygon.io/ --no-verify-ssl > /tmp/s3_list.log 2>&1
EXIT_CODE=$?
echo "aws s3 ls exit code: $EXIT_CODE"
if [ $EXIT_CODE -ne 0 ]; then
    echo "Error: aws s3 ls failed. Check credentials or endpoint."
    cat /tmp/s3_list.log
    exit 1
fi

# 파일 목록 확인
echo "S3 list log content:"
cat /tmp/s3_list.log || echo "Failed to read /tmp/s3_list.log"

# 특정 날짜의 파일 필터링
DAY_FILE=$(grep -v "urllib3/connectionpool.py" /tmp/s3_list.log | awk '{print $4}' | grep "${DAY}" || true)
if [ -z "$DAY_FILE" ]; then
    echo "No file found in ${S3_PREFIX} for ${YEAR}-${MONTH}-${DAY}"
    exit 0
fi

# 단일 파일 처리
echo "Processing file for ${YEAR}-${MONTH}-${DAY}:"
echo "File: $DAY_FILE"
echo "S3 source: ${S3_PREFIX}${DAY_FILE}"
echo "Local temp: /tmp/temp_${DAY_FILE}"
echo "GCS destination: ${GCS_PREFIX}${DAY_FILE}"

echo "Transferring: ${S3_PREFIX}${DAY_FILE} -> ${GCS_PREFIX}${DAY_FILE}"
# S3에서 로컬로 다운로드
aws s3 cp "${S3_PREFIX}${DAY_FILE}" "/tmp/temp_${DAY_FILE}" --endpoint-url https://files.polygon.io/ --no-verify-ssl > "/tmp/s3_cp_${DAY_FILE}.log" 2>&1
if [ $? -ne 0 ]; then
    echo "Error: aws s3 cp failed for ${DAY_FILE}"
    cat "/tmp/s3_cp_${DAY_FILE}.log"
    exit 1
fi
# GCS로 업로드
gsutil -m cp "/tmp/temp_${DAY_FILE}" "${GCS_PREFIX}${DAY_FILE}" > "/tmp/gsutil_${DAY_FILE}.log" 2>&1
if [ $? -ne 0 ]; then
    echo "Error: gsutil cp failed for ${DAY_FILE}"
    cat "/tmp/gsutil_${DAY_FILE}.log"
    exit 1
fi

rm "/tmp/temp_${DAY_FILE}"
echo "Successfully transferred ${DAY_FILE}"

echo "Script completed"
