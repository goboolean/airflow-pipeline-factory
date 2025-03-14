#!/bin/bash

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
    if [ -z "$YEAR" ] || [ -z "$MONTH" ]; then
        echo "Error: YEAR and MONTH environment variables must be set"
        exit 1
    fi
}

# 환경변수 확인
validate_env_variables
# AWS 인증 설정
setup_aws_auth
# GCS 인증 설정
setup_gcs_auth

S3_PREFIX="s3://flatfiles/us_stocks_sip/minute_aggs_v1/${YEAR}/${MONTH}/"
GCS_PREFIX="gs://goboolean-452007-raw/stock/usa/${YEAR}/${MONTH}/"

echo "Checking if GCS credential file exists:"
ls -l "$GOOGLE_APPLICATION_CREDENTIALS" 2>&1 || echo "GCS credential file not generated"
echo "Activating GCS service account:"
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS" 2>&1 || echo "Failed to activate GCS service account"
echo "Checking GCS auth:"
gsutil ls gs://goboolean-452007-raw/ 2>&1 || echo "GCS auth failed but continuing"
echo "Processing ${S3_PREFIX}"

echo "Running aws s3 ls ${S3_PREFIX}"
aws s3 ls "${S3_PREFIX}" --endpoint-url https://files.polygon.io/ --no-verify-ssl > /tmp/s3_list.log 2>&1
EXIT_CODE=$?
echo "aws s3 ls exit code: $EXIT_CODE"
if [ $EXIT_CODE -ne 0 ]; then
    echo "Error: aws s3 ls failed. Check credentials or endpoint."
    cat /tmp/s3_list.log
    exit 1
fi

echo "S3 list log content:"
cat /tmp/s3_list.log || echo "Failed to read /tmp/s3_list.log"

if [ ! -s /tmp/s3_list.log ]; then
    echo "Warning: No files found in ${S3_PREFIX} or empty output"
    aws configure list
    exit 0
fi

# 병렬 처리 함수 정의
process_file() {
    local file="$1"
    if [ -n "$file" ]; then
        # 디버깅: 변수 값 확인
        echo "File: $file"
        echo "S3 source: ${S3_PREFIX}${file}"
        echo "Local temp: /tmp/temp_${file}"
        echo "GCS destination: ${GCS_PREFIX}${file}"

        echo "Transferring: ${S3_PREFIX}${file} -> ${GCS_PREFIX}${file}"
        # S3에서 로컬로 다운로드
        aws s3 cp "${S3_PREFIX}${file}" "/tmp/temp_${file}" --endpoint-url https://files.polygon.io/ --no-verify-ssl > "/tmp/s3_cp_${file}.log" 2>&1
        if [ $? -ne 0 ]; then
            echo "Error: aws s3 cp failed for ${file}"
            cat "/tmp/s3_cp_${file}.log"
            echo "AWS config check:"
            aws configure list
            exit 1
        fi
        # .gz 파일 그대로 GCS로 업로드 (병렬 옵션 사용)
        gsutil -m cp "/tmp/temp_${file}" "${GCS_PREFIX}${file}" > "/tmp/gsutil_${file}.log" 2>&1
        if [ $? -ne 0 ]; then
            echo "Error: gsutil cp failed for ${file}"
            cat "/tmp/gsutil_${file}.log"
            exit 1
        fi
        rm "/tmp/temp_${file}"
        echo "Successfully transferred ${file}"
    fi
}

# 환경 변수와 함수 내보내기 (병렬 실행을 위해)
export -f process_file
export S3_PREFIX
export GCS_PREFIX
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY

echo "Processing S3 list output in parallel:"
# S3 리스트에서 파일 이름만 추출 후 병렬 처리
NUM_PROCESSES=8
grep -v "urllib3/connectionpool.py" /tmp/s3_list.log | awk '{print $4}' | xargs -I {} -P "$NUM_PROCESSES" bash -c 'process_file "{}"'

wait
echo "Script completed"
