#!/bin/bash
# YEAR와 MONTH는 환경 변수로 반드시 제공되어야 함
if [ -z "$YEAR" ] || [ -z "$MONTH" ]; then
    echo "Error: YEAR and MONTH environment variables must be set"
    exit 1
fi

S3_PREFIX="s3://flatfiles/us_stocks_sip/minute_aggs_v1/${YEAR}/${MONTH}/"
GCS_PREFIX="gs://goboolean-452007-raw/stock/usa/${YEAR}/${MONTH}/"

aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"

# GOOGLE_CREDENTIALS를 임시 파일로 저장
if [ -n "$GOOGLE_CREDENTIALS" ]; then
    echo "Using GOOGLE_CREDENTIALS from environment variable"
    echo "$GOOGLE_CREDENTIALS" > /tmp/gcp_credentials.json
    export GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp_credentials.json
else
    echo "Error: GOOGLE_CREDENTIALS environment variable not set"
    exit 1
fi

echo "Checking if GCS credential file exists:"
ls -l "$GOOGLE_APPLICATION_CREDENTIALS" 2>&1 || echo "GCS credential file not generated"
echo "Activating GCS service account:"
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS" 2>&1 || echo "Failed to activate GCS service account"
echo "Checking GCS auth:"
gsutil ls gs://goboolean-452007-raw/ 2>&1 || echo "GCS auth failed but continuing"
echo "Processing ${S3_PREFIX}..."

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

echo "Processing S3 list output in parallel:"
while read -r line; do
    FILE=$(echo "$line" | awk '{print $4}')  # 예: 2025-02-03.csv.gz
    if [ -n "$FILE" ]; then
        (
            # 파일명에서 .gz 제거
            BASE_NAME="${FILE%.gz}"  # 예: 2025-02-03.csv
            echo "Transferring: ${S3_PREFIX}${FILE} -> ${GCS_PREFIX}${BASE_NAME}"
            # S3에서 로컬로 다운로드
            aws s3 cp "${S3_PREFIX}${FILE}" "/tmp/temp_${FILE}" --endpoint-url https://files.polygon.io/ --no-verify-ssl > "/tmp/s3_cp_${FILE}.log" 2>&1
            if [ $? -ne 0 ]; then
                echo "Error: aws s3 cp failed for ${FILE}"
                cat "/tmp/s3_cp_${FILE}.log"
                exit 1
            fi
            # 압축 해제
            gzip -d "/tmp/temp_${FILE}"
            if [ $? -ne 0 ]; then
                echo "Error: gzip decompression failed for ${FILE}"
                exit 1
            fi
            # GCS로 업로드 (압축 해제된 파일)
            gsutil cp "/tmp/temp_${BASE_NAME}" "${GCS_PREFIX}${BASE_NAME}" 2> "/tmp/gsutil_error_${FILE}.log"
            if [ $? -ne 0 ]; then
                echo "Error: gsutil cp failed for ${BASE_NAME}"
                cat "/tmp/gsutil_error_${FILE}.log"
                exit 1
            fi
            rm "/tmp/temp_${BASE_NAME}"
            echo "Successfully transferred ${BASE_NAME}"
        ) &
    fi
done < <(grep -v "urllib3/connectionpool.py" /tmp/s3_list.log)

wait
echo "Script completed"
