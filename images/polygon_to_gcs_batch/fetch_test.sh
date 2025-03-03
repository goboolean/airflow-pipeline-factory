#!/bin/bash
# YEAR는 환경 변수로 반드시 제공되어야 함
if [ -z "$YEAR" ]; then
    echo "Error: YEAR environment variable must be set"
    exit 1
fi

S3_PREFIX="s3://flatfiles/us_stocks_sip/minute_aggs_v1/${YEAR}/"
GCS_PREFIX="gs://goboolean-452007-raw/stock/usa/${YEAR}/"

echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
echo "GOOGLE_CREDENTIALS: ${GOOGLE_CREDENTIALS:0:50}..."  # 일부만 출력
echo "Configuring AWS CLI with environment variables"
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

# 연도 전체를 처리 (월별 루프)
for MONTH in {01..12}; do
    S3_MONTH_PREFIX="${S3_PREFIX}${MONTH}/"
    GCS_MONTH_PREFIX="${GCS_PREFIX}${MONTH}/"

    echo "Running aws s3 ls ${S3_MONTH_PREFIX}"
    aws s3 ls "${S3_MONTH_PREFIX}" --endpoint-url https://files.polygon.io/ --no-verify-ssl > /tmp/s3_list.log 2>&1
    EXIT_CODE=$?
    echo "aws s3 ls exit code: $EXIT_CODE"
    if [ $EXIT_CODE -ne 0 ]; then
        echo "Error: aws s3 ls failed for month ${MONTH}. Check credentials or endpoint."
        cat /tmp/s3_list.log
        continue
    fi

    echo "S3 list log content for month ${MONTH}:"
    cat /tmp/s3_list.log || echo "Failed to read /tmp/s3_list.log"

    if [ ! -s /tmp/s3_list.log ]; then
        echo "Warning: No files found in ${S3_MONTH_PREFIX}"
        continue
    fi

    echo "Processing S3 list output in parallel for month ${MONTH}:"
    while read -r line; do
        FILE=$(echo "$line" | awk '{print $4}')  # 예: 2025-02-03.csv.gz
        if [ -n "$FILE" ]; then
            (
                BASE_NAME="${FILE%.gz}"  # 예: 2025-02-03.csv
                echo "Transferring: ${S3_MONTH_PREFIX}${FILE} -> ${GCS_MONTH_PREFIX}${BASE_NAME}"
                aws s3 cp "${S3_MONTH_PREFIX}${FILE}" "/tmp/temp_${FILE}" --endpoint-url https://files.polygon.io/ --no-verify-ssl > "/tmp/s3_cp_${FILE}.log" 2>&1
                if [ $? -ne 0 ]; then
                    echo "Error: aws s3 cp failed for ${FILE}"
                    cat "/tmp/s3_cp_${FILE}.log"
                    exit 1
                fi
                gzip -d "/tmp/temp_${FILE}"
                if [ $? -ne 0 ]; then
                    echo "Error: gzip decompression failed for ${FILE}"
                    exit 1
                fi
                gsutil cp "/tmp/temp_${BASE_NAME}" "${GCS_MONTH_PREFIX}${BASE_NAME}" 2> "/tmp/gsutil_error_${FILE}.log"
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
done

echo "Script completed"