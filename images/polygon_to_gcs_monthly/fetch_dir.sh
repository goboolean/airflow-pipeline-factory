#!/bin/bash
YEAR=${YEAR:-2025}
MONTH=${MONTH:-02}

# S3 디렉토리에서 파일 목록 가져오기
S3_PREFIX="s3://flatfiles/us_stocks_sip/minute_aggs_v1/${YEAR}/${MONTH}/"
GCS_PREFIX="gs://goboolean-452007-stock-data/${YEAR}/${MONTH}/"

# aws s3 ls로 파일 목록 가져와 각 파일 스트리밍
aws s3 ls "${S3_PREFIX}" --endpoint-url https://files.polygon.io/ --no-verify-ssl | \
while read -r line; do
    # 파일 이름 추출 (예: "2025-02-20.csv.gz")
    FILE=$(echo "$line" | awk '{print $4}')
    if [ -n "$FILE" ]; then
        echo "Processing: ${S3_PREFIX}${FILE} -> ${GCS_PREFIX}${FILE}"
        aws s3 cp "${S3_PREFIX}${FILE}" - --endpoint-url https://files.polygon.io/ --no-verify-ssl | \
        gsutil cp - "${GCS_PREFIX}${FILE}"
    fi
done