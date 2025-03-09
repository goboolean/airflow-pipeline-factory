# Google Cloud SDK 베이스 이미지 (멀티-아키텍처 지원)
FROM google/cloud-sdk:latest AS gcloud-stage

# AWS CLI 베이스 이미지 (멀티-아키텍처 지원)
FROM amazon/aws-cli:latest AS aws-stage

# 최종 이미지 구성
FROM python:latest

# 작업 디렉토리 및 스크립트 설정
WORKDIR /app
COPY polygon_to_gcs_monthly.sh .
RUN chmod +x polygon_to_gcs_monthly.sh

# 기본 실행 명령어
CMD ["./polygon_to_gcs_monthly.sh"]
