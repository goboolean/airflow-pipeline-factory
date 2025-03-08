# Google Cloud SDK 베이스 이미지 (멀티-아키텍처 지원)
FROM google/cloud-sdk:latest AS gcloud-stage

# AWS CLI 베이스 이미지 (멀티-아키텍처 지원)
FROM amazon/aws-cli:latest AS aws-stage

# 최종 이미지 구성
FROM alpine:latest

# Python 최신 버전 설치
RUN apk add --no-cache python3 py3-pip

# Google Cloud SDK 복사
COPY --from=gcloud-stage /google-cloud-sdk/ /google-cloud-sdk/

# AWS CLI 복사
COPY --from=aws-stage /usr/local/aws-cli/ /usr/local/aws-cli/

# PATH 설정
ENV PATH="/usr/local/aws-cli/v2/current/bin:/google-cloud-sdk/bin:${PATH}"

# 작업 디렉토리 및 스크립트 설정
WORKDIR /app
COPY fetch_test.sh .
RUN chmod +x fetch_test.sh

# 기본 실행 명령어
CMD ["./fetch_test.sh"]
