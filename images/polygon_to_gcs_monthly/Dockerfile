FROM python:3.9-slim

# 필수 패키지 설치 (curl, unzip, gzip 추가)
RUN apt-get update && apt-get install -y \
    curl unzip gzip \
    && rm -rf /var/lib/apt/lists/*

# AWS CLI 설치
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws

# Google Cloud SDK 설치
RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-363.0.0-linux-arm.tar.gz \
    && tar -xzf google-cloud-sdk-363.0.0-linux-arm.tar.gz \
    && ./google-cloud-sdk/install.sh --quiet \
    && rm -rf google-cloud-sdk-363.0.0-linux-arm.tar.gz
ENV PATH="/google-cloud-sdk/bin:${PATH}"

WORKDIR /app
COPY fetch_test.sh .
RUN chmod +x fetch_test.sh

CMD ["./fetch_test.sh"]
