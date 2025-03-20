FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    bash gzip \
    && rm -rf /var/lib/apt/lists/*

COPY --from=google/cloud-sdk:slim /usr/lib/google-cloud-sdk /usr/lib/google-cloud-sdk
ENV PATH="/usr/lib/google-cloud-sdk/bin:${PATH}"

COPY --from=amazon/aws-cli:latest /usr/local/aws-cli /usr/local/aws-cli
ENV PATH="/usr/local/aws-cli/v2/current/bin:${PATH}"

WORKDIR /app
COPY polygon_to_gcs_daily.sh .
RUN chmod +x polygon_to_gcs_daily.sh


ENTRYPOINT ["./polygon_to_gcs_daily.sh"]
