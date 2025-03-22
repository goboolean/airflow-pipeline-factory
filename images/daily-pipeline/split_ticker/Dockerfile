FROM python:3.9-slim
RUN pip install pandas google-cloud-storage
COPY ./split_ticker.py /app/split_ticker.py
WORKDIR /app
ENTRYPOINT ["python", "/app/split_ticker.py"]
