FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
COPY ./upload_to_influxdb.py /app/upload_to_influxdb.py
ENTRYPOINT ["python", "/app/upload_to_influxdb.py"]
