FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
COPY ./resample_ticker.py /app/resample_ticker.py
ENTRYPOINT ["python", "/app/resample_ticker.py"]
