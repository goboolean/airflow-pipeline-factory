FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt
COPY ./split_ticker.py /app/split_ticker.py
ENTRYPOINT ["python", "/app/split_ticker.py"]
