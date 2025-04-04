import pytest
import os
import json
import pandas as pd
import time
import docker
from testcontainers.influxdb import InfluxDbContainer
from testcontainers.core.container import DockerContainer
from google.cloud import storage
from influxdb_client import InfluxDBClient
import uuid
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.handlers = [handler]


@pytest.fixture(scope="session")
def docker_client():
    client = docker.from_env()
    network_name = f"test_network_{uuid.uuid4().hex[:8]}"
    try:
        network = client.networks.create(network_name, driver="bridge")
    except docker.errors.APIError as e:
        logger.warning(f"Network {network_name} already exists or error: {e}")
        network = client.networks.get(network_name)
    yield client, network
    try:
        # 모든 컨테이너 확인 및 제거
        for container in client.containers.list(all=True):
            if network.name in [n.get('Name') for n in container.attrs['NetworkSettings']['Networks'].values()]:
                logger.info(f"Removing container: {container.name}")
                container.stop()  # 먼저 중지
                container.remove(force=True)  # 강제 제거
                time.sleep(1)  # 제거 후 대기
        # 네트워크 제거 시도
        network.reload()  # 네트워크 상태 갱신
        if network.containers:  # 남은 컨테이너 확인
            logger.warning(f"Network {network_name} still has containers: {network.containers}")
        network.remove()
        logger.info(f"Removed network: {network_name}")
    except docker.errors.APIError as e:
        logger.warning(f"Failed to remove network {network_name}: {e}")


@pytest.fixture(scope="session")
def influx_db(docker_client):
    client, network = docker_client
    logger.info("Starting InfluxDB container...")
    influx_container_name = "influxdb"  # 고정된 이름 사용

    # 이미 실행 중인 컨테이너가 있으면 제거
    try:
        old_container = client.containers.get(influx_container_name)
        logger.info(f"Removing existing container: {influx_container_name}")
        old_container.remove(force=True)
    except docker.errors.NotFound:
        pass

    influx = InfluxDbContainer("influxdb:latest") \
        .with_env("DOCKER_INFLUXDB_INIT_MODE", "setup") \
        .with_env("DOCKER_INFLUXDB_INIT_USERNAME", "admin") \
        .with_env("DOCKER_INFLUXDB_INIT_PASSWORD", "password") \
        .with_env("DOCKER_INFLUXDB_INIT_ORG", "test-org") \
        .with_env("DOCKER_INFLUXDB_INIT_BUCKET", "test-bucket") \
        .with_env("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", "test-token")
    influx._configure()
    influx_container = client.containers.run(
        image=str(influx.image),
        environment=influx.env,
        ports={'8086/tcp': None},
        network=network.name,
        name=influx_container_name,
        detach=True
    )
    influx._container = influx_container
    influx.start()
    logger.info(
        f"InfluxDB container started at http://{influx.get_container_host_ip()}:{influx.get_exposed_port(8086)}")
    yield influx
    logger.info("Stopping InfluxDB container...")
    influx.stop()
    influx_container.remove(force=True)  # 강제 제거 추가


@pytest.fixture(scope="session")
def gcs_server(docker_client):
    client, network = docker_client
    # 고정된 이름 사용 - Docker 네트워크 내에서 일관된 이름으로 접근 가능
    gcs_container_name = "fake-gcs-server"
    logger.info(f"Starting fake GCS server with name {gcs_container_name}...")
    gcs = DockerContainer("fsouza/fake-gcs-server:latest") \
        .with_exposed_ports(4443) \
        .with_command("-scheme http")
    gcs._configure()

    # 이미 실행 중인 컨테이너가 있으면 제거
    try:
        old_container = client.containers.get(gcs_container_name)
        logger.info(f"Removing existing container: {gcs_container_name}")
        old_container.remove(force=True)
    except docker.errors.NotFound:
        pass

    try:
        gcs_container = client.containers.run(
            image=str(gcs.image),
            command="-scheme http",
            ports={'4443/tcp': None},
            network=network.name,
            name=gcs_container_name,
            detach=True
        )
    except docker.errors.APIError as e:
        logger.error(f"Failed to start GCS server: {e}")
        raise

    gcs._container = gcs_container
    gcs.start()
    max_attempts = 30
    host_endpoint = f"http://{gcs.get_container_host_ip()}:{gcs.get_exposed_port(4443)}"
    for _ in range(max_attempts):
        try:
            client = storage.Client(client_options={"api_endpoint": host_endpoint})
            client.list_buckets()
            break
        except Exception:
            time.sleep(1)
    else:
        raise Exception(f"fake-gcs-server ({gcs_container_name}) failed to start within timeout")
    logger.info(f"Fake GCS server started at {host_endpoint}")
    yield gcs
    logger.info("Stopping fake GCS server...")
    gcs.stop()
    gcs_container.remove(force=True)  # 강제 제거 추가


@pytest.fixture(scope="session")
def test_env(influx_db, gcs_server, docker_client):
    _, network = docker_client

    # 호스트 머신의 IP를 직접 참조할 수 있는 특수 DNS 이름
    host_reference = "host.docker.internal"

    # 호스트에서 접근할 때 사용하는 엔드포인트 (로컬 IP와 매핑된 포트 사용)
    host_gcs_port = gcs_server.get_exposed_port(4443)
    host_influx_port = influx_db.get_exposed_port(8086)

    host_gcs_endpoint = f"http://localhost:{host_gcs_port}"
    host_influx_url = f"http://localhost:{host_influx_port}"

    # Docker 컨테이너에서 호스트 머신에 접근할 때 사용하는 엔드포인트
    container_gcs_endpoint = f"http://{host_reference}:{host_gcs_port}"
    container_influx_url = f"http://{host_reference}:{host_influx_port}"

    influx_token = "test-token"
    influx_org = "test-org"
    influx_bucket = "test-bucket"

    env = {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "STORAGE_EMULATOR_HOST": container_gcs_endpoint  # 컨테이너에서 호스트 머신으로 접근
    }
    os.environ.update(env)

    # 호스트에서만 GCS 직접 접근 (버킷 생성 등)
    storage_client = storage.Client(client_options={"api_endpoint": host_gcs_endpoint})
    for bucket_name in ["goboolean-452007-raw", "goboolean-452007-resampled"]:
        bucket = storage_client.bucket(bucket_name)
        if not bucket.exists():
            logger.info(f"Creating GCS bucket: {bucket_name}")
            bucket.create()

    return {
        "gcs_endpoint": container_gcs_endpoint,  # 컨테이너에서 사용할 엔드포인트
        "host_gcs_endpoint": host_gcs_endpoint,  # 호스트에서 사용할 엔드포인트
        "influx_url": container_influx_url,  # 컨테이너에서 사용할 InfluxDB URL
        "host_influx_url": host_influx_url,  # 호스트에서 사용할 InfluxDB URL
        "influx_token": influx_token,
        "influx_org": influx_org,
        "influx_bucket": influx_bucket,
        "env": env,
        "network": network
    }


@pytest.fixture(scope="session")
def sample_data(test_env, tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("sample_data")
    sample_file = os.path.join(temp_dir, "2023-01-01.csv.gz")

    start_time = 1672531200000000000
    data = {
        "window_start": [],
        "ticker": [],
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": []
    }
    tickers = ["AAPL", "MSFT", "AMZN", "GOOGL"]

    for i in range(1440):
        for ticker in tickers:
            data["window_start"].append(start_time + i * 60 * 1000000000)
            data["ticker"].append(ticker)
            price = 100 + (i % 10)
            data["open"].append(price - 0.5)
            data["high"].append(price + 1.0)
            data["low"].append(price - 1.0)
            data["close"].append(price + 0.5)
            data["volume"].append(1000 + i * 10)

    df = pd.DataFrame(data)
    df.to_csv(sample_file, compression="gzip", index=False)
    logger.info(f"Created sample data file at {sample_file}")

    year, month, day = "2023", "01", "01"
    storage_client = storage.Client(client_options={"api_endpoint": test_env["host_gcs_endpoint"]})
    bucket = storage_client.bucket("goboolean-452007-raw")
    target_path = f"stock/usa/{year}/{month}/{year}-{month}-{day}.csv.gz"
    blob = bucket.blob(target_path)
    blob.upload_from_filename(sample_file)
    logger.info(f"Uploaded sample data to GCS: gs://goboolean-452007-raw/{target_path}")

    return {
        "file_path": sample_file,
        "year": year,
        "month": month,
        "day": day,
        "tickers": tickers
    }


@pytest.fixture(scope="session")
def build_images(docker_client):
    client, _ = docker_client
    images = {}
    logger.info("Building Docker images...")

    components = {
        "split_ticker": "../../images/daily-pipeline/split_ticker",
        "resample_ticker": "../../images/daily-pipeline/resample_ticker",
        "upload_to_influxdb": "../../images/daily-pipeline/upload_to_influxdb"
    }

    for component, build_path in components.items():
        image_name = f"test_e2e_{component}_{uuid.uuid4().hex[:8]}"
        build_path = os.path.abspath(build_path)
        logger.info(f"Building {component} image from {build_path}")

        try:
            image, _ = client.images.build(
                path=build_path,
                tag=image_name,
                rm=True,
                forcerm=True
            )
            images[component] = image_name
            logger.info(f"Successfully built {component} image: {image_name}")
        except Exception as e:
            logger.error(f"Failed to build {component} image: {e}")
            pytest.fail(f"Docker build failed for {component}: {e}")

    return images


def test_end_to_end_pipeline(test_env, sample_data, build_images, docker_client):
    client, network = docker_client
    year, month, day = sample_data["year"], sample_data["month"], sample_data["day"]
    ticker = sample_data["tickers"][0]  # "AAPL"

    logger.info(f"Starting E2E test for {year}-{month}-{day}, ticker: {ticker}")

    # 호스트에서 GCS 접근 시 host_gcs_endpoint 사용
    storage_client = storage.Client(client_options={"api_endpoint": test_env["host_gcs_endpoint"]})

    # Docker의 host.docker.internal을 사용하기 위한 설정
    extra_hosts = {"host.docker.internal": "host-gateway"}

    # 1. Split Ticker
    logger.info("Running split_ticker container...")
    split_ticker_container = client.containers.run(
        build_images["split_ticker"],
        command=[year, month, day],
        environment=test_env["env"],
        network=network.name,
        extra_hosts=extra_hosts,  # host.docker.internal 설정 추가
        detach=True
    )

    logs = []
    for line in split_ticker_container.logs(stream=True):
        log_line = line.decode('utf-8').strip()
        logger.info(f"split_ticker: {log_line}")
        logs.append(log_line)

    exit_code = split_ticker_container.wait()["StatusCode"]
    if exit_code != 0:
        full_logs = "\n".join(logs)
        logger.error(f"split_ticker container logs:\n{full_logs}")
        pytest.fail(f"split_ticker container failed with exit code {exit_code}. Logs:\n{full_logs}")

    bucket = storage_client.bucket("goboolean-452007-resampled")
    ticker_blob_path = f"stock/usa/{ticker}/1m/{year}/{month}/{ticker}_{year}-{month}-{day}.csv.gz"
    assert bucket.blob(ticker_blob_path).exists(), "split_ticker 실패"
    logger.info(f"split_ticker completed successfully, created: {ticker_blob_path}")

    # 2. Resample Ticker
    logger.info("Running resample_ticker container...")
    resample_ticker_container = client.containers.run(
        build_images["resample_ticker"],
        command=[year, month, day, ticker],
        environment=test_env["env"],
        network=network.name,
        extra_hosts=extra_hosts,  # host.docker.internal 설정 추가
        detach=True
    )

    logs = []
    for line in resample_ticker_container.logs(stream=True):
        log_line = line.decode('utf-8').strip()
        logger.info(f"resample_ticker: {log_line}")
        logs.append(log_line)

    exit_code = resample_ticker_container.wait()["StatusCode"]
    if exit_code != 0:
        full_logs = "\n".join(logs)
        logger.error(f"resample_ticker container logs:\n{full_logs}")
        pytest.fail(f"resample_ticker container failed with exit code {exit_code}. Logs:\n{full_logs}")

    periods = ["5m", "10m", "15m", "30m", "1h", "4h", "1d"]
    for period in periods:
        resampled_blob_path = f"stock/usa/{ticker}/{period}/{year}/{month}/{ticker}_{year}-{month}-{day}_{period}.csv.gz"
        assert bucket.blob(resampled_blob_path).exists(), f"resample_ticker 실패: {period}"
    logger.info("resample_ticker completed successfully")

    # 3. Upload to InfluxDB
    logger.info("Running upload_to_influxdb container...")
    upload_influx_container = client.containers.run(
        build_images["upload_to_influxdb"],
        command=[
            year, month, day, ticker,
            test_env["influx_url"], test_env["influx_token"],
            test_env["influx_org"], test_env["influx_bucket"]
        ],
        environment=test_env["env"],
        network=network.name,
        extra_hosts=extra_hosts,  # host.docker.internal 설정 추가
        detach=True
    )

    logs = []
    for line in upload_influx_container.logs(stream=True):
        log_line = line.decode('utf-8').strip()
        logger.info(f"upload_to_influxdb: {log_line}")
        logs.append(log_line)

    exit_code = upload_influx_container.wait()["StatusCode"]
    if exit_code != 0:
        full_logs = "\n".join(logs)
        logger.error(f"upload_to_influxdb container logs:\n{full_logs}")
        pytest.fail(f"upload_to_influxdb container failed with exit code {exit_code}. Logs:\n{full_logs}")

    logger.info("Verifying data in InfluxDB...")
    client = InfluxDBClient(url=test_env["host_influx_url"], token=test_env["influx_token"], org=test_env["influx_org"])
    try:
        time.sleep(3)
        # 2023-01-01 데이터를 포함하도록 범위 수정
        query = f'from(bucket:"{test_env["influx_bucket"]}") |> range(start: 2023-01-01T00:00:00Z, stop: 2023-01-02T00:00:00Z) |> filter(fn:(r) => r._measurement == "stock_price" and r.ticker == "{ticker}")'
        tables = client.query_api().query(query)

        assert len(tables) > 0, "InfluxDB 업로드 실패"
        assert any(record.get_value() for table in tables for record in table.records), "InfluxDB 데이터 비어 있음"

        logger.info(f"Found {sum(len(table.records) for table in tables)} records in InfluxDB")
    finally:
        client.close()

    logger.info("E2E test completed successfully!")


if __name__ == "__main__":
    pytest.main(["-v", __file__])
