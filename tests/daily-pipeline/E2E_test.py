import pytest
import os
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
        for container in client.containers.list(all=True):
            if network.name in [n.get('Name') for n in container.attrs['NetworkSettings']['Networks'].values()]:
                logger.info(f"Removing container: {container.name}")
                container.stop()
                container.remove(force=True)
                time.sleep(1)
        network.reload()
        if network.containers:
            logger.warning(f"Network {network_name} still has containers: {network.containers}")
        network.remove()
        logger.info(f"Removed network: {network_name}")
    except docker.errors.APIError as e:
        logger.warning(f"Failed to remove network {network_name}: {e}")


@pytest.fixture(scope="session")
def influx_db(docker_client):
    client, network = docker_client
    logger.info("Starting InfluxDB container...")
    influx_container_name = "influxdb"
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
    influx_container.remove(force=True)


@pytest.fixture(scope="session")
def gcs_server(docker_client):
    client, network = docker_client
    gcs_container_name = "fake-gcs-server"
    logger.info(f"Starting fake GCS server with name {gcs_container_name}...")
    gcs = DockerContainer("fsouza/fake-gcs-server:latest") \
        .with_exposed_ports(4443) \
        .with_command("-scheme http")
    gcs._configure()

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
    gcs_container.remove(force=True)


@pytest.fixture(scope="session")
def test_env(influx_db, gcs_server, docker_client):
    _, network = docker_client
    host_reference = "host.docker.internal"
    host_gcs_port = gcs_server.get_exposed_port(4443)
    host_influx_port = influx_db.get_exposed_port(8086)

    host_gcs_endpoint = f"http://localhost:{host_gcs_port}"
    host_influx_url = f"http://localhost:{host_influx_port}"
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
    # ARRANGE: 준비 단계 - 테스트 데이터를 설정하고, 필요한 파라미터들을 추출
    client, network = docker_client
    year, month, day = sample_data["year"], sample_data["month"], sample_data["day"]
    ticker = sample_data["tickers"][0]  # "AAPL"
    logger.info(f"Starting E2E test for {year}-{month}-{day}, ticker: {ticker}")
    storage_client = storage.Client(client_options={"api_endpoint": test_env["host_gcs_endpoint"]})
    bucket = storage_client.bucket("goboolean-452007-resampled")
    extra_hosts = {"host.docker.internal": "host-gateway"}

    # ACT: 실행 단계
    # 1. Split Ticker 컨테이너 실행 및 결과 확인
    logger.info("Running split_ticker container...")
    split_ticker_container = client.containers.run(
        build_images["split_ticker"],
        command=[year, month, day],
        environment=test_env["env"],
        network=network.name,
        extra_hosts=extra_hosts,
        detach=True
    )
    split_logs = []
    for line in split_ticker_container.logs(stream=True):
        split_logs.append(line.decode('utf-8').strip())
    exit_code = split_ticker_container.wait()["StatusCode"]
    if exit_code != 0:
        pytest.fail(f"split_ticker failed with exit code {exit_code}. Logs:\n" + "\n".join(split_logs))
    split_blob_path = f"stock/usa/{ticker}/1m/{year}/{month}/{ticker}_{year}-{month}-{day}_1m.csv.gz"

    # 2. Resample Ticker 컨테이너 실행 및 결과 파일 업로드 확인
    logger.info("Running resample_ticker container...")
    resample_ticker_container = client.containers.run(
        build_images["resample_ticker"],
        command=[year, month, day, ticker],
        environment=test_env["env"],
        network=network.name,
        extra_hosts=extra_hosts,
        detach=True
    )
    resample_logs = []
    for line in resample_ticker_container.logs(stream=True):
        resample_logs.append(line.decode('utf-8').strip())
    exit_code = resample_ticker_container.wait()["StatusCode"]
    if exit_code != 0:
        pytest.fail(f"resample_ticker failed with exit code {exit_code}. Logs:\n" + "\n".join(resample_logs))

    # 3. Upload to InfluxDB 컨테이너 실행
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
        extra_hosts=extra_hosts,
        detach=True
    )
    upload_logs = []
    for line in upload_influx_container.logs(stream=True):
        upload_logs.append(line.decode('utf-8').strip())
    exit_code = upload_influx_container.wait()["StatusCode"]
    if exit_code != 0:
        pytest.fail(f"upload_to_influxdb failed with exit code {exit_code}. Logs:\n" + "\n".join(upload_logs))

    # ASSERT: 검증 단계
    # A. split_ticker 결과 검증
    assert bucket.blob(split_blob_path).exists(), "split_ticker did not produce the expected blob."
    logger.info(f"split_ticker produced: {split_blob_path}")

    # B. resample_ticker 결과 검증 (모든 주기는 '_norm' 접미사 사용)
    for period in ["1m", "5m", "10m", "15m", "30m", "1h", "4h", "1d"]:
        resampled_blob_path = f"stock/usa/{ticker}/{period}_norm/{year}/{month}/{ticker}_{year}-{month}-{day}_{period}_norm.csv.gz"
        assert bucket.blob(
            resampled_blob_path).exists(), f"resample_ticker did not produce the expected blob for period: {period}"
    logger.info("All resample_ticker blobs verified.")

    # C. InfluxDB 데이터 검증
    logger.info("Verifying data in InfluxDB...")
    client_influx = InfluxDBClient(url=test_env["host_influx_url"],
                                   token=test_env["influx_token"],
                                   org=test_env["influx_org"])
    try:
        time.sleep(3)
        query = f'from(bucket:"{test_env["influx_bucket"]}") |> range(start: 2023-01-01T00:00:00Z, stop: 2023-01-02T00:00:00Z) ' \
                f'|> filter(fn:(r) => r._measurement == "stock_price" and r.ticker == "{ticker}")'
        tables = client_influx.query_api().query(query)
        assert len(tables) > 0, "No data was returned from InfluxDB."
        assert any(record.get_value() for table in tables for record in table.records), "InfluxDB data is empty."
        logger.info(f"InfluxDB contains {sum(len(table.records) for table in tables)} records.")
    finally:
        client_influx.close()

    logger.info("E2E test completed successfully!")


if __name__ == "__main__":
    pytest.main(["-v", __file__])
