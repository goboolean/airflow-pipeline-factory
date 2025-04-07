import pytest
import logging
import os
import time
import sys
from urllib.parse import quote
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from testcontainers.core.container import DockerContainer
import docker
import requests

# 로깅 설정
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
    """Docker 클라이언트 및 네트워크 설정 (Arrange)"""
    client = docker.from_env()
    network_name = "test_network_gcs"
    try:
        network = client.networks.create(network_name, driver="bridge")
        logger.info(f"Created network: {network_name}")
    except docker.errors.APIError as e:
        logger.warning(f"Network {network_name} already exists or error: {e}")
        network = client.networks.get(network_name)
    yield client, network
    # Cleanup: 실행된 모든 컨테이너 종료 및 네트워크 제거
    try:
        for container in client.containers.list(all=True):
            if network.name in [n.get('Name') for n in container.attrs['NetworkSettings']['Networks'].values()]:
                logger.info(f"Removing container: {container.name}")
                container.stop()
                container.remove(force=True)
                time.sleep(1)
        network.remove()
        logger.info(f"Removed network: {network_name}")
    except docker.errors.APIError as e:
        logger.warning(f"Failed to remove network {network_name}: {e}")


@pytest.fixture(scope="session")
def gcs_server(docker_client):
    """fake-gcs-server 컨테이너 설정 및 실행 (Arrange)"""
    client, network = docker_client
    gcs_container_name = "fake-gcs-server"
    logger.info(f"Starting fake GCS server with name {gcs_container_name}...")
    gcs = DockerContainer("fsouza/fake-gcs-server:latest") \
        .with_exposed_ports(4443) \
        .with_command("-scheme http")
    gcs._configure()

    # 기존 컨테이너가 있으면 제거
    try:
        old_container = client.containers.get(gcs_container_name)
        logger.info(f"Removing existing container: {gcs_container_name}")
        old_container.remove(force=True)
        time.sleep(1)
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
    host_endpoint = f"http://{gcs.get_container_host_ip()}:{gcs.get_exposed_port(4443)}"
    os.environ["STORAGE_EMULATOR_HOST"] = host_endpoint
    logger.info(f"Set STORAGE_EMULATOR_HOST to {host_endpoint}")
    logger.info(f"Exposed port for 4443: {gcs.get_exposed_port(4443)}")
    logger.info(f"Container port mapping: {gcs_container.attrs['NetworkSettings']['Ports']}")

    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            test_client = storage.Client(
                credentials=AnonymousCredentials(),
                project="test",
                client_options={"api_endpoint": host_endpoint}
            )
            test_client.list_buckets()
            response = requests.get(f"{host_endpoint}/storage/v1/b", timeout=5)
            logger.info(f"Server health check response: {response.status_code}")
            logger.info(f"Fake GCS server started at {host_endpoint}")
            break
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_attempts}: Waiting for server - {str(e)}")
            time.sleep(1)
    else:
        container_logs = gcs_container.logs().decode('utf-8')
        logger.error(f"Container logs:\n{container_logs}")
        raise Exception(f"fake-gcs-server ({gcs_container_name}) failed to start within timeout")

    yield gcs, host_endpoint

    # Cleanup: 컨테이너 종료 및 제거, 이미지도 삭제 시도
    logger.info("Stopping fake GCS server...")
    gcs.stop()
    gcs_container.remove(force=True)
    try:
        client.images.remove("fsouza/fake-gcs-server:latest", force=True)
        logger.info("Removed image: fsouza/fake-gcs-server:latest")
    except Exception as e:
        logger.warning(f"Failed to remove fake-gcs-server image: {e}")


def test_fake_gcs_server(gcs_server):
    """fake-gcs-server 모듈 테스트 (Arrange, Act, Assert)"""
    # Arrange: 설정 및 client 생성
    gcs_container, endpoint = gcs_server
    os.environ["STORAGE_EMULATOR_HOST"] = endpoint
    client = storage.Client(
        credentials=AnonymousCredentials(),
        project="test",
        client_options={"api_endpoint": endpoint}
    )
    client._connection.API_BASE_URL = endpoint
    logger.info(f"Testing fake-gcs-server at endpoint: {endpoint}")

    bucket_name = "goboolean-452007-raw"
    blob_name = "stock/usa/2023/01/2023-01-01.csv.gz"
    expected_content = "Sample CSV content"
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        bucket.create()
    blob = bucket.blob(blob_name)

    # Act: Blob 업로드 및 media_link 재정의
    blob.upload_from_string(expected_content)
    download_url = f"{endpoint}/download/storage/v1/b/{bucket_name}/o/{quote(blob.name, safe='')}?alt=media"
    logger.info(f"Overriding blob.media_link with: {download_url}")

    # blob.media_link를 해당 인스턴스에 한해 재정의 (다른 인스턴스에 영향 주지 않음)
    def custom_media_link(_self):
        return download_url

    blob.__class__.media_link = property(custom_media_link)

    # Act: 재정의된 media_link를 사용하여 다운로드
    downloaded_content = blob.download_as_text(client=client)

    # Assert: 업로드한 내용과 다운로드한 내용이 일치하는지 확인
    assert bucket.exists(), f"Bucket {bucket_name} should exist"
    assert blob.exists(), f"Blob {blob_name} should exist"
    assert downloaded_content == expected_content, (
        f"Expected content '{expected_content}', but got '{downloaded_content}'"
    )
    logger.info("Fake GCS server test completed successfully")


if __name__ == "__main__":
    pytest.main(["-v", "-s"])
