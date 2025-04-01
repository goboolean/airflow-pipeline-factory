import pytest
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
import os
import requests
import logging

logging.basicConfig(level=logging.INFO)


@pytest.fixture(scope="module")
def fake_gcs_server():
    gcs_container = DockerContainer("fsouza/fake-gcs-server:latest").with_exposed_ports(4443).with_command(
        ["-scheme", "http", "-port", "4443"])
    gcs_container.start()
    wait_for_logs(gcs_container, "server started at", timeout=30)

    host = "localhost"
    port = gcs_container.get_exposed_port(4443)
    endpoint = f"http://{host}:{port}"
    os.environ["STORAGE_EMULATOR_HOST"] = endpoint

    print(f"STORAGE_EMULATOR_HOST set to: {endpoint}")
    print(f"Container logs: {gcs_container.get_logs()}")

    try:
        response = requests.get(f"{endpoint}/storage/v1/b", timeout=5)
        print(f"Manual test response: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Manual test failed: {e}")

    yield gcs_container, endpoint
    gcs_container.stop()


def download_blob_text(blob, endpoint):
    """GCS Blob을 다운로드하는 커스텀 함수"""
    url = f"{endpoint}/download/storage/v1/b/{blob.bucket.name}/o/{blob.name}?alt=media"
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.text


def test_fake_gcs_server(fake_gcs_server):
    gcs_container, endpoint = fake_gcs_server

    client = storage.Client(credentials=AnonymousCredentials(), project="test",
                            client_options={"api_endpoint": endpoint})
    client._connection.API_BASE_URL = endpoint

    print(f"Client API endpoint: {client._connection.API_BASE_URL}")

    bucket_name = "test-bucket"
    bucket = client.create_bucket(bucket_name)
    assert bucket.name == bucket_name

    blob_name = "test-file.txt"
    blob = bucket.blob(blob_name)
    blob.upload_from_string("Hello, Testcontainers!")

    # 디버깅: 원래 URL 확인
    print(f"Original Blob download URL: {blob._get_download_url(client=client)}")

    # 커스텀 함수로 다운로드
    downloaded_content = download_blob_text(blob, endpoint)
    assert downloaded_content == "Hello, Testcontainers!"


if __name__ == "__main__":
    pytest.main(["-v", "-s"])
