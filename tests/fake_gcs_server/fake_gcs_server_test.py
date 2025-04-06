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
    # Arrange: 컨테이너 설정
    gcs_container = DockerContainer("fsouza/fake-gcs-server:latest").with_exposed_ports(4443).with_command(
        ["-scheme", "http"]  # -port 생략, 기본값 4443 사용
    )

    # Act: 컨테이너 시작 및 엔드포인트 설정
    gcs_container.start()
    wait_for_logs(gcs_container, "server started at", timeout=30)

    host = "localhost"
    port = gcs_container.get_exposed_port(4443)
    endpoint = f"http://{host}:{port}"
    os.environ["STORAGE_EMULATOR_HOST"] = endpoint

    # 디버깅 출력
    print(f"STORAGE_EMULATOR_HOST set to: {endpoint}")
    print(f"Container logs: {gcs_container.get_logs()}")

    # Act: 서버 연결 테스트
    try:
        response = requests.get(f"{endpoint}/storage/v1/b", timeout=5)
        print(f"Manual test response: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Manual test failed: {e}")

    # Yield: 테스트에 필요한 값 제공
    yield gcs_container, endpoint

    # Cleanup
    gcs_container.stop()


def download_blob_text(blob, endpoint):
    """GCS Blob을 다운로드하는 커스텀 함수"""
    url = f"{endpoint}/download/storage/v1/b/{blob.bucket.name}/o/{blob.name}?alt=media"
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.text


def test_fake_gcs_server(fake_gcs_server):
    # Arrange: 테스트 준비
    gcs_container, endpoint = fake_gcs_server
    client = storage.Client(
        credentials=AnonymousCredentials(),
        project="test",
        client_options={"api_endpoint": endpoint}
    )
    client._connection.API_BASE_URL = endpoint
    bucket_name = "test-bucket"
    blob_name = "test-file.txt"
    expected_content = "Hello, Testcontainers!"

    print(f"Client API endpoint: {client._connection.API_BASE_URL}")

    # Act: 버킷 생성, 파일 업로드, 다운로드
    bucket = client.create_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(expected_content)
    downloaded_content = download_blob_text(blob, endpoint)

    print(f"Original Blob download URL: {blob._get_download_url(client=client)}")

    # Assert: 결과 검증
    assert bucket.name == bucket_name, f"Expected bucket name {bucket_name}, but got {bucket.name}"
    assert downloaded_content == expected_content, f"Expected content {expected_content}, but got {downloaded_content}"


if __name__ == "__main__":
    pytest.main(["-v", "-s"])
