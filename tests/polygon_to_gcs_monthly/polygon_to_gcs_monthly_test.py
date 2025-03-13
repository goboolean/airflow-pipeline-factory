import os
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
import pytest
import docker
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

YEAR = "2024"
MONTH = "02"

# 환경 변수에서 값 가져오기 (기본값 설정 가능)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
GCS_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")


@pytest.fixture(scope="module")
def polygon_fetcher_container():
    docker_host = os.environ.get("DOCKER_HOST", "unix:///var/run/docker.sock")
    print(f"DOCKER_HOST: {docker_host}")

    try:
        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        print("Docker API version:", client.version()["ApiVersion"])
    except docker.errors.DockerException as e:
        print(f"Docker 연결 실패: {e}")
        raise

    if not GCS_CREDENTIALS:
        raise ValueError("GOOGLE_CREDENTIALS 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.")

    image_name = "polygon_fetcher:test"
    build_path = os.path.abspath("../images/polygon_to_gcs_monthly")
    print(f"Building Docker image from {build_path}...")
    build_result = os.system(f"docker buildx build --platform linux/arm64,linux/amd64 -t {image_name} {build_path}")

    if build_result != 0:
        raise Exception(f"Docker 이미지 빌드 실패: {build_result}")

    print("Starting container...")
    container = DockerContainer(image_name) \
        .with_env("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID) \
        .with_env("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY) \
        .with_env("GOOGLE_CREDENTIALS", GCS_CREDENTIALS) \
        .with_env("YEAR", YEAR) \
        .with_env("MONTH", MONTH)
    print("Set GOOGLE_CREDENTIALS:", container.env["GOOGLE_CREDENTIALS"][:50] + "...")  # 일부만 출력
    print(f"Set YEAR: {YEAR}, MONTH: {MONTH}")

    container.start()

    try:
        wait_for_logs(container, "Script completed", timeout=300)
    except Exception as e:
        print(f"로그 대기 실패: {e}")
        print(container.get_logs())
        raise

    yield container
    container.stop()


def test_fetch_sample(polygon_fetcher_container):
    logs = polygon_fetcher_container.get_logs()
    print("Container logs:", logs)
    log_str = logs[0].decode('utf-8') if logs and logs[0] else ""
    print("Decoded logs:", log_str)
    s3_prefix = f"Processing s3://flatfiles/us_stocks_sip/minute_aggs_v1/{YEAR}/{MONTH}"
    assert s3_prefix in log_str, "S3 디렉토리 처리 실패"

    gcs_path = f"gs://goboolean-452007-raw/stock/usa/{YEAR}/{MONTH}/"
    result = os.system(f"gsutil ls {gcs_path}")
    print(f"gsutil ls exit code: {result}")
    if result == 0:
        print("GCS bucket contents:")
        os.system(f"gsutil ls {gcs_path}")
    else:
        print("GCS ls failed. Checking local gsutil output:")
        os.system(f"gsutil ls {gcs_path} 2>&1")
    assert result == 0, "GCS에 파일 업로드 실패"


if __name__ == "__main__":
    pytest.main(["-v"])
