import os
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
import pytest
import docker
import re
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 테스트용 날짜 지정
YEAR = "2025"
MONTH = "03"
DAY = "14"

# 환경 변수에서 값 가져오기
GCS_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
ENVIRONMENT = os.getenv("ENVIRONMENT")


@pytest.fixture(scope="module")
def stock_splitter_container():
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

    image_name = "split_ticker:test"
    build_path = os.path.abspath("../../images/daily-pipeline/split_ticker")
    print(f"Building Docker image from {build_path}...")
    build_result = os.system(f"docker buildx build --platform linux/arm64,linux/amd64 -t {image_name} {build_path}")

    if build_result != 0:
        raise Exception(f"Docker 이미지 빌드 실패: {build_result}")

    print("Starting container...")
    container = DockerContainer(image_name) \
        .with_env("GOOGLE_CREDENTIALS", GCS_CREDENTIALS) \
        .with_env("ENVIRONMENT", ENVIRONMENT) \
        .with_command([YEAR, MONTH, DAY])
    print("Set GOOGLE_CREDENTIALS:", container.env["GOOGLE_CREDENTIALS"][:50] + "...")
    print(f'ENVIRONMENT: {container.env["ENVIRONMENT"]}')
    print(f"Command: python /app/split_ticker.py {YEAR} {MONTH} {DAY}")

    container.start()

    try:
        wait_for_logs(container, "Uploaded: gs://", timeout=600)  # 업로드 완료 로그 대기
    except Exception as e:
        print(f"로그 대기 실패: {e}")
        print(container.get_logs())
        raise

    yield container
    container.stop()


def test_split_stock_data(stock_splitter_container):
    logs = stock_splitter_container.get_logs()
    log_str = logs[0].decode('utf-8') if logs and logs[0] else ""
    print("Container logs:", logs)
    print("Decoded logs:", log_str)

    # GCS 소스 경로 처리 확인
    expected_log = f"Processing data for {YEAR}-{MONTH}-{DAY}"
    assert expected_log in log_str, "GCS 소스 경로 처리 실패"

    # 특정 날짜 파일 업로드 성공 확인 (로그 기반)
    day_file_pattern = rf"Uploaded: gs://goboolean-452007-resampled/stock/usa/.*/1m/2025/03/.*{YEAR}-{MONTH}-{DAY}\.csv\.gz"
    assert any(re.search(day_file_pattern, line) for line in
               log_str.splitlines()), f"{YEAR}-{MONTH}-{DAY}.csv.gz에 해당하는 파일 업로드 실패"

    # GCS에서 파일 존재 여부 확인
    gcs_path = f"gs://goboolean-452007-resampled/stock/usa/*/1m/{YEAR}/{MONTH}/"  # 1m 추가
    result = os.system(f"gsutil ls {gcs_path} | grep {YEAR}-{MONTH}-{DAY}.csv.gz")
    print(f"gsutil ls exit code: {result}")
    if result == 0:
        print("GCS bucket contents for the day:")
        os.system(f"gsutil ls {gcs_path} | grep {YEAR}-{MONTH}-{DAY}.csv.gz")
    else:
        print("GCS ls failed or no matching files. Checking full GCS output:")
        os.system(f"gsutil ls {gcs_path} 2>&1")
    assert result == 0, f"GCS에 {YEAR}-{MONTH}-{DAY}.csv.gz 파일 업로드 실패"


if __name__ == "__main__":
    pytest.main(["-v"])
