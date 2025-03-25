import os
import subprocess
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
import pytest
import docker
from dotenv import load_dotenv
import logging
import sys

# 로그 설정: stdout에 강제로 연결
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)  # 명시적으로 sys.stdout 사용
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.handlers = [handler]  # 기존 핸들러 제거 후 새 핸들러 추가

load_dotenv()

YEAR = "2025"
MONTH = "03"
DAY = "14"

GCS_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
ENVIRONMENT = os.getenv("ENVIRONMENT")

@pytest.fixture(scope="module")
def stock_splitter_container():
    docker_host = os.environ.get("DOCKER_HOST", "unix:///var/run/docker.sock")
    logger.info(f"DOCKER_HOST: {docker_host}")

    try:
        client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        logger.info(f"Docker API version: {client.version()['ApiVersion']}")
    except docker.errors.DockerException as e:
        logger.error(f"Docker 연결 실패: {e}")
        raise

    if not GCS_CREDENTIALS:
        raise ValueError("GOOGLE_CREDENTIALS 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.")

    image_name = "split_ticker:test"
    build_path = os.path.abspath("../../images/daily-pipeline/split_ticker")
    logger.info(f"Building Docker image from {build_path}...")
    build_result = os.system(f"docker buildx build --platform linux/arm64,linux/amd64 -t {image_name} {build_path}")

    if build_result != 0:
        raise Exception(f"Docker 이미지 빌드 실패: {build_result}")

    logger.info("Starting container...")
    container = DockerContainer(image_name) \
        .with_env("GOOGLE_CREDENTIALS", GCS_CREDENTIALS) \
        .with_env("ENVIRONMENT", ENVIRONMENT) \
        .with_command([YEAR, MONTH, DAY])

    logger.info(f"Set GOOGLE_CREDENTIALS: {container.env['GOOGLE_CREDENTIALS'][:50]}...")
    logger.info(f"ENVIRONMENT: {container.env['ENVIRONMENT']}")
    logger.info(f"Command: {container._command}")

    container.start()

    try:
        wait_for_logs(container, "Execution completed|Ticker mismatch|Failed to upload", timeout=600)
    except Exception as e:
        logger.error(f"로그 대기 실패: {e}")
        logs = container.get_logs()
        logger.error(f"Container logs: {logs}")
        logger.error(f"Container exit code: {container.get_wrapped_container().attrs['State']['ExitCode']}")
        raise

    yield container
    container.stop()

def test_split_stock_data(stock_splitter_container):
    logs = stock_splitter_container.get_logs()
    log_str = (logs[0].decode('utf-8') + logs[1].decode('utf-8')).strip() if logs else ""
    logger.info(f"Container logs (excerpt): {log_str[:2000]}...")  # logger로 출력

    # 원본 파일에서 마지막 티커 가져오기
    source_path = f"gs://goboolean-452007-raw/stock/usa/{YEAR}/{MONTH}/{YEAR}-{MONTH}-{DAY}.csv.gz"
    try:
        last_line_result = subprocess.run(
            f"gsutil cat {source_path} | gunzip | tail -n 1",
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        last_line = last_line_result.stdout.strip()
        last_ticker = last_line.split(',')[0]  # ticker가 첫 번째 열이라고 가정
        logger.info(f"원본 파일의 마지막 티커: {last_ticker}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"원본 파일에서 마지막 티커를 가져오지 못했습니다: {e.stderr}")

    # GCS에서 마지막 티커 업로드 확인
    last_ticker_path = f"gs://goboolean-452007-resampled/stock/usa/{last_ticker}/1m/{YEAR}/{MONTH}/{last_ticker}_{YEAR}-{MONTH}-{DAY}.csv.gz"
    gcs_check = subprocess.run(
        f"gsutil ls {last_ticker_path}",
        shell=True,
        capture_output=True,
        text=True
    )
    last_ticker_uploaded = gcs_check.returncode == 0
    logger.info(f"마지막 티커 '{last_ticker}'의 업로드 경로: {last_ticker_path}")
    logger.info(f"마지막 티커 '{last_ticker}' GCS 업로드 여부: {'성공' if last_ticker_uploaded else '실패'}")

    # 검증: 마지막 티커 업로드 여부
    assert last_ticker_uploaded, f"마지막 티커 '{last_ticker}'가 GCS에 업로드되지 않았습니다. 경로: {last_ticker_path}"

    # 실행 완료 확인 (최소 로그 기반 검증)
    execution_completed = "Execution completed" in log_str
    assert execution_completed, "스크립트가 끝까지 실행되지 않았습니다."

    logger.info("테스트 통과! 모든 검증 완료.")

if __name__ == "__main__":
    pytest.main(["-v", "-s"])
