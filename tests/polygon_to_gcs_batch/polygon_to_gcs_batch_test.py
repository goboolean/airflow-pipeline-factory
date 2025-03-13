import os
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
import pytest
import docker
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수에서 값 가져오기
YEAR = os.getenv("YEAR", "2025")
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
    build_path = os.path.abspath("../images/polygon_to_gcs_batch")
    print(f"Building Docker image from {build_path}...")
    build_result = os.system(f"docker buildx build --platform linux/arm64,linux/amd64 -t {image_name} {build_path}")
    if build_result != 0:
        raise Exception(f"Docker 이미지 빌드 실패: {build_result}")

    print("Starting container...")
    container = DockerContainer(image_name) \
        .with_env("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID) \
        .with_env("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY) \
        .with_env("GOOGLE_CREDENTIALS", GCS_CREDENTIALS) \
        .with_env("YEAR", YEAR)
    print("Set GOOGLE_CREDENTIALS:", container.env["GOOGLE_CREDENTIALS"][:50] + "...")
    print(f"Set YEAR: {YEAR}")

    container.start()

    try:
        wait_for_logs(container, "Script completed", timeout=600)
    except Exception as e:
        print(f"로그 대기 실패: {e}")
        print(container.get_logs())
        raise

    yield container
    container.stop()


def test_batch_processing(polygon_fetcher_container):
    logs = polygon_fetcher_container.get_logs()
    log_str = logs[0].decode('utf-8') if logs[0] else ""
    print("Container logs:", log_str)

    s3_prefix = f"Processing s3://flatfiles/us_stocks_sip/minute_aggs_v1/{YEAR}"
    assert s3_prefix in log_str, f"S3 prefix '{s3_prefix}'가 로그에 없습니다. S3 처리 실패."

    success_line = "Successful months ("
    assert success_line in log_str, "성공한 월에 대한 결과가 로그에 없습니다."
    failed_line = "Failed months ("
    assert failed_line in log_str, "실패한 월에 대한 결과가 로그에 없습니다."

    gcs_base_path = f"gs://goboolean-452007-raw/stock/usa/{YEAR}/"
    result = os.system(f"gsutil ls {gcs_base_path}")
    print(f"gsutil ls {gcs_base_path} exit code: {result}")
    if result == 0:
        print("GCS bucket contents:")
        os.system(f"gsutil ls {gcs_base_path}")
    else:
        print("GCS ls failed. Checking local gsutil output:")
        os.system(f"gsutil ls {gcs_base_path} 2>&1")
    assert result == 0, f"GCS 경로 '{gcs_base_path}'에 데이터가 업로드되지 않았습니다."


def test_container_exit_code(polygon_fetcher_container):
    exit_code = polygon_fetcher_container.get_wrapped_container().attrs["State"]["ExitCode"]
    print(f"Container exit code: {exit_code}")
    assert exit_code == 0, f"컨테이너가 비정상 종료되었습니다. Exit code: {exit_code}"


def test_at_least_one_month_success(polygon_fetcher_container):
    logs = polygon_fetcher_container.get_logs()
    log_str = logs[0].decode('utf-8') if logs[0] else ""
    success_count = int(
        log_str.split("Successful months (")[1].split(")")[0]) if "Successful months (" in log_str else 0
    print(f"Number of successful months: {success_count}")
    assert success_count > 0, "최소 하나의 월도 성공적으로 처리되지 않았습니다."


if __name__ == "__main__":
    pytest.main(["-v"])
