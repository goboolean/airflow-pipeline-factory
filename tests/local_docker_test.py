import docker

# Docker 클라이언트 초기화 (소켓 경로 명시)
client = docker.DockerClient(base_url="unix://var/run/docker.sock")

# 컨테이너 실행
try:
    container = client.containers.run(
        "nginx:latest",
        detach=True,
        ports={'80/tcp': 8080},
        name="my-nginx"
    )
    print(f"컨테이너 '{container.name}'가 성공적으로 실행되었습니다!")
    print(f"컨테이너 ID: {container.id}")

except docker.errors.NotFound:
    print("지정한 이미지를 찾을 수 없습니다. 먼저 이미지를 pull하세요.")
except docker.errors.APIError as e:
    print(f"도커 API 오류: {e}")

# 실행 중인 컨테이너 목록 확인
for container in client.containers.list():
    print(f"실행 중인 컨테이너: {container.name}")
