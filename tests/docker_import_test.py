import docker

def test_docker_import():
    client = docker.DockerClient()  # 명시적 URL
    # 또는 client = docker.from_env()  # 환경 변수 기반
    version = client.version()["ApiVersion"]
    print(f"Docker API version: {version}")
    assert version is not None

if __name__ == "__main__":
    test_docker_import()