# airflow-pipeline-factory

## Airflow KubernetesPodOperator 에 사용될 컨테이너 이미지의 파이썬 모듈 Repository 입니다.

## 1. 패키지 및 모듈 명명 (PEP8 권장)
- 모듈 파일명: 소문자와 밑줄(_)을 사용합니다.
  - 예: data_loader.py, task_executor.py
- 패키지 폴더명: 소문자로 구성하며, 특별한 경우가 아니라면 밑줄 사용은 피합니다.
  - 예: app, config, utils

## 2. 주요 파일 및 폴더 구조 예시

```text
my_project/
├── Dockerfile
├── requirements.txt
├── entrypoint.sh             # 컨테이너 초기화 스크립트
└── app/                      # 메인 애플리케이션 코드
    ├── __init__.py
    ├── main.py             # 애플리케이션의 엔트리 포인트
    ├── config/             # 설정 관련 모듈
    │   ├── __init__.py
    │   └── settings.py     # 환경별 설정, 상수, 비밀 변수 등
    ├── tasks/              # 개별 작업(task) 관련 모듈
    │   ├── __init__.py
    │   ├── download_data.py    # 데이터 다운로드 작업
    │   └── process_data.py     # 데이터 전처리/가공 작업
    └── utils/              # 공통 유틸리티 함수 모듈
        ├── __init__.py
        └── file_helper.py      # 파일 I/O, 로깅, 오류 처리 등
```

## 3.세부 명명 규칙
- 엔트리 포인트:
  - main.py: 컨테이너 실행 시 진입점 역할을 하는 파일은 main.py로 지정합니다.
  또는 run.py와 같이 명확한 이름 사용 가능.
- 설정 파일:
  - settings.py 또는 config.py: 환경 변수, 기본 설정, 비밀 정보 로딩 기능을 포함.
  복잡한 설정이 필요한 경우 config 폴더를 생성하여 역할별로 분리.
- 작업(Task) 관련 파일:
  - 각 Airflow Task에 대응하는 작업은 해당 역할을 명확히 표현하는 파일명을 사용합니다.
  예: 데이터 다운로드는 download_data.py, 데이터 전처리는 process_data.py 등.
- 유틸리티 파일:
  - 공통적으로 재사용 가능한 함수는 utils 폴더 아래에 모아둡니다.
  - 파일명은 기능을 쉽게 파악할 수 있도록 작성합니다.
    - 예: file_helper.py, logger.py
- 스크립트 파일:
  - 컨테이너 실행 전 초기화가 필요한 경우, entrypoint.sh 등 실행 스크립트 파일을 최상위에 둡니다.
  - 스크립트 파일은 실행 권한을 부여하고, 명령어와 인자를 명확히 기술합니다.
