import os
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수 설정
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")

# DAG 정의
with DAG(
    dag_id="polygon_s3_to_gcs",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@yearly",
    catchup=False,
) as dag:

    # 시작과 종료 더미 태스크
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # 10년치 데이터 (2015~2024)
    years = range(2015, 2025)

    with TaskGroup("polygon_to_gcs") as polygon_to_gcs:
        for year in years:
            task_id = f"transfer_{year}"
            year_str = str(year)

            # KubernetesPodOperator로 태스크 정의
            task = KubernetesPodOperator(
                task_id=task_id,
                name=f"polygon-transfer-{year}",
                namespace="default",  # TODO: K8s 네임스페이스 설정 필요
                image="polygon_fetcher:test",  # 커스텀 이미지
                cmds=["bash"],
                arguments=["fetch_test.sh"],
                env_vars={
                    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
                    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
                    "GOOGLE_CREDENTIALS": GOOGLE_CREDENTIALS,
                    "YEAR": year_str,
                },
                get_logs=True,
                is_delete_pod=True,
            )

    # DAG 흐름 정의
    start >> polygon_to_gcs >> end
    # TODO: polygon_to_gcs >> split_ticker_to_resample_bucket >> make_serveral_resampled_data >> insert_to_influxDB
