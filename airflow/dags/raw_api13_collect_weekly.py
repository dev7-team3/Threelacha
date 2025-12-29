from datetime import datetime
import logging
import os

from airflow.providers.standard.operators.python import PythonOperator
from eco_collector import base_url, collect_eco_data, common_params

from airflow import DAG

logger = logging.getLogger(__name__)

# ============================================================
# Default Args
# ============================================================

default_args = {
    "owner": "dahye",
    "depends_on_past": False,
    "retries": 0,
}

# ============================================================
# Task 함수 정의
# ============================================================


def collect_weekly(**context):
    """
    Airflow Task 함수

    주어진 실행 날짜(logical_date)를 기준으로 친환경 가격 데이터를 수집하고 S3 버킷에 업로드합니다.

    logical_date는 Airflow가 DAG 실행 시 전달하는 컨텍스트에서 가져오며,
    이를 YYYY-MM-DD 형식의 문자열로 변환하여 API 요청에 사용합니다.

    Args:
        **context: Airflow DAG 실행 컨텍스트.
            - logical_date (datetime): DAG 실행 기준 날짜.
    """
    logical_date = context["logical_date"].date()
    regday = logical_date

    regday_str = regday.strftime("%Y-%m-%d")
    logger.info(f"collect regday={regday_str}")

    collect_eco_data(
        base_url=base_url,
        regday=regday_str,
        common_params=common_params,
        bucket=os.environ["AIRFLOW_VAR_BUCKET_NAME"],
    )


# ============================================================
# DAG 정의
# ============================================================

with DAG(
    dag_id="raw_api13_collect_weekly",
    start_date=datetime(2025, 1, 6),
    schedule="0 13 * * 1",  # 매주 월요일
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    PythonOperator(
        task_id="collect_eco_weekly",
        python_callable=collect_weekly,
    )
