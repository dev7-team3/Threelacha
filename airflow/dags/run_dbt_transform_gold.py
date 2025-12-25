"""
Silver → Gold 데이터 마트 변환 DAG (dbt)
dbt 모델을 실행하여 Gold 레이어 데이터 마트 생성
"""

import logging

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
import pendulum

logger = logging.getLogger("airflow.task")


@dag(
    dag_id="run_dbt_transform_gold",
    start_date=pendulum.datetime(2025, 12, 11, tz="UTC"),
    schedule="0 3 * * *",  # 매일 새벽 3시 (Silver 변환 후)
    catchup=False,
    default_args={
        "owner": "jungeun_park",
    },
    tags=["dbt", "gold", "transform"],
    description="Silver → Gold 데이터 마트 변환 (dbt)",
)
def dbt_transform_gold():
    """dbt를 사용한 Gold 데이터 마트 생성"""
    # 1. Silver 파티션 동기화 (완전 동기화)
    sync_silver = SQLExecuteQueryOperator(
        task_id="sync_silver_partitions",
        conn_id="trino_conn",
        sql="""
        CALL hive.system.sync_partition_metadata(
            schema_name => 'silver',
            table_name => 'api1',
            mode => 'FULL'
        )
        """,
    )

    # 2. dbt 모델 실행 (dbt 컨테이너에서 실행)
    dbt_run = BashOperator(
        task_id="dbt_run", bash_command='docker exec dbt bash -c "cd /usr/app/Threelacha && dbt run"', do_xcom_push=True
    )

    @task
    def log_dbt_results(**context):
        """Dbt 실행 결과 로깅"""
        ti = context["ti"]
        dbt_output = ti.xcom_pull(task_ids="dbt_run")

        if dbt_output:
            logger.info("=" * 80)
            logger.info("dbt run 실행 결과:")
            logger.info("=" * 80)
            logger.info(dbt_output)
            logger.info("=" * 80)
        else:
            logger.warning("⚠️ dbt run 출력을 가져올 수 없습니다.")

    # 3. 결과 로깅
    log_results = log_dbt_results()

    # 실행 순서
    sync_silver >> dbt_run >> log_results


dbt_transform_gold()
