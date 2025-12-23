from datetime import timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum

from airflow import DAG

# 기본 설정
default_args = {
    "owner": "jungeun park",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="test_trino_conn",
    default_args=default_args,
    description="Trino 연결 테스트를 위한 간단한 DAG",
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    tags=["trino", "test", "connection"],
) as dag:
    # 1. 간단한 연결 확인 (Catalog 목록 조회)
    check_connectivity = SQLExecuteQueryOperator(
        task_id="check_trino_connectivity", conn_id="trino_conn", sql="SHOW CATALOGS", do_xcom_push=True
    )

    # 2. 실제 데이터 조회 테스트
    check_system_nodes = SQLExecuteQueryOperator(
        task_id="check_trino_nodes",
        conn_id="trino_conn",
        sql="SELECT * FROM system.runtime.nodes LIMIT 5",
    )

    check_connectivity >> check_system_nodes
