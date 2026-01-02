"""
Silver API1 테이블을 Hive Metastore에 등록하는 DAG

필수 사전 작업:
- Airflow Connection: trino_conn (Trino 연결)
- MinIO에 silver/api-1/ 데이터 존재
"""

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag
import pendulum


@dag(
    dag_id="make_silver_schema_ap1",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,  # 수동 실행
    catchup=False,
    default_args={
        "owner": "jungeun_park",
    },
    tags=["schema", "silver", "metastore"],
    description="Silver API1 테이블 구조를 Hive Metastore에 등록",
)
def setup_silver_api1_metastore():
    """Silver API1 테이블 Hive Metastore 등록"""
    # 1. Silver 스키마 생성
    create_silver_schema = SQLExecuteQueryOperator(
        task_id="create_silver_schema",
        conn_id="trino_conn",
        sql="""
        CREATE SCHEMA IF NOT EXISTS hive.silver
        WITH (location = 's3a://team3-batch/silver/')
        """,
    )

    # 2. Silver API1 테이블 생성
    create_api1_table = SQLExecuteQueryOperator(
        task_id="create_api1_table",
        conn_id="trino_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS hive.silver.api_1 (
            dt VARCHAR,
            day_of_week INTEGER,
            weekday_name VARCHAR,
            is_weekend BOOLEAN,
            product_cls_code VARCHAR,
            product_cls_name VARCHAR,
            item_category_code VARCHAR,
            item_category_name VARCHAR,
            country_code VARCHAR,
            country_name VARCHAR,
            item_name VARCHAR,
            item_code VARCHAR,
            kind_name VARCHAR,
            kind_code VARCHAR,
            rank VARCHAR,
            rank_code VARCHAR,
            unit VARCHAR,
            day1 VARCHAR,
            day2 VARCHAR,
            day3 VARCHAR,
            day4 VARCHAR,
            day5 VARCHAR,
            day6 VARCHAR,
            day7 VARCHAR,
            dpr1 VARCHAR,
            dpr2 VARCHAR,
            dpr3 VARCHAR,
            dpr4 VARCHAR,
            dpr5 VARCHAR,
            dpr6 VARCHAR,
            dpr7 VARCHAR,
            year INTEGER,
            month INTEGER
        )
        WITH (
            format = 'PARQUET',
            external_location = 's3a://team3-batch/silver/api-1/',
            partitioned_by = ARRAY['year', 'month']
        )
        """,
    )

    # 3. 파티션 동기화 (S3의 실제 파티션을 Metastore에 반영)
    sync_partitions = SQLExecuteQueryOperator(
        task_id="sync_partitions",
        conn_id="trino_conn",
        sql="""
        CALL hive.system.sync_partition_metadata(
            schema_name => 'silver',
            table_name => 'api_1',
            mode => 'ADD'
        )
        """,
    )

    # 4. Gold 스키마 생성 (dbt 사용을 위해)
    create_gold_schema = SQLExecuteQueryOperator(
        task_id="create_gold_schema",
        conn_id="trino_conn",
        sql="""
        CREATE SCHEMA IF NOT EXISTS hive.gold
        WITH (location = 's3a://team3-batch/gold/')
        """,
    )

    # Task 순서
    create_silver_schema >> create_api1_table >> sync_partitions >> create_gold_schema


setup_silver_api1_metastore()
