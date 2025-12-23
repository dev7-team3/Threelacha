"""
Silver API1 테이블을 Hive Metastore에 등록하는 DAG

필수 사전 작업:
- Airflow Connection: trino_conn (Trino 연결)
- MinIO에 silver/api-1/ 데이터 존재

실행 방법:
1. 최초 실행: 테이블 생성
2. 스키마 변경 시: 테이블 재생성 (DROP → CREATE)
3. 데이터 추가 시: 파티션 동기화만 실행
"""

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag
import pendulum


@dag(
    dag_id="schema_register_silver_api1",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # 수동 실행 (또는 Silver Transform 완료 후 트리거)
    catchup=False,
    default_args={
        "owner": "jungeun_park",
    },
    tags=["KAMIS", "api-1", "silver", "metastore", "schema"],
    description="Silver API1 테이블을 Hive Metastore에 등록 및 파티션 동기화",
)
def register_silver_api1_metastore():
    """
    Silver API1 테이블 Metastore 등록

    처리 흐름:
    1. Silver 스키마 생성
    2. 기존 테이블 삭제 (존재 시)
    3. 새 테이블 생성 (컬럼 코멘트 포함)
    4. 파티션 동기화
    """
    # 1. Silver 스키마 생성
    create_silver_schema = SQLExecuteQueryOperator(
        task_id="create_silver_schema",
        conn_id="trino_conn",
        sql="""
        CREATE SCHEMA IF NOT EXISTS hive.silver
        WITH (location = 's3a://team3-batch/silver/')
        """,
    )

    # 2. 기존 테이블 삭제 (재생성을 위해)
    drop_existing_table = SQLExecuteQueryOperator(
        task_id="drop_existing_table",
        conn_id="trino_conn",
        sql="""
        DROP TABLE IF EXISTS hive.silver.api1
        """,
    )

    # 3. Silver API1 테이블 생성 (컬럼 코멘트 포함)
    create_api1_table = SQLExecuteQueryOperator(
        task_id="create_api1_table",
        conn_id="trino_conn",
        sql="""
        CREATE TABLE hive.silver.api1 (
            -- 날짜/시간 정보
            res_dt DATE COMMENT '응답일자',
            week_of_year INTEGER COMMENT '연도기준주차',
            weekday_num INTEGER COMMENT '요일번호',
            weekday_nm VARCHAR COMMENT '요일명',
            weekend_yn BOOLEAN COMMENT '주말여부',
            -- 상품 분류
            product_cls_cd VARCHAR COMMENT '도소매구분코드',
            product_cls_nm VARCHAR COMMENT '도소매구분명',
            category_cd VARCHAR COMMENT '부류코드',
            category_nm VARCHAR COMMENT '부류명',
            -- 지역
            country_cd VARCHAR COMMENT '지역코드',
            country_nm VARCHAR COMMENT '지역명',
            -- 품목 정보
            item_nm VARCHAR COMMENT '품목명',
            item_cd VARCHAR COMMENT '품목코드',
            kind_nm VARCHAR COMMENT '품종명',
            kind_cd VARCHAR COMMENT '품종코드',
            rank_nm VARCHAR COMMENT '등급명',
            rank_cd VARCHAR COMMENT '등급코드',
            unit VARCHAR COMMENT '단위',
            -- 당일 정보
            base_dt VARCHAR COMMENT '조회일자',
            base_pr DOUBLE COMMENT '조회일자가격',
            -- 1일전
            prev_1d_dt VARCHAR COMMENT '1일전일자',
            prev_1d_pr DOUBLE COMMENT '1일전가격',
            -- 1주일전
            prev_1w_dt VARCHAR COMMENT '1주일전일자',
            prev_1w_pr DOUBLE COMMENT '1주일전가격',
            -- 2주일전
            prev_2w_dt VARCHAR COMMENT '2주일전일자',
            prev_2w_pr DOUBLE COMMENT '2주일전가격',
            -- 1개월전
            prev_1m_dt VARCHAR COMMENT '1개월전일자',
            prev_1m_pr DOUBLE COMMENT '1개월전가격',
            -- 1년전
            prev_1y_dt VARCHAR COMMENT '1년전일자',
            prev_1y_pr DOUBLE COMMENT '1년전가격',
            -- 평년
            avg_tp VARCHAR COMMENT '평년타입',
            avg_pr DOUBLE COMMENT '평년가격',
            -- 파티션 컬럼(path 기반 생성)
            year INTEGER COMMENT '연도',
            month INTEGER COMMENT '월'
        )
        COMMENT 'KAMIS API1 Silver 레이어'
        WITH (
            format = 'PARQUET',
            external_location = 's3a://team3-batch/silver/api-1/',
            partitioned_by = ARRAY['year', 'month']
        )
        """,
    )

    # 4. 파티션 동기화 (S3의 실제 파티션을 Metastore에 자동 반영)
    sync_partitions = SQLExecuteQueryOperator(
        task_id="sync_partitions",
        conn_id="trino_conn",
        sql="""
        CALL hive.system.sync_partition_metadata(
            schema_name => 'silver',
            table_name => 'api1',
            mode => 'ADD'
        )
        """,
    )

    # 5. 테이블 정보 확인 (검증)
    verify_table = SQLExecuteQueryOperator(
        task_id="verify_table",
        conn_id="trino_conn",
        sql="""
        -- 테이블 구조 확인
        DESCRIBE hive.silver.api1;
        -- 파티션 목록 확인
        SELECT * FROM hive.silver."api1$partitions" ORDER BY year, month;
        """,
    )

    # 6. Gold 스키마 생성 (dbt 사용을 위해)
    create_gold_schema = SQLExecuteQueryOperator(
        task_id="create_gold_schema",
        conn_id="trino_conn",
        sql="""
        CREATE SCHEMA IF NOT EXISTS hive.gold
        WITH (location = 's3a://team3-batch/gold/')
        """,
    )

    # Task 의존성
    (
        create_silver_schema
        >> drop_existing_table
        >> create_api1_table
        >> sync_partitions
        >> verify_table
        >> create_gold_schema
    )


register_silver_api1_metastore()
