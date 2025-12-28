"""Athena 데이터베이스 연결 모듈"""

import os
import time
from typing import Optional

import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import ClientError, NoCredentialsError


@st.cache_resource
def get_athena_client():
    """Athena 클라이언트를 생성하고 캐시합니다.

    환경 변수:
        AWS_ACCESS_KEY_ID: AWS Access Key ID
        AWS_SECRET_ACCESS_KEY: AWS Secret Access Key
        AWS_REGION: AWS 리전 (기본값: ap-northeast-2)
        AWS_ENDPOINT_URL: AWS 엔드포인트 URL (로컬 MinIO 사용 시)

    Returns:
        boto3.client: Athena 클라이언트 객체
    """
    region = os.getenv("AWS_REGION", "ap-northeast-2")
    endpoint_url = os.getenv("AWS_ENDPOINT_URL")

    config = {
        "region_name": region,
    }

    # 로컬 MinIO 사용 시 endpoint_url 추가
    if endpoint_url:
        config["endpoint_url"] = endpoint_url

    # 자격 증명이 환경 변수에 있는 경우 명시적으로 전달
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if access_key and secret_key:
        config["aws_access_key_id"] = access_key
        config["aws_secret_access_key"] = secret_key

    try:
        client = boto3.client("athena", **config)
    except (NoCredentialsError, ClientError) as e:
        error_msg = f"Athena 연결 실패: {e!s}"
        st.error(error_msg)
        raise
    else:
        return client


def get_athena_config() -> tuple[str, Optional[str]]:
    """Athena 설정을 가져옵니다.

    환경 변수:
        ATHENA_DATABASE: Athena 데이터베이스 (기본값: team3_silver)
        ATHENA_WORKGROUP: Athena WorkGroup (기본값: primary)
        ATHENA_OUTPUT_LOCATION: S3 출력 위치 (기본값: s3://team3-batch/gold/athena-results/)

    Returns:
        tuple: (database, workgroup) 튜플
    """
    database = os.getenv("ATHENA_DATABASE", "team3_gold")
    workgroup = os.getenv("ATHENA_WORKGROUP", "team3-wg")
    return database, workgroup


def execute_athena_query(
    query: str,
    database: Optional[str] = None,
    workgroup: Optional[str] = None,
    output_location: Optional[str] = None,
) -> pd.DataFrame:
    """Athena 쿼리를 실행하고 DataFrame으로 반환합니다.

    Args:
        query: 실행할 SQL 쿼리 문자열
        database: Athena 데이터베이스 (기본값: 환경 변수 또는 team3_silver)
        workgroup: Athena WorkGroup (기본값: 환경 변수 또는 primary)
        output_location: S3 출력 위치 (기본값: 환경 변수 또는 s3://team3-batch/gold/athena-results/)

    Returns:
        pd.DataFrame: 쿼리 결과를 담은 DataFrame

    Raises:
        Exception: 쿼리 실행 중 오류 발생 시
    """
    # 기본값 설정
    if database is None:
        database, _ = get_athena_config()

    if workgroup is None:
        _, workgroup = get_athena_config()

    if output_location is None:
        output_location = os.getenv(
            "ATHENA_OUTPUT_LOCATION",
            "s3://team3-batch/gold/athena-results/",
        )

    client = get_athena_client()

    try:
        # 쿼리 실행 시작
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            WorkGroup=workgroup,
            ResultConfiguration={"OutputLocation": output_location},
        )

        query_execution_id = response["QueryExecutionId"]

        # 쿼리 완료 대기
        while True:
            response = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

            time.sleep(1)

        if status == "FAILED":
            reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            raise Exception(f"Athena 쿼리 실패: {reason}") from None

        if status == "CANCELLED":
            raise Exception("Athena 쿼리가 취소되었습니다.") from None

        # 결과 가져오기
        results = client.get_query_results(QueryExecutionId=query_execution_id)

        # 첫 번째 행은 컬럼명
        columns = [col["Name"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]

        # 데이터 행 추출 (첫 번째 행 제외)
        rows = []
        for row in results["ResultSet"]["Rows"][1:]:
            row_data = []
            for col in row["Data"]:
                value = col.get("VarCharValue", "")
                # 숫자 타입 변환 시도
                try:
                    if "." in value:
                        row_data.append(float(value))
                    else:
                        row_data.append(int(value))
                except (ValueError, TypeError):
                    row_data.append(value)
            rows.append(row_data)

        # DataFrame 생성
        df = pd.DataFrame(rows, columns=columns)

        # 다음 페이지가 있으면 계속 가져오기
        next_token = results.get("NextToken")
        while next_token:
            results = client.get_query_results(
                QueryExecutionId=query_execution_id,
                NextToken=next_token,
            )

            for row in results["ResultSet"]["Rows"]:
                row_data = []
                for col in row["Data"]:
                    value = col.get("VarCharValue", "")
                    try:
                        if "." in value:
                            row_data.append(float(value))
                        else:
                            row_data.append(int(value))
                    except (ValueError, TypeError):
                        row_data.append(value)
                rows.append(row_data)

            next_token = results.get("NextToken")

        # 최종 DataFrame 재생성 (모든 데이터 수집 후)
        df = pd.DataFrame(rows, columns=columns)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_message = e.response.get("Error", {}).get("Message", f"{e!s}")
        raise Exception(f"Athena 클라이언트 오류 ({error_code}): {error_message}") from e
    except Exception as e:
        error_msg = f"Athena 쿼리 실행 중 오류: {e!s}"
        raise Exception(error_msg) from e
    else:
        return df


def get_athena_connection():
    """Athena 연결 정보를 반환합니다 (호환성을 위한 함수).

    Returns:
        dict: Athena 연결 정보 딕셔너리
    """
    database, workgroup = get_athena_config()
    return {
        "database": database,
        "workgroup": workgroup,
        "output_location": os.getenv(
            "ATHENA_OUTPUT_LOCATION",
            "s3://team3-batch/gold/athena-results/",
        ),
    }
