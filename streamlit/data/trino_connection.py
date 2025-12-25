"""Trino 데이터베이스 연결 모듈"""
import os
import pandas as pd
from trino.dbapi import connect, Connection
import streamlit as st


@st.cache_resource
def get_trino_connection():
    """Trino 연결을 생성하고 캐시합니다.
    
    환경 변수:
        TRINO_HOST: Trino 호스트 (기본값: localhost)
        TRINO_PORT: Trino 포트 (기본값: 8082)
    
    Returns:
        trino.dbapi.Connection: Trino 연결 객체
    """
    # 환경에 따라 호스트 설정 (docker-compose 내부 vs 외부)
    host = os.getenv("TRINO_HOST", "localhost")
    port = int(os.getenv("TRINO_PORT", "8082"))

    conn = connect(
        host=host,
        port=port,
        user="trino",
        catalog="hive",
        schema="gold",
    )
    return conn


def execute_query(query: str, conn: Connection) -> pd.DataFrame:
    """Trino 쿼리를 실행하고 DataFrame으로 반환합니다.
    
    Args:
        query: 실행할 SQL 쿼리 문자열
        
    Returns:
        pd.DataFrame: 쿼리 결과를 담은 DataFrame
        
    Raises:
        Exception: 쿼리 실행 중 오류 발생 시
    """
    cursor = conn.cursor()
    
    try:
        cursor.execute(query)
        
        # 컬럼명 가져오기
        columns = [desc[0] for desc in cursor.description]
        
        # 데이터 가져오기
        rows = cursor.fetchall()
        
        # DataFrame 생성
        df = pd.DataFrame(rows, columns=columns)
        
        return df
    finally:
        cursor.close()

