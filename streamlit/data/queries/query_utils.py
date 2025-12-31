"""쿼리 생성 공통 유틸리티"""
from typing import Optional
from datetime import date


def build_where_clause(
    date_filter: Optional[date] = None,
    category_filter: Optional[str] = None
) -> str:
    """WHERE 절을 구성합니다.
    
    Args:
        date_filter: 날짜 필터 (date 객체)
        category_filter: 카테고리 필터 (문자열, "전체"인 경우 필터링 안함)
    
    Returns:
        str: WHERE 절 SQL 문자열 (조건이 없으면 빈 문자열)
    """
    where_clauses = []
    
    if date_filter:
        where_clauses.append(f"res_dt = DATE '{date_filter}'")
    
    if category_filter and category_filter != "전체":
        where_clauses.append(f"category_nm = '{category_filter}'")
    
    if where_clauses:
        return f"WHERE {' AND '.join(where_clauses)}"
    
    return ""

def build_where_country_clause(country_filter: Optional[str] = None) -> str:
    clauses = ["1=1"]  # 기본 조건
    if country_filter:
        clauses.append(f"country_nm = '{country_filter}'")
    return "WHERE " + " AND ".join(clauses)
