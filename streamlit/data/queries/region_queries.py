"""지역별 쿼리 생성 모듈"""
from typing import Optional
from datetime import date
from .query_utils import build_where_clause


def get_region_comparison_query(
    date_filter: Optional[date] = None,
    category_filter: Optional[str] = None,
    regions: Optional[list[str]] = None
) -> str:
    """지역별 가격 비교 쿼리를 생성합니다.
    
    Args:
        date_filter: 날짜 필터
        category_filter: 카테고리 필터
        regions: 비교할 지역 리스트 (기본값: ["서울", "부산", "대구", "광주", "대전"])
    
    Returns:
        str: SQL 쿼리 문자열
    """
    if regions is None:
        regions = ["서울", "부산", "대구", "광주", "대전"]
    
    where_sql = build_where_clause(date_filter, category_filter)
    
    # 동적으로 지역별 컬럼 생성
    region_cases = "\n        ".join([
        f"MAX(CASE WHEN country_nm = '{region}' THEN avg_price END) as \"{region}_평균가격\","
        for region in regions
    ])
    # 마지막 쉼표 제거
    region_cases = region_cases.rstrip(",")
    
    query = f"""
    WITH aggregated_data AS (
        SELECT 
            item_nm,
            kind_nm,
            country_nm,
            AVG(avg_price) as avg_price,
            SUM(record_count) as total_records
        FROM hive.gold.api17_region_comparison
        {where_sql}
        GROUP BY item_nm, kind_nm, country_nm
        HAVING item_nm IS NOT NULL
    )
    SELECT 
        item_nm,
        kind_nm,
        {region_cases}
    FROM aggregated_data
    GROUP BY item_nm, kind_nm
    HAVING COUNT(DISTINCT country_nm) >= 2
    ORDER BY item_nm, kind_nm
    """
    
    return query.strip()


def get_region_stats_query(
    date_filter: Optional[date] = None,
    category_filter: Optional[str] = None
) -> str:
    """지역별 통계 쿼리를 생성합니다.
    
    Args:
        date_filter: 날짜 필터
        category_filter: 카테고리 필터
    
    Returns:
        str: SQL 쿼리 문자열
    """
    where_sql = build_where_clause(date_filter, category_filter)
    
    query = f"""
    SELECT 
        country_nm,
        item_nm,
        kind_nm,
        AVG(avg_price) as "평균가격",
        MIN(min_price) as "최저가격",
        MAX(max_price) as "최고가격",
        SUM(record_count) as "총레코드수"
    FROM hive.gold.api17_region_comparison
    {where_sql}
    GROUP BY country_nm, item_nm, kind_nm
    ORDER BY country_nm, item_nm, kind_nm
    """
    
    return query.strip()

