"""
<RDS SQL 쿼리 생성>
제철 식자재 지도 쿼리 생성 모듈
"""
from typing import Optional
import pandas as pd
from .query_utils import build_where_country_clause

def get_season(
        ) -> pd.DataFrame:
    """
    mart_season_region_product에서 제철명 불러오기
    """
    query = """
    SELECT DISTINCT season
    FROM hive.gold.mart_season_region_product
    """
    return query.strip()

def get_season_item_list(
        ) -> pd.DataFrame:
    """
    mart_season_region_product에서 사용 가능한 제철 품목 목록 조회
    """
    query = """
    SELECT DISTINCT 
        item_nm,
        kind_nm,
        CONCAT(item_nm, '(', kind_nm, ')') AS item_kind
    FROM hive.gold.mart_season_region_product
    ORDER BY item_nm, kind_nm
    """
    return query.strip()

def get_season_region_price_query(
    item_kind_filter: Optional[str] = None,
) -> str:
    """
    제철 식자재 지역별 지도 쿼리 생성
    Args:
        item_kind_filter: 품목+품종 필터 (예: 사과(부사))
    """
    where_sql = ""
    if item_kind_filter:
        where_sql = f"WHERE CONCAT(item_nm, '(', kind_nm, ')') = '{item_kind_filter}'"

    query = f"""
    SELECT
        product_no,
        category_nm,
        item_nm,
        kind_nm,
        CONCAT(item_nm, '(', kind_nm, ')') AS item_kind,
        product_cls_unit,
        country_nm,
        latitude,
        longitude,
        dt,
        base_dt,
        base_pr,
        prev_1y_dt,
        prev_1y_pr,
        present_month,
        season,
        season_month,
        -- yoy_pct 계산: prev_1y_pr가 0 또는 NULL이면 NULL 처리
        CASE 
            WHEN prev_1y_pr IS NULL OR prev_1y_pr = 0 THEN NULL
            ELSE ( (base_pr - prev_1y_pr) / prev_1y_pr ) * 100
        END AS yoy_pct,
        -- price_rank 계산: base_pr 기준 오름차순
        RANK() OVER (ORDER BY base_pr ASC) AS price_rank
    FROM hive.gold.mart_season_region_product
    {where_sql}
    """
    return query.strip()

def get_region_all_items_price_query(country_filter: str) -> str:
    """
    특정 지역의 모든 제철 식재료 가격 조회 + 전국 가격 순위
    Args:
        country_filter: 지역명 (예: 서울)
    """
    where_sql = f"WHERE country_nm = '{country_filter}'"

    query = f"""
    WITH CTE AS (
        SELECT
            item_nm,
            kind_nm,
            CONCAT(item_nm, '(', kind_nm, ')') AS item_kind,
            product_cls_unit,
            country_nm,
            base_pr,
            prev_1y_pr,
            -- 전국 기준으로 품목별 가격 순위 계산
            RANK() OVER (
                PARTITION BY CONCAT(item_nm, '(', kind_nm, ')')
                ORDER BY base_pr ASC
            ) AS national_rank
        FROM hive.gold.mart_season_region_product
    )
    SELECT *
    FROM CTE
    {where_sql}
    """
    return query.strip()
