"""친환경 페이지 쿼리 생성 모듈"""


def get_latest_price_statistics_query() -> str:
    """최신 가격 통계 데이터 조회 쿼리를 생성합니다.
    
    Returns:
        str: SQL 쿼리 문자열
    """
    query = """
    WITH latest_date AS (
        SELECT MAX(res_dt) as max_date
        FROM team3_gold.api13_price_statistics_by_category
    )
    SELECT 
        res_dt,
        item_cd,
        item_nm,
        market_category,
        record_count,
        avg_price,
        min_price,
        max_price
    FROM team3_gold.api13_price_statistics_by_category
    CROSS JOIN latest_date
    WHERE res_dt = latest_date.max_date
    ORDER BY item_nm, market_category, avg_price
    """
    
    return query.strip()

