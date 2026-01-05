
def get_update_status_query() -> str:
    """
    대시보드 메타 정보 (최신 업데이트 상태) 조회 쿼리
    - 최신 업데이트 날짜
    - 업데이트된 품목 수
    - 업데이트된 지역 수
    """
    return """
        SELECT
            latest_date,
            row_count,
            country_count
        FROM hive.gold.mart_update_status
    """
