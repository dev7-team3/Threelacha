{{ config(
    materialized='table',
    format='PARQUET',
    location='s3a://team3-batch/gold/api17_region_comparison/',
    partitioned_by=['year', 'month']
) }}

SELECT 
    res_dt,
    year,
    month,
    week_of_year,
    weekday_nm,
    weekend_yn,
    -- 지역 정보
    country_cd,
    country_nm,
    -- 품목 정보
    category_nm,
    item_nm,
    kind_nm,
    rank_nm,
    -- 가격 통계
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(*) as record_count,
    COUNT(DISTINCT market_nm) as market_count,
    -- 메타데이터
    CAST(NOW() AS VARCHAR) as created_at
FROM {{ source('silver', 'api17') }}
WHERE price IS NOT NULL
  AND country_nm IS NOT NULL
GROUP BY 
    res_dt, year, month, week_of_year, weekday_nm, weekend_yn,
    country_cd, country_nm,
    category_nm, item_nm, kind_nm, rank_nm