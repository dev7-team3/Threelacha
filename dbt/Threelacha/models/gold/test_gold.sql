{{ config(
    materialized='table',
    format='PARQUET',
    location='s3a://team3-batch/gold/test_gold/'
) }}

SELECT 
    CAST(dt AS DATE) as dt,
    year,
    month,
    product_cls_name,
    item_name,
    COUNT(*) as record_count,
    CAST(NOW() AS VARCHAR) as created_at
FROM {{ source('silver', 'api_1') }}
WHERE year = 2025 AND month = 12
GROUP BY dt, year, month, product_cls_name, item_name
LIMIT 100