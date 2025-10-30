{{ config(
    materialized='table',
    file_format='parquet'
) }}

-- Simple warm-up query to initialize Glue Interactive Session
-- This ensures fair comparison by pre-warming the session
select
    count(*) as total_records,
    count(distinct year) as unique_years,
    count(distinct month) as unique_months,
    min(created_date) as min_date,
    max(created_date) as max_date
from {{ source('json_source', 'sample_json_data') }}
