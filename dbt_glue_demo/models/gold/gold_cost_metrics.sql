{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=["year", "month", "type"],
    file_format='iceberg',
    iceberg_expire_snapshots='False',
    table_properties={'format-version': '2'}
) }}
SELECT (avg_total_amount/avg_trip_distance) as avg_cost_per_distance
, (avg_total_amount/avg_duration) as avg_cost_per_minute
, year
, month 
, type 
FROM {{ ref('silver_avg_metrics') }}
