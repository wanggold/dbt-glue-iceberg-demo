{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=["year", "month", "type"],
    file_format='iceberg',
    iceberg_expire_snapshots='False',
    table_properties={'format-version': '2'}
) }}
SELECT (avg_total_amount/avg_passenger_count) as avg_cost_per_passenger
, (avg_duration/avg_passenger_count) as avg_duration_per_passenger
, (avg_trip_distance/avg_passenger_count) as avg_trip_distance_per_passenger
, year
, month 
, type 
FROM {{ ref('silver_avg_metrics') }}
