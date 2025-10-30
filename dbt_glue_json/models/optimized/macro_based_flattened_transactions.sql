{{ config(
    materialized='table',
    file_format='parquet'
) }}

-- Optimized JSON flattening using macro for schema reusability
with parsed_json as (
    select
        id,
        year,
        month,
        created_date,
        -- Use macro for consistent schema definition
        from_json(data, {{ transaction_json_schema() }}) as parsed_data
    from {{ source('json_source', 'sample_json_data') }}
),

-- Extract key business metrics efficiently
business_metrics as (
    select
        id,
        year,
        month,
        created_date,
        
        -- Core transaction identifiers
        parsed_data._id as transaction_id,
        parsed_data.gnId as account_id,
        parsed_data.status,
        parsed_data.type as transaction_type,
        
        -- Financial metrics
        parsed_data.amount,
        cast(parsed_data.originalTransaction.price as double) as original_price,
        parsed_data.currency,
        parsed_data.originalTransaction.currency as original_currency,
        
        -- Platform and product info
        parsed_data.platform,
        coalesce(parsed_data.appType, 'ios') as app_type,
        parsed_data.originalTransaction.store as store,
        parsed_data.originalTransaction.environment as environment,
        
        -- Geographic and user segmentation
        parsed_data.extraInfo.countryCode as country_code,
        parsed_data.extraInfo.transactionSource as transaction_source,
        
        -- Product categorization
        parsed_data.extraInfo.trackInfo.productName as product_name,
        parsed_data.extraInfo.trackInfo.productType as product_type,
        parsed_data.extraInfo.trackInfo.creatorName as creator_name,
        
        -- A/B testing and experimentation
        parsed_data.extraInfo.trackInfo.experiments[0].name as experiment_name,
        case 
            when parsed_data.extraInfo.trackInfo.experiments[0].userGroup = 'default' then 'control'
            when parsed_data.extraInfo.trackInfo.experiments[0].userGroup = 'true' then 'experiment'
            else 'unknown'
        end as experiment_variant,
        
        -- Subscription and lifecycle
        parsed_data.originalTransaction.is_trial_period = 'true' as is_trial,
        parsed_data.originalTransaction.is_in_intro_offer_period = 'true' as is_intro_offer,
        parsed_data.extraInfo.isSandbox = 'true' as is_sandbox,
        
        -- Timestamps as strings (AWS Glue doesn't support date_parse)
        parsed_data.createdAt as created_at,
        parsed_data.updatedAt as updated_at
        
    from parsed_json
)

select * from business_metrics
