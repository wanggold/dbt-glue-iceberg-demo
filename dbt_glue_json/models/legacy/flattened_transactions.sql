-- Legacy approach: Multiple get_json_object calls (60+ JSON parsing operations)
select
    id,
    -- Basic transaction fields (10 JSON parses)
    get_json_object(data, '$._id') as _id,
    get_json_object(data, '$.gnId') as gn_account_id,
    get_json_object(data, '$.status') as status,
    get_json_object(data, '$.type') as transaction_type,
    get_json_object(data, '$.platform') as platform,
    coalesce(get_json_object(data, '$.appType'), 'ios') as app_type,
    get_json_object(data, '$.currency') as currency,
    cast(get_json_object(data, '$.amount') as double) as amount,
    get_json_object(data, '$.purchaseDateMs') as purchase_date_ms,
    get_json_object(data, '$.completedAt') as completed_at,

    -- Product information (5 JSON parses)
    get_json_object(data, '$.platformProductIdentifier') as platform_product_identifier,
    get_json_object(data, '$.transactionId') as transaction_id,
    get_json_object(data, '$.ownerType') as owner_type,
    get_json_object(data, '$.customerSubscription') as customer_subscription,
    get_json_object(data, '$.marketplaceProductId') as marketplace_product_id,

    -- Original transaction details (20 JSON parses)
    get_json_object(data, '$.originalTransaction.product_id') as original_transaction_product_id,
    get_json_object(data, '$.originalTransaction.original_transaction_id') as original_transaction_original_transaction_id,
    get_json_object(data, '$.originalTransaction.is_trial_period') as original_transaction_is_trial_period,
    get_json_object(data, '$.originalTransaction.is_in_intro_offer_period') as original_transaction_is_in_intro_offer_period,
    get_json_object(data, '$.originalTransaction.in_app_ownership_type') as original_transaction_in_app_ownership_type,
    get_json_object(data, '$.originalTransaction.is_family_share') as original_transaction_is_family_share,
    get_json_object(data, '$.originalTransaction.purchase_date') as original_transaction_purchase_date,
    get_json_object(data, '$.originalTransaction.purchase_date_ms') as original_transaction_purchase_date_ms,
    get_json_object(data, '$.originalTransaction.type') as original_transaction_type,
    get_json_object(data, '$.originalTransaction.subscription_group_identifier') as original_transaction_subscription_group_identifier,
    get_json_object(data, '$.originalTransaction.expires_date_ms') as original_transaction_expires_date_ms,
    get_json_object(data, '$.originalTransaction.expires_date') as original_transaction_expires_date,
    get_json_object(data, '$.originalTransaction.transactionReason') as original_transaction_transaction_reason,
    get_json_object(data, '$.originalTransaction.signedDate') as original_transaction_signed_date,
    cast(get_json_object(data, '$.originalTransaction.quantity') as int) as original_transaction_quantity,
    get_json_object(data, '$.originalTransaction.transaction_id') as original_transaction_transaction_id,
    cast(get_json_object(data, '$.originalTransaction.price') as double) as original_transaction_price,
    get_json_object(data, '$.originalTransaction.currency') as original_transaction_currency,
    get_json_object(data, '$.originalTransaction.store') as original_transaction_store,
    get_json_object(data, '$.originalTransaction.environment') as original_transaction_environment,

    -- Extra info fields (15 JSON parses)
    get_json_object(data, '$.extraInfo.countryCode') as extra_info_country_code,
    get_json_object(data, '$.extraInfo.transactionSource') as extra_info_transaction_source,
    get_json_object(data, '$.extraInfo.isSandbox') as extra_info_is_sandbox,
    get_json_object(data, '$.extraInfo.bundleTransactionId') as bundle_transaction_id,
    get_json_object(data, '$.extraInfo.productPurchaseType') as deal_type,
    get_json_object(data, '$.extraInfo.priceTier') as price_tier,
    get_json_object(data, '$.extraInfo.trackInfo.productName') as product_name,
    get_json_object(data, '$.extraInfo.trackInfo.creatorName') as creator_name,
    get_json_object(data, '$.extraInfo.trackInfo.productType') as product_type,
    get_json_object(data, '$.extraInfo.trackInfo.experiments[0].name') as experiment_name,
    case 
        when get_json_object(data, '$.extraInfo.trackInfo.experiments[0].userGroup') = 'default' then 'control'
        when get_json_object(data, '$.extraInfo.trackInfo.experiments[0].userGroup') = 'true' then 'experiment'
        else 'unknown'
    end as experiment_variant,
    get_json_object(data, '$.originalTransaction.offerDiscountType') as original_transaction_offer_discount_type,
    get_json_object(data, '$.originalTransaction.storefront') as original_transaction_storefront,
    get_json_object(data, '$.originalTransaction.bundleId') as bundle_id,
    get_json_object(data, '$.expireDateMs') as rental_expire_date,

    -- Partition fields
    year,
    month,
    created_date

from {{ source('json_source', 'sample_json_data') }}
