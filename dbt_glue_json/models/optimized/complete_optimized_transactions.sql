-- Complete optimized approach: Single from_json parse with exact field parity to legacy model
with parsed_json as (
    select
        id,
        year,
        month,
        created_date,
        from_json(
            data,
            'struct<_id:string,gnId:string,status:string,type:string,platform:string,appType:string,currency:string,amount:double,purchaseDateMs:string,completedAt:string,platformProductIdentifier:string,transactionId:string,ownerType:string,customerSubscription:string,marketplaceProductId:string,expireDateMs:string,originalTransaction:struct<product_id:string,original_transaction_id:string,transaction_id:string,is_trial_period:string,is_in_intro_offer_period:string,in_app_ownership_type:string,is_family_share:string,purchase_date:string,purchase_date_ms:string,type:string,subscription_group_identifier:string,expires_date_ms:string,expires_date:string,transactionReason:string,signedDate:string,quantity:string,price:string,currency:string,store:string,storefront:string,environment:string,offerDiscountType:string,bundleId:string>,extraInfo:struct<countryCode:string,transactionSource:string,isSandbox:string,bundleTransactionId:string,productPurchaseType:string,priceTier:string,trackInfo:struct<productName:string,creatorName:string,productType:string,experiments:array<struct<name:string,userGroup:string>>>>>'
        ) as p
    from {{ source('json_source', 'sample_json_data') }}
)

select
    id,
    p._id as _id,
    p.gnId as gn_account_id,
    p.status as status,
    p.type as transaction_type,
    p.platform as platform,
    coalesce(p.appType, 'ios') as app_type,
    p.currency as currency,
    p.amount as amount,
    p.purchaseDateMs as purchase_date_ms,
    p.completedAt as completed_at,
    p.platformProductIdentifier as platform_product_identifier,
    p.transactionId as transaction_id,
    p.ownerType as owner_type,
    p.customerSubscription as customer_subscription,
    p.marketplaceProductId as marketplace_product_id,
    p.expireDateMs as rental_expire_date,
    p.originalTransaction.product_id as original_transaction_product_id,
    p.originalTransaction.original_transaction_id as original_transaction_original_transaction_id,
    p.originalTransaction.is_trial_period as original_transaction_is_trial_period,
    p.originalTransaction.is_in_intro_offer_period as original_transaction_is_in_intro_offer_period,
    p.originalTransaction.in_app_ownership_type as original_transaction_in_app_ownership_type,
    p.originalTransaction.is_family_share as original_transaction_is_family_share,
    p.originalTransaction.purchase_date as original_transaction_purchase_date,
    p.originalTransaction.purchase_date_ms as original_transaction_purchase_date_ms,
    p.originalTransaction.type as original_transaction_type,
    p.originalTransaction.subscription_group_identifier as original_transaction_subscription_group_identifier,
    p.originalTransaction.expires_date_ms as original_transaction_expires_date_ms,
    p.originalTransaction.expires_date as original_transaction_expires_date,
    p.originalTransaction.transactionReason as original_transaction_transaction_reason,
    p.originalTransaction.signedDate as original_transaction_signed_date,
    cast(p.originalTransaction.quantity as int) as original_transaction_quantity,
    p.originalTransaction.transaction_id as original_transaction_transaction_id,
    cast(p.originalTransaction.price as double) as original_transaction_price,
    p.originalTransaction.currency as original_transaction_currency,
    p.originalTransaction.store as original_transaction_store,
    p.originalTransaction.storefront as original_transaction_storefront,
    p.originalTransaction.environment as original_transaction_environment,
    p.originalTransaction.offerDiscountType as original_transaction_offer_discount_type,
    p.originalTransaction.bundleId as bundle_id,
    p.extraInfo.countryCode as extra_info_country_code,
    p.extraInfo.transactionSource as extra_info_transaction_source,
    p.extraInfo.isSandbox as extra_info_is_sandbox,
    p.extraInfo.bundleTransactionId as bundle_transaction_id,
    p.extraInfo.productPurchaseType as deal_type,
    p.extraInfo.priceTier as price_tier,
    p.extraInfo.trackInfo.productName as product_name,
    p.extraInfo.trackInfo.creatorName as creator_name,
    p.extraInfo.trackInfo.productType as product_type,
    p.extraInfo.trackInfo.experiments[0].name as experiment_name,
    case 
        when p.extraInfo.trackInfo.experiments[0].userGroup = 'default' then 'control'
        when p.extraInfo.trackInfo.experiments[0].userGroup = 'true' then 'experiment'
        else 'unknown'
    end as experiment_variant,
    year,
    month,
    created_date
from parsed_json
