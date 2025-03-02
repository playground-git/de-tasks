{{ 
    config(
        materialized='incremental',
        unique_key='order_id'
    ) 
}}

with stg_orders as (
    select
        id as order_id,
        order_date,
        customer_id,
        order_amount,
        created_at as order_created_at,
        updated_at as order_updated_at,
        uploaded_at as order_uploaded_at
    from {{ ref('orders') }}
),

stg_transactions as (
    select
        id as transaction_id,
        order_id,
        payment_method,
        transaction_amount,
        created_at as transaction_created_at,
        updated_at as transaction_updated_at,
        uploaded_at as transaction_uploaded_at
    from {{ ref('transactions') }}
),

stg_verification as (
    select
        id as verification_id,
        order_id,
        verification_status,
        created_at as verification_created_at,
        updated_at as verification_updated_at,
        uploaded_at as verification_uploaded_at
    from {{ ref('verification') }}
),

joined as (
    select
        o.order_id,
        o.order_date,
        o.customer_id,
        o.order_amount,
        o.order_created_at,
        o.order_updated_at,
        o.order_uploaded_at,

        t.transaction_id,
        t.payment_method,
        t.transaction_amount,
        t.transaction_created_at,
        t.transaction_updated_at,
        t.transaction_uploaded_at,

        v.verification_id,
        v.verification_status,
        v.verification_created_at,
        v.verification_updated_at,
        v.verification_uploaded_at,

        greatest(o.order_uploaded_at, t.transaction_uploaded_at, v.verification_uploaded_at) as latest_uploaded_at
    from stg_orders o
    join stg_transactions t
	on o.order_id = t.order_id
    join stg_verification v
	on o.order_id = v.order_id
)

select
    order_id,
    order_date,
    customer_id,
    order_amount,
    order_created_at,
    order_updated_at,
    order_uploaded_at,
    transaction_id,
    payment_method,
    transaction_amount,
    transaction_created_at,
    transaction_updated_at,
    transaction_uploaded_at,
    verification_id,
    verification_status,
    verification_created_at,
    verification_updated_at,
    verification_uploaded_at,

    current_timestamp as uploaded_at
from joined
{% if is_incremental() %}
where
    -- I suppose that every update to a record in PostgreSQL results in a new ingestion with a refreshed
    -- uploaded_at timestamp, so uploaded_at alone is enough for incremental filtering
    latest_uploaded_at > (select max(uploaded_at) from {{ this }})
{% endif %}
