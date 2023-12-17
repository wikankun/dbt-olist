{{ 
    config(
        materialized='incremental'
    ) 
}}

with
history as (

    select
        id,
        start_date,
        end_date
    from
        {{ ref('f_sales_item_history') }}
    where
        id = (
            select max(id) from {{ ref('f_sales_item_history') }}
        )
),
source_data as (

    select
        md5(ROW(o.order_id || random())::TEXT) AS pk_id,
        o.order_id,
        o.customer_id,
        oi.seller_id,
        p.product_id,
        op.payment_sequential,
        oi.order_item_id,
        geo.geolocation_zip_code_prefix,
        to_char(o.order_purchase_timestamp, 'YYYYMMDD')::integer AS order_date,
        to_char(o.order_purchase_timestamp, 'HH24MI') AS order_time,
        to_char(o.order_approved_at, 'YYYYMMDD')::integer AS approved_date,
        to_char(o.order_approved_at, 'HH24MI') AS approved_time,
        to_char(o.order_delivered_carrier_date, 'YYYYMMDD')::integer AS delivered_carrier_date,
        to_char(o.order_delivered_carrier_date, 'HH24MI') AS delivered_carrier_time,
        to_char(o.order_delivered_customer_date, 'YYYYMMDD')::integer AS delivered_customer_date,
        to_char(o.order_delivered_customer_date, 'HH24MI') AS delivered_customer_time,
        to_char(o.order_estimated_delivery_date, 'YYYYMMDD')::integer AS delivered_estimated_date,
        o.order_status,
        CASE WHEN op.payment_sequential = 1 THEN oi.price ELSE 0 END AS price,
        CASE WHEN op.payment_sequential = 1 THEN oi.freight_value ELSE 0 END AS shipping,
        op.payment_type,
        op.payment_installments,
        op.payment_value
    from
        {{ source('raw', 'order_items') }} oi
    left join {{ source('raw', 'orders') }} o on
        oi.order_id = o.order_id
    left join {{ source('raw', 'products') }} p on
        p.product_id = oi.product_id
    left join {{ source('raw', 'order_payments') }} op on
        op.order_id = oi.order_id
    left join (
        select
            c.customer_id, g.*
        from
            {{ source('raw', 'customers') }} c
        inner join {{ source('raw', 'geolocation') }} g on
            g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
    ) geo on
        geo.customer_id = o.customer_id
    left join {{ source('raw', 'sellers') }} s on
        s.seller_id = oi.seller_id

    {% if is_incremental() %}

        where o.order_purchase_timestamp >= (select start_date from history)
        and o.order_purchase_timestamp < (select end_date from history)

    {% else %}

        where o.order_purchase_timestamp >= '2016-09-01'
        and o.order_purchase_timestamp < '2016-10-01'

    {% endif %}

)

select *
from source_data
