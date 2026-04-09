{{ config(materialized='incremental', incremental_strategy='append') }}

WITH orders AS (
    SELECT * FROM {{ ref('clean_orders') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
    {% endif %}
),

transactions AS (
    SELECT * FROM {{ ref('clean_transactions') }}
),

customers AS (
    SELECT customer_sk, customer_id FROM {{ ref('dim_customers') }}
),

products AS (
    SELECT product_sk, product_id FROM {{ ref('dim_products') }}
),

vendors AS (
    SELECT vendor_sk, vendor_id FROM {{ ref('dim_vendors') }}
),

dates AS (
    SELECT date_sk, date_key FROM {{ ref('dim_dates') }}
)

SELECT
    o.order_id,
    c.customer_sk,
    o.customer_id,
    p.product_sk,
    t.product_id,
    v.vendor_sk,
    COALESCE(o.vendor_id, t.vendor_id) AS vendor_id,
    d.date_sk AS order_date_sk,
    o.order_purchase_date::DATE AS order_date_key,
    t.sales,
    t.quantity,
    t.discount,
    t.profit,
    o.ship_mode,
    o.order_status,
    o.order_purchase_date,
    t.payment_type,
    o._load_timestamp
FROM orders o
INNER JOIN transactions t ON o.order_id = t.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products p ON t.product_id = p.product_id
LEFT JOIN vendors v ON COALESCE(o.vendor_id, t.vendor_id) = v.vendor_id
LEFT JOIN dates d ON o.order_purchase_date::DATE = d.date_key
