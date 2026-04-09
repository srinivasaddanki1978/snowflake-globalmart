{{ config(materialized='incremental', incremental_strategy='append') }}

WITH returns AS (
    SELECT * FROM {{ ref('clean_returns') }}
    {% if is_incremental() %}
      WHERE _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
    {% endif %}
),

orders AS (
    SELECT order_id, customer_id, vendor_id FROM {{ ref('clean_orders') }}
),

customers AS (
    SELECT customer_sk, customer_id FROM {{ ref('dim_customers') }}
),

dates AS (
    SELECT date_sk, date_key FROM {{ ref('dim_dates') }}
)

SELECT
    r.order_id,
    c.customer_sk,
    o.customer_id,
    d.date_sk AS return_date_sk,
    r.return_date AS return_date_key,
    r.refund_amount,
    r.return_reason,
    r.return_status,
    r.return_date,
    o.vendor_id,
    r._load_timestamp
FROM returns r
LEFT JOIN orders o ON r.order_id = o.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN dates d ON r.return_date = d.date_key
