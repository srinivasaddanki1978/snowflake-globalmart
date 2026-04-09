{{ config(materialized='incremental', incremental_strategy='append') }}

-- Customers rejections (4 rules)
SELECT
    'clean_customers' AS source_table,
    customer_id AS record_key,
    'customer_id' AS affected_field,
    'missing_value' AS issue_type,
    OBJECT_CONSTRUCT(*)::VARCHAR AS record_data,
    _source_file,
    CURRENT_TIMESTAMP() AS rejected_at,
    _load_timestamp
FROM {{ ref('_customers_harmonized') }}
WHERE _fail_customer_id_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_customers', customer_id, 'customer_name', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_customers_harmonized') }}
WHERE _fail_customer_name_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_customers', customer_id, 'segment', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_customers_harmonized') }}
WHERE _fail_invalid_segment
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_customers', customer_id, 'region', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_customers_harmonized') }}
WHERE _fail_invalid_region
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

-- Orders rejections (5 rules)
UNION ALL
SELECT 'clean_orders', order_id, 'order_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE _fail_order_id_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_orders', order_id, 'customer_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE _fail_customer_id_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_orders', order_id, 'ship_mode', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE _fail_invalid_ship_mode
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_orders', order_id, 'order_status', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE _fail_invalid_order_status
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_orders', order_id, 'order_purchase_date', 'parse_failure',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_orders_harmonized') }}
WHERE _fail_unparseable_date
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

-- Transactions rejections (5 rules)
UNION ALL
SELECT 'clean_transactions', order_id, 'order_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE _fail_order_id_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_transactions', order_id, 'product_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE _fail_product_id_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_transactions', order_id, 'sales', 'out_of_range',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE _fail_negative_sales
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_transactions', order_id, 'quantity', 'out_of_range',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE _fail_non_positive_quantity
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_transactions', order_id, 'discount', 'out_of_range',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_transactions_harmonized') }}
WHERE _fail_discount_out_of_range
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

-- Returns rejections (5 rules)
UNION ALL
SELECT 'clean_returns', order_id, 'order_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE _fail_order_id_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_returns', order_id, 'refund_amount', 'out_of_range',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE _fail_negative_refund
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_returns', order_id, 'return_status', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE _fail_invalid_return_status
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_returns', order_id, 'return_reason', 'invalid_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE _fail_invalid_return_reason
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_returns', order_id, 'return_date', 'parse_failure',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_returns_harmonized') }}
WHERE _fail_unparseable_return_date
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

-- Products rejections (2 rules)
UNION ALL
SELECT 'clean_products', product_id, 'product_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_products_harmonized') }}
WHERE _fail_product_id_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_products', product_id, 'product_name', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_products_harmonized') }}
WHERE _fail_product_name_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

-- Vendors rejections (2 rules)
UNION ALL
SELECT 'clean_vendors', vendor_id, 'vendor_id', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_vendors_harmonized') }}
WHERE _fail_vendor_id_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}

UNION ALL
SELECT 'clean_vendors', vendor_id, 'vendor_name', 'missing_value',
    OBJECT_CONSTRUCT(*)::VARCHAR, _source_file, CURRENT_TIMESTAMP(), _load_timestamp
FROM {{ ref('_vendors_harmonized') }}
WHERE _fail_vendor_name_null
{% if is_incremental() %}
  AND _load_timestamp > (SELECT MAX(_load_timestamp) FROM {{ this }})
{% endif %}
