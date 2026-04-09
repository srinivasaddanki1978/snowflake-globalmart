{{ config(materialized='ephemeral') }}

WITH source AS (
    SELECT * FROM {{ ref('raw_returns') }}
),

harmonized AS (
    SELECT
        -- COALESCE order_id variants
        COALESCE(order_id_v1, order_id_v2) AS order_id,

        -- COALESCE + map return reason ('?' → 'Unknown')
        CASE
            WHEN COALESCE(return_reason_v1, return_reason_v2) = '?' THEN 'Unknown'
            ELSE COALESCE(return_reason_v1, return_reason_v2)
        END AS return_reason,

        -- COALESCE + map return status abbreviations
        CASE UPPER(TRIM(COALESCE(return_status_v1, return_status_v2)))
            WHEN 'APPRVD' THEN 'Approved'
            WHEN 'PENDG' THEN 'Pending'
            WHEN 'RJCTD' THEN 'Rejected'
            WHEN 'APPROVED' THEN 'Approved'
            WHEN 'PENDING' THEN 'Pending'
            WHEN 'REJECTED' THEN 'Rejected'
            ELSE TRIM(COALESCE(return_status_v1, return_status_v2))
        END AS return_status,

        -- Save raw values for rejection reporting
        COALESCE(refund_amount_v1, refund_amount_v2, refund_amount_v3) AS _raw_refund,
        COALESCE(return_date_v1, return_date_v2, return_date_v3) AS _raw_return_date,

        -- Handle '?' refund amount → NULL
        CASE
            WHEN COALESCE(refund_amount_v1, refund_amount_v2, refund_amount_v3) = '?'
                THEN NULL
            ELSE TRY_TO_DOUBLE(COALESCE(refund_amount_v1, refund_amount_v2, refund_amount_v3))
        END AS refund_amount,

        -- Parse return date with multiple formats
        COALESCE(
            TRY_TO_DATE(COALESCE(return_date_v1, return_date_v2, return_date_v3), 'YYYY-MM-DD'),
            TRY_TO_DATE(COALESCE(return_date_v1, return_date_v2, return_date_v3), 'MM-DD-YYYY'),
            TRY_TO_DATE(COALESCE(return_date_v1, return_date_v2, return_date_v3), 'MM/DD/YYYY'),
            TRY_TO_DATE(COALESCE(return_date_v1, return_date_v2, return_date_v3), 'YYYY-MM-DD"T"HH24:MI:SS')
        ) AS return_date,

        _source_file,
        _load_timestamp
    FROM source
),

flagged AS (
    SELECT
        *,
        -- Rule 15: order_id NOT NULL
        (order_id IS NULL) AS _fail_order_id_null,
        -- Rule 16: non-negative refund
        (refund_amount IS NULL AND _raw_refund != '?' AND _raw_refund IS NOT NULL)
            OR (refund_amount IS NOT NULL AND refund_amount < 0)
            AS _fail_negative_refund,
        -- Rule 17: valid return status
        (return_status IS NULL OR return_status NOT IN ('Approved', 'Pending', 'Rejected'))
            AS _fail_invalid_return_status,
        -- Rule 18: valid return reason (not NULL or Unknown)
        (return_reason IS NULL OR return_reason = 'Unknown')
            AS _fail_invalid_return_reason,
        -- Rule 19: parseable return date
        (return_date IS NULL AND _raw_return_date IS NOT NULL)
            AS _fail_unparseable_return_date,

        -- Composite
        (order_id IS NULL)
        OR ((refund_amount IS NULL AND _raw_refund != '?' AND _raw_refund IS NOT NULL)
            OR (refund_amount IS NOT NULL AND refund_amount < 0))
        OR (return_status IS NULL OR return_status NOT IN ('Approved', 'Pending', 'Rejected'))
        OR (return_reason IS NULL OR return_reason = 'Unknown')
        OR (return_date IS NULL AND _raw_return_date IS NOT NULL)
            AS _has_any_failure
    FROM harmonized
)

SELECT * FROM flagged
