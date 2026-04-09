{{ config(materialized='table') }}

WITH date_bounds AS (
    SELECT
        LEAST(
            (SELECT MIN(order_purchase_date)::DATE FROM {{ ref('clean_orders') }}),
            (SELECT MIN(return_date) FROM {{ ref('clean_returns') }})
        ) AS min_date,
        GREATEST(
            (SELECT MAX(order_purchase_date)::DATE FROM {{ ref('clean_orders') }}),
            (SELECT MAX(return_date) FROM {{ ref('clean_returns') }})
        ) AS max_date
),

calendar AS (
    SELECT
        DATEADD(DAY, seq4(), (SELECT DATE_TRUNC('YEAR', min_date) FROM date_bounds)) AS date_key
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
    WHERE date_key <= (SELECT DATEADD(YEAR, 1, DATE_TRUNC('YEAR', max_date)) FROM date_bounds)
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_key']) }} AS date_sk,
    date_key,
    YEAR(date_key) AS year,
    QUARTER(date_key) AS quarter,
    MONTH(date_key) AS month,
    MONTHNAME(date_key) AS month_name,
    DAY(date_key) AS day,
    DAYOFWEEK(date_key) AS day_of_week,
    CASE WHEN DAYOFWEEK(date_key) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM calendar
