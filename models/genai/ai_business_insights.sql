{{ config(materialized='table', schema='GOLD') }}

WITH revenue_kpis AS (
    SELECT
        ROUND(SUM(total_revenue), 2) AS grand_total_revenue,
        ROUND(SUM(total_profit), 2) AS grand_total_profit,
        SUM(total_quantity) AS grand_total_units,
        SUM(order_count) AS grand_total_orders
    FROM {{ ref('mv_monthly_revenue_by_region') }}
),

revenue_by_region AS (
    SELECT
        region,
        ROUND(SUM(total_revenue), 2) AS revenue,
        ROUND(SUM(total_profit), 2) AS profit
    FROM {{ ref('mv_monthly_revenue_by_region') }}
    GROUP BY region
),

revenue_summary AS (
    SELECT
        'revenue_performance' AS insight_type,
        OBJECT_CONSTRUCT(
            'grand_total_revenue', k.grand_total_revenue,
            'grand_total_profit', k.grand_total_profit,
            'grand_total_units', k.grand_total_units,
            'by_region', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('region', region, 'revenue', revenue, 'profit', profit)) FROM revenue_by_region)
        )::VARCHAR AS kpi_data
    FROM revenue_kpis k
),

vendor_kpis AS (
    SELECT
        'vendor_return_rate' AS insight_type,
        OBJECT_CONSTRUCT(
            'vendors', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'vendor', vendor_name,
                'total_sales', total_sales,
                'return_rate', return_rate_pct,
                'total_refunded', total_refunded
            )) FROM {{ ref('mv_return_rate_by_vendor') }})
        )::VARCHAR AS kpi_data
),

inventory_kpis AS (
    SELECT
        'slow_moving_inventory' AS insight_type,
        OBJECT_CONSTRUCT(
            'total_return_cost', (SELECT ROUND(SUM(total_return_cost), 2) FROM {{ ref('mv_product_return_impact') }}),
            'avg_return_cost', (SELECT ROUND(AVG(avg_return_cost), 2) FROM {{ ref('mv_product_return_impact') }}),
            'top_5', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('product', product_name, 'region', region, 'cost', total_return_cost))
                      FROM (SELECT * FROM {{ ref('mv_product_return_impact') }} ORDER BY total_return_cost DESC LIMIT 5))
        )::VARCHAR AS kpi_data
),

all_domains AS (
    SELECT * FROM revenue_summary
    UNION ALL SELECT * FROM vendor_kpis
    UNION ALL SELECT * FROM inventory_kpis
)

SELECT
    insight_type,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'You are an executive analyst for GlobalMart. Write a 4-6 sentence executive summary. '
        || 'Cite specific dollar amounts and percentages. Identify strongest and weakest areas. '
        || 'Include one actionable recommendation. '
        || 'Domain: ' || insight_type || '. KPIs: ' || kpi_data,
        {'temperature': 0.3, 'max_tokens': 512}
    ) AS executive_summary,
    kpi_data,
    CURRENT_TIMESTAMP() AS generated_at
FROM all_domains
