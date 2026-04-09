#!/usr/bin/env python3
"""
Run dbt models without a local dbt installation.
Compiles Jinja templates to SQL and executes via Snowflake REST API.
Supports: config, ref, source, is_incremental, dbt_utils.generate_surrogate_key.
Ephemeral models are materialized as views for downstream use.
"""

import requests
import json
import re
import sys
import time
import os

BASE_URL = "https://chc70950.us-east-1.snowflakecomputing.com"
WAREHOUSE = "COMPUTE_WH"
ROLE = "ACCOUNTADMIN"
DATABASE = "GLOBALMART"


def load_token():
    token_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Connect-token-secret.txt")
    with open(token_file, "r") as f:
        return f.read().strip()


def execute_sql(token, statement, database=None, schema=None, poll=True):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "statement": statement,
        "timeout": 600,
        "warehouse": WAREHOUSE,
        "role": ROLE,
    }
    if database:
        payload["database"] = database
    if schema:
        payload["schema"] = schema

    resp = requests.post(f"{BASE_URL}/api/v2/statements", headers=headers, json=payload, timeout=120)

    if resp.status_code == 202 and poll:
        data = resp.json()
        handle = data.get("statementHandle")
        if handle:
            return poll_statement(token, handle, headers)

    if resp.status_code not in (200, 202):
        data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        msg = data.get("message", resp.text[:300])
        raise Exception(f"SQL error ({resp.status_code}): {msg}")

    data = resp.json()
    if data.get("code") == "333334" and poll:
        handle = data.get("statementHandle")
        if handle:
            return poll_statement(token, handle, headers)

    return data


def poll_statement(token, handle, headers):
    for _ in range(240):  # 20 minutes max
        time.sleep(5)
        resp = requests.get(f"{BASE_URL}/api/v2/statements/{handle}", headers=headers, timeout=30)
        data = resp.json()
        code = data.get("code", "")
        if code != "333334":
            return data
    raise Exception(f"Statement {handle} timed out")


# ---------------------------------------------------------------------------
# Model registry: name -> schema, materialization, file path
# ---------------------------------------------------------------------------
MODELS = {
    # Bronze (incremental -> table on full refresh)
    "raw_customers":    {"schema": "BRONZE", "mat": "table", "file": "models/bronze/raw_customers.sql"},
    "raw_orders":       {"schema": "BRONZE", "mat": "table", "file": "models/bronze/raw_orders.sql"},
    "raw_transactions": {"schema": "BRONZE", "mat": "table", "file": "models/bronze/raw_transactions.sql"},
    "raw_returns":      {"schema": "BRONZE", "mat": "table", "file": "models/bronze/raw_returns.sql"},
    "raw_products":     {"schema": "BRONZE", "mat": "table", "file": "models/bronze/raw_products.sql"},
    "raw_vendors":      {"schema": "BRONZE", "mat": "table", "file": "models/bronze/raw_vendors.sql"},

    # Silver ephemeral -> materialized as views
    "_customers_harmonized":    {"schema": "SILVER", "mat": "view", "file": "models/silver/_customers_harmonized.sql"},
    "_orders_harmonized":       {"schema": "SILVER", "mat": "view", "file": "models/silver/_orders_harmonized.sql"},
    "_transactions_harmonized": {"schema": "SILVER", "mat": "view", "file": "models/silver/_transactions_harmonized.sql"},
    "_returns_harmonized":      {"schema": "SILVER", "mat": "view", "file": "models/silver/_returns_harmonized.sql"},
    "_products_harmonized":     {"schema": "SILVER", "mat": "view", "file": "models/silver/_products_harmonized.sql"},
    "_vendors_harmonized":      {"schema": "SILVER", "mat": "view", "file": "models/silver/_vendors_harmonized.sql"},

    # Silver clean (incremental -> table on full refresh)
    "clean_customers":    {"schema": "SILVER", "mat": "table", "file": "models/silver/clean_customers.sql"},
    "clean_orders":       {"schema": "SILVER", "mat": "table", "file": "models/silver/clean_orders.sql"},
    "clean_transactions": {"schema": "SILVER", "mat": "table", "file": "models/silver/clean_transactions.sql"},
    "clean_returns":      {"schema": "SILVER", "mat": "table", "file": "models/silver/clean_returns.sql"},
    "clean_products":     {"schema": "SILVER", "mat": "table", "file": "models/silver/clean_products.sql"},
    "clean_vendors":      {"schema": "SILVER", "mat": "table", "file": "models/silver/clean_vendors.sql"},

    # Silver support
    "rejected_records":   {"schema": "SILVER", "mat": "table", "file": "models/silver/rejected_records.sql"},
    "pipeline_audit_log": {"schema": "SILVER", "mat": "view",  "file": "models/silver/pipeline_audit_log.sql"},

    # Gold dimensions
    "dim_customers": {"schema": "GOLD", "mat": "table", "file": "models/gold/dim_customers.sql"},
    "dim_products":  {"schema": "GOLD", "mat": "table", "file": "models/gold/dim_products.sql"},
    "dim_vendors":   {"schema": "GOLD", "mat": "table", "file": "models/gold/dim_vendors.sql"},
    "dim_dates":     {"schema": "GOLD", "mat": "table", "file": "models/gold/dim_dates.sql"},

    # Gold facts
    "fact_sales":   {"schema": "GOLD", "mat": "table", "file": "models/gold/fact_sales.sql"},
    "fact_returns": {"schema": "GOLD", "mat": "table", "file": "models/gold/fact_returns.sql"},

    # Gold bridge + MVs
    "bridge_return_products":   {"schema": "GOLD", "mat": "table", "file": "models/gold/bridge_return_products.sql"},
    "mv_revenue_by_region":     {"schema": "GOLD", "mat": "table", "file": "models/gold/mv_revenue_by_region.sql"},
    "mv_return_rate_by_vendor": {"schema": "GOLD", "mat": "table", "file": "models/gold/mv_return_rate_by_vendor.sql"},
    "mv_slow_moving_products":  {"schema": "GOLD", "mat": "table", "file": "models/gold/mv_slow_moving_products.sql"},

    # Metrics (views)
    "mv_monthly_revenue_by_region":  {"schema": "METRICS", "mat": "view", "file": "models/metrics/mv_monthly_revenue_by_region.sql"},
    "mv_segment_profitability":      {"schema": "METRICS", "mat": "view", "file": "models/metrics/mv_segment_profitability.sql"},
    "mv_customer_return_history":    {"schema": "METRICS", "mat": "view", "file": "models/metrics/mv_customer_return_history.sql"},
    "mv_return_reason_analysis":     {"schema": "METRICS", "mat": "view", "file": "models/metrics/mv_return_reason_analysis.sql"},
    "mv_vendor_product_performance": {"schema": "METRICS", "mat": "view", "file": "models/metrics/mv_vendor_product_performance.sql"},
    "mv_product_return_impact":      {"schema": "METRICS", "mat": "view", "file": "models/metrics/mv_product_return_impact.sql"},

    # GenAI (tables in GOLD schema)
    "dq_audit_report":          {"schema": "GOLD", "mat": "table", "file": "models/genai/dq_audit_report.sql"},
    "flagged_return_customers": {"schema": "GOLD", "mat": "table", "file": "models/genai/flagged_return_customers.sql"},
    "ai_business_insights":     {"schema": "GOLD", "mat": "table", "file": "models/genai/ai_business_insights.sql"},
}

# Topological execution order
EXECUTION_ORDER = [
    # Bronze
    "raw_customers", "raw_orders", "raw_transactions", "raw_returns", "raw_products", "raw_vendors",
    # Silver ephemeral (materialized as views)
    "_customers_harmonized", "_orders_harmonized", "_transactions_harmonized",
    "_returns_harmonized", "_products_harmonized", "_vendors_harmonized",
    # Silver clean
    "clean_customers", "clean_orders", "clean_transactions",
    "clean_returns", "clean_products", "clean_vendors",
    # Silver support
    "rejected_records", "pipeline_audit_log",
    # Gold dims (dates depends on clean_orders + clean_returns)
    "dim_customers", "dim_products", "dim_vendors", "dim_dates",
    # Gold facts
    "fact_sales", "fact_returns",
    # Gold bridge + MVs
    "bridge_return_products",
    "mv_revenue_by_region", "mv_return_rate_by_vendor", "mv_slow_moving_products",
    # Metrics
    "mv_monthly_revenue_by_region", "mv_segment_profitability",
    "mv_customer_return_history", "mv_return_reason_analysis",
    "mv_vendor_product_performance", "mv_product_return_impact",
    # GenAI
    "dq_audit_report", "flagged_return_customers", "ai_business_insights",
]


def compile_jinja(sql, model_name):
    """Compile Jinja templates in a dbt model SQL file to plain SQL."""

    # 1. Strip {{ config(...) }} blocks
    sql = re.sub(r'\{\{[\s]*config\([^)]*\)[\s]*\}\}', '', sql)

    # 2. Strip {% if is_incremental() %} ... {% endif %} blocks (full refresh)
    sql = re.sub(
        r'\{%[\s]*if[\s]+is_incremental\(\)[\s]*%\}.*?\{%[\s]*endif[\s]*%\}',
        '', sql, flags=re.DOTALL
    )

    # 3. Resolve {{ source('schema_name', 'TABLE_NAME') }}
    def resolve_source(m):
        table = m.group(2).strip("'\"")
        return f"GLOBALMART.RAW.{table}"
    sql = re.sub(
        r"""\{\{[\s]*source\([\s]*['"](\w+)['"][\s]*,[\s]*['"](\w+)['"][\s]*\)[\s]*\}\}""",
        resolve_source, sql
    )

    # 4. Resolve {{ ref('model_name') }}
    def resolve_ref(m):
        ref_name = m.group(1)
        if ref_name in MODELS:
            schema = MODELS[ref_name]["schema"]
            table_upper = ref_name.upper()
            return f"GLOBALMART.{schema}.{table_upper}"
        return ref_name
    sql = re.sub(
        r"""\{\{[\s]*ref\([\s]*['"]([^'"]+)['"][\s]*\)[\s]*\}\}""",
        resolve_ref, sql
    )

    # 5. Resolve {{ this }}
    if model_name in MODELS:
        schema = MODELS[model_name]["schema"]
        table_upper = model_name.upper()
        sql = re.sub(r'\{\{[\s]*this[\s]*\}\}', f"GLOBALMART.{schema}.{table_upper}", sql)

    # 6. Resolve {{ dbt_utils.generate_surrogate_key(['col1', 'col2']) }}
    def resolve_surrogate_key(m):
        cols_str = m.group(1)
        cols = re.findall(r"'([^']+)'", cols_str)
        parts = []
        for col in cols:
            parts.append(f"COALESCE(CAST({col} AS VARCHAR), '_dbt_utils_surrogate_key_null_')")
        if len(parts) == 1:
            return f"MD5({parts[0]})"
        return "MD5(" + " || '-' || ".join(parts) + ")"
    sql = re.sub(
        r"""\{\{[\s]*dbt_utils\.generate_surrogate_key\(\[([^\]]+)\]\)[\s]*\}\}""",
        resolve_surrogate_key, sql
    )

    # 7. Strip any remaining Jinja comments {# ... #}
    sql = re.sub(r'\{#.*?#\}', '', sql, flags=re.DOTALL)

    return sql.strip()


def build_ddl(model_name, compiled_sql, materialization, schema):
    """Wrap compiled SQL in CREATE TABLE/VIEW AS statement."""
    table_upper = model_name.upper()
    fqn = f"GLOBALMART.{schema}.{table_upper}"
    if materialization == "view":
        return f"CREATE OR REPLACE VIEW {fqn} AS\n{compiled_sql}"
    else:
        return f"CREATE OR REPLACE TABLE {fqn} AS\n{compiled_sql}"


def main():
    token = load_token()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_dir = os.path.join(script_dir, "globalmart_dbt")

    # Parse optional --select argument
    selected = None
    if "--select" in sys.argv:
        idx = sys.argv.index("--select")
        if idx + 1 < len(sys.argv):
            selected = sys.argv[idx + 1].split(",")

    print("=" * 60)
    print("GlobalMart dbt Model Runner (REST API)")
    print("=" * 60)

    # Verify connection
    print("\nVerifying connection...")
    data = execute_sql(token, "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()", database=DATABASE)
    row = data["data"][0]
    print(f"  User: {row[0]}, Role: {row[1]}, Warehouse: {row[2]}")

    # Determine which models to run
    if selected:
        # Expand layer names to individual models
        layer_map = {
            "bronze": [m for m in EXECUTION_ORDER if m.startswith("raw_")],
            "silver": [m for m in EXECUTION_ORDER if m.startswith(("_", "clean_", "rejected_", "pipeline_"))],
            "gold": [m for m in EXECUTION_ORDER if MODELS.get(m, {}).get("schema") == "GOLD" and not m.startswith(("dq_", "flagged_", "ai_", "rag_"))],
            "metrics": [m for m in EXECUTION_ORDER if MODELS.get(m, {}).get("schema") == "METRICS"],
            "genai": [m for m in EXECUTION_ORDER if m in ("dq_audit_report", "flagged_return_customers", "ai_business_insights")],
        }
        models_to_run = []
        for s in selected:
            s = s.strip()
            if s in layer_map:
                models_to_run.extend(layer_map[s])
            elif s in MODELS:
                models_to_run.append(s)
            else:
                print(f"  WARNING: Unknown model/layer '{s}', skipping")
        # Preserve execution order
        models_to_run = [m for m in EXECUTION_ORDER if m in models_to_run]
    else:
        models_to_run = EXECUTION_ORDER

    print(f"\nRunning {len(models_to_run)} models...\n")

    success = 0
    errors = 0
    skipped = 0

    for i, model_name in enumerate(models_to_run):
        info = MODELS[model_name]
        schema = info["schema"]
        mat = info["mat"]
        filepath = os.path.join(dbt_dir, info["file"])

        label = f"[{i+1}/{len(models_to_run)}]"
        obj_type = "VIEW" if mat == "view" else "TABLE"

        if not os.path.exists(filepath):
            print(f"  {label} SKIP {schema}.{model_name.upper()} — file not found")
            skipped += 1
            continue

        # Read and compile
        with open(filepath, "r") as f:
            raw_sql = f.read()

        try:
            compiled = compile_jinja(raw_sql, model_name)
            ddl = build_ddl(model_name, compiled, mat, schema)

            print(f"  {label} Creating {obj_type} {schema}.{model_name.upper()}...", end=" ", flush=True)
            result = execute_sql(token, ddl, database=DATABASE, schema=schema)

            num_rows = result.get("resultSetMetaData", {}).get("numRows", 0)
            msg = result.get("message", "")
            if "error" in msg.lower():
                print(f"ERROR: {msg[:120]}")
                errors += 1
            else:
                # For tables, query row count
                if mat == "table":
                    try:
                        cnt = execute_sql(token, f"SELECT COUNT(*) FROM GLOBALMART.{schema}.{model_name.upper()}", database=DATABASE)
                        row_count = cnt["data"][0][0]
                        print(f"OK ({row_count} rows)")
                    except Exception:
                        print(f"OK")
                else:
                    print(f"OK (view)")
                success += 1

        except Exception as e:
            err = str(e)[:200]
            print(f"ERROR: {err}")
            errors += 1

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Results: {success} succeeded, {errors} errors, {skipped} skipped")
    print(f"{'=' * 60}")

    # Final table counts
    if not selected or any(s in (selected or []) for s in ["gold", "all"]):
        print("\nGold layer row counts:")
        gold_tables = ["DIM_CUSTOMERS", "DIM_PRODUCTS", "DIM_VENDORS", "DIM_DATES",
                        "FACT_SALES", "FACT_RETURNS", "BRIDGE_RETURN_PRODUCTS",
                        "MV_REVENUE_BY_REGION", "MV_RETURN_RATE_BY_VENDOR", "MV_SLOW_MOVING_PRODUCTS"]
        for t in gold_tables:
            try:
                r = execute_sql(token, f"SELECT COUNT(*) FROM GLOBALMART.GOLD.{t}", database=DATABASE)
                print(f"  {t}: {r['data'][0][0]} rows")
            except Exception:
                print(f"  {t}: not found")


if __name__ == "__main__":
    main()
