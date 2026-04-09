#!/usr/bin/env python3
"""
GlobalMart Snowflake + dbt Deployment Script
Uses Snowflake REST API (SQL API v2) with Bearer token authentication.
Runs setup SQL scripts 01-07, then leverages Snowflake-native dbt.
"""

import requests
import json
import sys
import time
import os

BASE_URL = "https://chc70950.us-east-1.snowflakecomputing.com"
WAREHOUSE = "COMPUTE_WH"
ROLE = "ACCOUNTADMIN"

def load_token():
    token_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Connect-token-secret.txt")
    with open(token_file, "r") as f:
        return f.read().strip()

def execute_sql(token, statement, database=None, schema=None, poll=True):
    """Execute a single SQL statement via Snowflake REST API."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "statement": statement,
        "timeout": 300,
        "warehouse": WAREHOUSE,
        "role": ROLE,
    }
    if database:
        payload["database"] = database
    if schema:
        payload["schema"] = schema

    resp = requests.post(f"{BASE_URL}/api/v2/statements", headers=headers, json=payload, timeout=60)

    if resp.status_code != 200:
        data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        msg = data.get("message", resp.text[:200])
        # Check if async
        if resp.status_code == 202 and poll:
            handle = data.get("statementHandle")
            if handle:
                return poll_statement(token, handle, headers)
        raise Exception(f"SQL error ({resp.status_code}): {msg}")

    data = resp.json()
    # If the query is still running (async)
    if data.get("code") == "333334" and poll:
        handle = data.get("statementHandle")
        if handle:
            return poll_statement(token, handle, headers)

    return data

def poll_statement(token, handle, headers):
    """Poll for async statement completion."""
    for _ in range(120):  # 10 minutes max
        time.sleep(5)
        resp = requests.get(f"{BASE_URL}/api/v2/statements/{handle}", headers=headers, timeout=30)
        data = resp.json()
        status = data.get("statementStatusUrl", "")
        code = data.get("code", "")
        if code != "333334":  # Not still running
            return data
    raise Exception(f"Statement {handle} timed out")

def run_sql_file(token, filepath, database=None, schema=None):
    """Parse and execute a SQL file, splitting on semicolons."""
    with open(filepath, "r") as f:
        content = f.read()

    # Split on semicolons, filter empty/comment-only statements
    statements = []
    current = []
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("--") and not current:
            continue
        current.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt and stmt != ";":
                # Remove trailing semicolons for API
                stmt = stmt.rstrip(";").strip()
                if stmt and not all(l.strip().startswith("--") or not l.strip() for l in stmt.split("\n")):
                    statements.append(stmt)
            current = []
    # Handle last statement without semicolon
    if current:
        stmt = "\n".join(current).strip().rstrip(";").strip()
        if stmt and not all(l.strip().startswith("--") or not l.strip() for l in stmt.split("\n")):
            statements.append(stmt)

    results = []
    for i, stmt in enumerate(statements):
        # Skip pure comment blocks
        lines = [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
        if not lines:
            continue

        # Skip PUT commands (not supported via REST API)
        first_word = lines[0].strip().split()[0].upper() if lines else ""
        if first_word == "PUT":
            print(f"    [SKIP] PUT command (requires SnowSQL/CLI)")
            continue
        if first_word == "LIST":
            print(f"    [SKIP] LIST command")
            continue

        short = lines[0][:80].replace("\n", " ")
        try:
            data = execute_sql(token, stmt, database=database, schema=schema)
            num_rows = data.get("resultSetMetaData", {}).get("numRows", 0)
            # Print result data for SELECT/SHOW queries
            if first_word in ("SELECT", "SHOW") and "data" in data:
                cols = [c["name"] for c in data["resultSetMetaData"]["rowType"]]
                print(f"    [OK] {short}...")
                for row in data.get("data", [])[:10]:
                    print(f"         {dict(zip(cols, row))}")
            else:
                print(f"    [OK] {short}... ({num_rows} rows)")
            results.append(("OK", short))
        except Exception as e:
            err = str(e)[:120]
            print(f"    [ERR] {short}... => {err}")
            results.append(("ERR", short, err))

    return results

def main():
    token = load_token()
    script_dir = os.path.dirname(os.path.abspath(__file__))

    print("=" * 60)
    print("GlobalMart Snowflake + dbt Deployment")
    print("=" * 60)

    # Verify connection
    print("\n[0/7] Verifying connection...")
    data = execute_sql(token, "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
    row = data["data"][0]
    print(f"  User: {row[0]}, Role: {row[1]}, Warehouse: {row[2]}")

    # Run setup scripts
    scripts = [
        ("1/7", "Creating infrastructure...", "setup/01_create_infrastructure.sql", None, None),
        ("2/7", "Creating stage...", "setup/02_create_stage_and_upload.sql", "GLOBALMART", "RAW"),
        ("3/7", "Creating landing tables...", "setup/03_create_landing_tables.sql", "GLOBALMART", "RAW"),
        ("4/7", "Running initial seed...", "setup/04_initial_seed.sql", "GLOBALMART", "RAW"),
        ("5/7", "Creating Snowpipes...", "setup/05_create_snowpipes.sql", "GLOBALMART", "RAW"),
        ("6/7", "Creating streams and tasks...", "setup/06_create_streams_and_tasks.sql", "GLOBALMART", "RAW"),
        ("7/7", "Setting up GenAI infra...", "setup/07_genai_cortex.sql", "GLOBALMART", "GOLD"),
    ]

    for step, desc, sql_file, db, schema in scripts:
        print(f"\n[{step}] {desc}")
        filepath = os.path.join(script_dir, sql_file)
        run_sql_file(token, filepath, database=db, schema=schema)

    # Final verification
    print("\n" + "=" * 60)
    print("Verifying RAW table counts...")
    verify_sql = """
        SELECT 'RAW_CUSTOMERS_LOAD' AS tbl, COUNT(*) AS cnt FROM GLOBALMART.RAW.RAW_CUSTOMERS_LOAD
        UNION ALL SELECT 'RAW_ORDERS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_ORDERS_LOAD
        UNION ALL SELECT 'RAW_TRANSACTIONS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD
        UNION ALL SELECT 'RAW_RETURNS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_RETURNS_LOAD
        UNION ALL SELECT 'RAW_PRODUCTS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_PRODUCTS_LOAD
        UNION ALL SELECT 'RAW_VENDORS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_VENDORS_LOAD
    """
    data = execute_sql(token, verify_sql, database="GLOBALMART")
    for row in data.get("data", []):
        print(f"  {row[0]}: {row[1]} rows")

    print("\n" + "=" * 60)
    print("SQL setup complete!")
    print("Next: Use Snowflake-native dbt to run the dbt models.")
    print("=" * 60)

if __name__ == "__main__":
    main()
