#!/usr/bin/env python3
"""
GlobalMart: One-command deployment.

Usage:
    python setup/deploy.py              # Full deploy (setup + upload + seed + dbt)
    python setup/deploy.py --skip-dbt   # Setup only (no dbt run)
    python setup/deploy.py --genai      # Include GenAI setup + models

Prerequisites:
    pip install snowflake-connector-python dbt-snowflake

Credentials (pick one):
    export SNOWFLAKE_TOKEN="your-token"
    -- or --
    Create Connect-token-secret.txt in the project root
"""

import glob
import os
import re
import subprocess
import sys
import time

import snowflake.connector

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ACCOUNT = "chc70950.us-east-1"
USER = "SRINIVAS"
ROLE = "ACCOUNTADMIN"
DATABASE = "GLOBALMART"
SCHEMA = "RAW"
WAREHOUSE = "COMPUTE_WH"

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SETUP_DIR = os.path.join(PROJECT_ROOT, "setup")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# 16 source files: (local relative to data/, stage sub-path)
FILES = [
    ("Region 1/customers_1.csv", "Region1/"),
    ("Region 1/orders_1.csv", "Region1/"),
    ("Region 2/customers_2.csv", "Region2/"),
    ("Region 2/transactions_1.csv", "Region2/"),
    ("Region 3/customers_3.csv", "Region3/"),
    ("Region 3/orders_2.csv", "Region3/"),
    ("Region 4/customers_4.csv", "Region4/"),
    ("Region 4/transactions_2.csv", "Region4/"),
    ("Region 5/customers_5.csv", "Region5/"),
    ("Region 5/orders_3.csv", "Region5/"),
    ("Region 6/customers_6.csv", "Region6/"),
    ("Region 6/returns_1.json", "Region6/"),
    ("transactions_3.csv", ""),
    ("returns_2.json", ""),
    ("products.json", ""),
    ("vendors.csv", ""),
]

STAGE = "@GLOBALMART.RAW.SOURCE_DATA_STAGE"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_token():
    """Read token from env var or file."""
    token = os.environ.get("SNOWFLAKE_TOKEN", "").strip()
    if token:
        return token

    token_file = os.path.join(PROJECT_ROOT, "Connect-token-secret.txt")
    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            token = f.read().strip()
        if token:
            return token

    print("ERROR: No Snowflake token found.")
    print("  Option 1: export SNOWFLAKE_TOKEN='your-token'")
    print("  Option 2: Create Connect-token-secret.txt in the project root")
    sys.exit(1)


def connect(token):
    """Create Snowflake connection."""
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        role=ROLE,
        database=DATABASE,
        schema=SCHEMA,
        warehouse=WAREHOUSE,
        authenticator="programmatic_access_token",
        token=token,
    )


def split_sql_statements(content):
    """Split SQL file into individual statements, handling $$ blocks."""
    statements = []
    current = []
    in_dollar_block = False

    for line in content.split("\n"):
        stripped = line.strip()

        # Track $$ delimiters
        dollar_count = stripped.count("$$")
        if dollar_count % 2 == 1:
            in_dollar_block = not in_dollar_block

        current.append(line)

        # Statement ends at ; outside a $$ block
        if stripped.endswith(";") and not in_dollar_block:
            stmt = "\n".join(current).strip()
            # Skip pure comment/empty blocks
            non_comment = [
                l
                for l in stmt.split("\n")
                if l.strip() and not l.strip().startswith("--")
            ]
            if non_comment:
                statements.append(stmt)
            current = []

    # Handle trailing statement without semicolon
    if current:
        stmt = "\n".join(current).strip()
        non_comment = [
            l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")
        ]
        if non_comment:
            statements.append(stmt)

    return statements


def run_sql_file(cur, filepath, label):
    """Execute all statements in a SQL file."""
    print(f"\n[{label}] Running {os.path.basename(filepath)}...")

    with open(filepath, "r") as f:
        content = f.read()

    statements = split_sql_statements(content)
    for i, stmt in enumerate(statements):
        # Get first meaningful line for display
        lines = [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
        preview = lines[0].strip()[:70] if lines else "(empty)"

        try:
            cur.execute(stmt)
            print(f"  OK: {preview}")
        except Exception as e:
            err = str(e).split("\n")[0][:100]
            print(f"  ERR: {preview}")
            print(f"       {err}")


def upload_files(cur):
    """Upload 16 data files to internal stage via PUT."""
    print(f"\n[2/5] Uploading 16 data files to stage...")
    success = 0
    for local_rel, stage_path in FILES:
        local_abs = os.path.join(DATA_DIR, local_rel)
        if not os.path.exists(local_abs):
            print(f"  SKIP (not found): {local_rel}")
            continue

        local_uri = local_abs.replace("\\", "/")
        target = f"{STAGE}/{stage_path}" if stage_path else STAGE

        put_cmd = f"PUT 'file://{local_uri}' '{target}' AUTO_COMPRESS=FALSE OVERWRITE=TRUE"

        try:
            cur.execute(put_cmd)
            result = cur.fetchone()
            status = result[6] if result and len(result) > 6 else "OK"
            print(f"  {local_rel} -> {stage_path or '(root)'} [{status}]")
            success += 1
        except Exception as e:
            print(f"  FAILED: {local_rel} - {str(e)[:80]}")

    print(f"  {success}/16 files uploaded.")


def seed_data(cur):
    """Call the SEED_RAW_TABLES stored procedure."""
    print(f"\n[3/5] Seeding RAW tables...")
    cur.execute("CALL GLOBALMART.RAW.SEED_RAW_TABLES('full')")
    result = cur.fetchone()
    print(result[0])

    # Resume tasks
    print("\n  Resuming scheduled tasks...")
    cur.execute("ALTER TASK GLOBALMART.RAW.TRANSFORM_ON_NEW_DATA RESUME")
    cur.execute("ALTER TASK GLOBALMART.RAW.INGEST_NEW_FILES RESUME")
    print("  Tasks resumed (INGEST every 5 min -> TRANSFORM on new data).")


def verify_counts(cur):
    """Print RAW table row counts."""
    print("\n  Verification:")
    cur.execute("""
        SELECT 'RAW_CUSTOMERS_LOAD' AS t, COUNT(*) FROM GLOBALMART.RAW.RAW_CUSTOMERS_LOAD
        UNION ALL SELECT 'RAW_ORDERS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_ORDERS_LOAD
        UNION ALL SELECT 'RAW_TRANSACTIONS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD
        UNION ALL SELECT 'RAW_RETURNS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_RETURNS_LOAD
        UNION ALL SELECT 'RAW_PRODUCTS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_PRODUCTS_LOAD
        UNION ALL SELECT 'RAW_VENDORS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_VENDORS_LOAD
    """)
    for row in cur.fetchall():
        print(f"    {row[0]}: {row[1]} rows")


def run_dbt():
    """Run dbt deps, run, and test from the project root."""
    print(f"\n[4/5] Running dbt...")

    # Ensure SNOWFLAKE_TOKEN is available for dbt
    if "SNOWFLAKE_TOKEN" not in os.environ:
        token_file = os.path.join(PROJECT_ROOT, "Connect-token-secret.txt")
        if os.path.exists(token_file):
            with open(token_file, "r") as f:
                os.environ["SNOWFLAKE_TOKEN"] = f.read().strip()

    commands = [
        ["dbt", "deps"],
        ["dbt", "run", "--full-refresh"],
        ["dbt", "test"],
    ]

    for cmd in commands:
        print(f"\n  $ {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=False)
        if result.returncode != 0:
            print(f"  WARNING: '{' '.join(cmd)}' exited with code {result.returncode}")
            return False

    return True


def run_genai(cur):
    """Execute GenAI setup SQL and dbt models."""
    print(f"\n[5/5] Setting up GenAI...")
    genai_file = os.path.join(SETUP_DIR, "genai_setup.sql")
    if os.path.exists(genai_file):
        run_sql_file(cur, genai_file, "5/5")
    else:
        print("  genai_setup.sql not found, skipping.")
        return

    print("\n  Running GenAI dbt models...")
    result = subprocess.run(
        ["dbt", "run", "--select", "genai"],
        cwd=PROJECT_ROOT,
        capture_output=False,
    )
    if result.returncode != 0:
        print("  WARNING: GenAI dbt models had errors.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    skip_dbt = "--skip-dbt" in sys.argv
    include_genai = "--genai" in sys.argv

    print("=" * 55)
    print("  GlobalMart - One-Command Deployment")
    print("=" * 55)

    token = get_token()
    conn = connect(token)
    cur = conn.cursor()

    # Verify connection
    cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
    row = cur.fetchone()
    print(f"\n  Connected: {row[0]} | {row[1]} | {row[2]}")

    start = time.time()

    # Step 1: Create all Snowflake objects
    run_sql_file(cur, os.path.join(SETUP_DIR, "snowflake_setup.sql"), "1/5")

    # Step 2: Upload data files to stage
    upload_files(cur)

    # Step 3: Seed RAW tables + resume tasks
    seed_data(cur)
    verify_counts(cur)

    # Step 4: Run dbt
    if not skip_dbt:
        run_dbt()
    else:
        print("\n[4/5] Skipped (--skip-dbt)")

    # Step 5: GenAI (optional)
    if include_genai:
        run_genai(cur)
    else:
        print("\n[5/5] Skipped GenAI (use --genai to include)")

    elapsed = time.time() - start

    print()
    print("=" * 55)
    print(f"  Deployment complete ({elapsed:.0f}s)")
    print("=" * 55)
    print()
    print("  Objects created:")
    print("    - 6 RAW landing tables (VARIANT)")
    print("    - 6 Bronze models (incremental)")
    print("    - 14 Silver models (6 ephemeral + 6 clean + rejected + audit)")
    print("    - 10 Gold models (4 dims + 2 facts + 1 bridge + 3 MVs)")
    print("    - 6 Metrics views")
    if include_genai:
        print("    - 4 GenAI models")
    print("    - Total: 40 data objects")
    print()
    print("  Automation:")
    print("    - INGEST_NEW_FILES task (every 5 min)")
    print("    - TRANSFORM_ON_NEW_DATA task (on new data)")
    print()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
