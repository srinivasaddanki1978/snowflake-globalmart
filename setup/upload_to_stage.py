"""
GlobalMart: Upload 16 source files to Snowflake internal stage.

Usage:
    pip install snowflake-connector-python
    python setup/upload_to_stage.py

Reads the token from Connect-token-secret.txt (project root) or
the SNOWFLAKE_TOKEN environment variable.

Run this from the project root directory:
    cd C:\Srinivas\project\snowflake-globalmart
    python setup/upload_to_stage.py
"""

import os
import sys
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
STAGE = "@GLOBALMART.RAW.SOURCE_DATA_STAGE"

# Project root = parent of setup/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# 16 files: (local_path_relative_to_data, stage_subpath)
FILES = [
    # Region 1
    ("Region 1/customers_1.csv",     "Region1/"),
    ("Region 1/orders_1.csv",        "Region1/"),
    # Region 2
    ("Region 2/customers_2.csv",     "Region2/"),
    ("Region 2/transactions_1.csv",  "Region2/"),
    # Region 3
    ("Region 3/customers_3.csv",     "Region3/"),
    ("Region 3/orders_2.csv",        "Region3/"),
    # Region 4
    ("Region 4/customers_4.csv",     "Region4/"),
    ("Region 4/transactions_2.csv",  "Region4/"),
    # Region 5
    ("Region 5/customers_5.csv",     "Region5/"),
    ("Region 5/orders_3.csv",        "Region5/"),
    # Region 6
    ("Region 6/customers_6.csv",     "Region6/"),
    ("Region 6/returns_1.json",      "Region6/"),
    # Root-level files
    ("transactions_3.csv",           ""),
    ("returns_2.json",               ""),
    ("products.json",                ""),
    ("vendors.csv",                  ""),
]


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

    print("ERROR: No token found.")
    print("  Set SNOWFLAKE_TOKEN environment variable, or")
    print("  Place your token in Connect-token-secret.txt at the project root.")
    sys.exit(1)


def main():
    token = get_token()

    print(f"Connecting to {ACCOUNT} as {USER}...")
    conn = snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        role=ROLE,
        database=DATABASE,
        schema=SCHEMA,
        warehouse=WAREHOUSE,
        authenticator="programmatic_access_token",
        token=token,
    )
    cur = conn.cursor()

    # Ensure stage exists
    print("Creating stage (if not exists)...")
    cur.execute(
        "CREATE STAGE IF NOT EXISTS GLOBALMART.RAW.SOURCE_DATA_STAGE "
        "COMMENT = 'Landing zone for GlobalMart regional source files'"
    )

    # Upload each file
    success = 0
    failed = 0
    for local_rel, stage_path in FILES:
        local_abs = os.path.join(DATA_DIR, local_rel)

        if not os.path.exists(local_abs):
            print(f"  SKIP (not found): {local_rel}")
            failed += 1
            continue

        # PUT command: file:// prefix + forward slashes
        local_uri = local_abs.replace("\\", "/")
        target = f"{STAGE}/{stage_path}" if stage_path else STAGE

        put_cmd = (
            f"PUT 'file://{local_uri}' '{target}' "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )

        print(f"  Uploading: {local_rel} -> {stage_path or '(root)'}...", end=" ")
        try:
            cur.execute(put_cmd)
            result = cur.fetchone()
            status = result[6] if result and len(result) > 6 else "UNKNOWN"
            print(f"[{status}]")
            success += 1
        except Exception as e:
            print(f"[FAILED: {e}]")
            failed += 1

    # Verify
    print(f"\nUpload complete: {success} succeeded, {failed} failed")
    print("\nFiles on stage:")
    cur.execute(f"LIST {STAGE}")
    rows = cur.fetchall()
    for row in rows:
        print(f"  {row[0]}  ({row[1]} bytes)")
    print(f"\nTotal files on stage: {len(rows)}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
