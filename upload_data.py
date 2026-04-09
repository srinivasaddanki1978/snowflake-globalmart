#!/usr/bin/env python3
"""Upload data files to Snowflake RAW tables via REST API INSERT statements."""

import requests
import json
import csv
import os

BASE_URL = "https://chc70950.us-east-1.snowflakecomputing.com"

def load_token():
    token_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Connect-token-secret.txt")
    with open(token_file, "r") as f:
        return f.read().strip()

TOKEN = load_token()
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

def run_sql(stmt):
    payload = {
        "statement": stmt,
        "timeout": 120,
        "warehouse": "COMPUTE_WH",
        "role": "ACCOUNTADMIN",
        "database": "GLOBALMART",
        "schema": "RAW",
    }
    resp = requests.post(f"{BASE_URL}/api/v2/statements", headers=HEADERS, json=payload, timeout=60)
    return resp.json()


def upload_csv(filepath, table_name):
    """Read CSV file and INSERT rows as VARIANT via SELECT PARSE_JSON FROM VALUES."""
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return 0

    total = 0
    batch_size = 50
    # Get relative source path
    parts = filepath.replace("\\", "/")
    idx = parts.find("data/")
    source_file = parts[idx:] if idx >= 0 else os.path.basename(filepath)

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = []
        for row in batch:
            # Convert CSV row dict to JSON, preserving nulls for empty values
            clean = {}
            for k, v in row.items():
                if v is None or v == "":
                    clean[k] = None
                else:
                    clean[k] = v
            json_str = json.dumps(clean).replace("'", "''")
            src_esc = source_file.replace("'", "''")
            values.append(f"('{json_str}', '{src_esc}')")

        insert_sql = (
            f"INSERT INTO GLOBALMART.RAW.{table_name} (raw_data, _source_file, _load_timestamp) "
            f"SELECT PARSE_JSON(column1), column2, CURRENT_TIMESTAMP() "
            f"FROM VALUES " + ", ".join(values)
        )
        result = run_sql(insert_sql)
        msg = result.get("message", "")
        if "error" not in msg.lower() and result.get("statementStatusUrl"):
            total += len(batch)
        elif "data" in result:
            total += len(batch)
        else:
            print(f"    ERROR batch {i}: {msg[:100]}")

    return total


def upload_json(filepath, table_name):
    """Read JSON file and INSERT rows as VARIANT via SELECT PARSE_JSON FROM VALUES."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        data = [data]

    total = 0
    batch_size = 20  # Smaller batches for JSON (larger payloads)
    parts = filepath.replace("\\", "/")
    idx = parts.find("data/")
    source_file = parts[idx:] if idx >= 0 else os.path.basename(filepath)

    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        values = []
        for row in batch:
            json_str = json.dumps(row).replace("'", "''")
            src_esc = source_file.replace("'", "''")
            values.append(f"('{json_str}', '{src_esc}')")

        insert_sql = (
            f"INSERT INTO GLOBALMART.RAW.{table_name} (raw_data, _source_file, _load_timestamp) "
            f"SELECT PARSE_JSON(column1), column2, CURRENT_TIMESTAMP() "
            f"FROM VALUES " + ", ".join(values)
        )
        result = run_sql(insert_sql)
        msg = result.get("message", "")
        if "error" not in msg.lower() and result.get("statementStatusUrl"):
            total += len(batch)
        elif "data" in result:
            total += len(batch)
        else:
            print(f"    ERROR batch {i}: {msg[:100]}")

    return total


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data")

    print("=" * 50)
    print("Uploading data files to Snowflake RAW tables")
    print("=" * 50)

    # Customers (6 CSV files)
    print("\nCustomers:")
    for region in range(1, 7):
        fp = os.path.join(data_dir, f"Region {region}", f"customers_{region}.csv")
        if os.path.exists(fp):
            n = upload_csv(fp, "RAW_CUSTOMERS_LOAD")
            print(f"  Region {region}: {n} rows")
        else:
            print(f"  Region {region}: file not found")

    # Orders (3 CSV files)
    print("\nOrders:")
    for region, num in [(1, 1), (3, 2), (5, 3)]:
        fp = os.path.join(data_dir, f"Region {region}", f"orders_{num}.csv")
        if os.path.exists(fp):
            n = upload_csv(fp, "RAW_ORDERS_LOAD")
            print(f"  Region {region}: {n} rows")

    # Transactions (3 CSV files)
    print("\nTransactions:")
    tx_files = [
        (os.path.join(data_dir, "Region 2", "transactions_1.csv"), "Region 2"),
        (os.path.join(data_dir, "Region 4", "transactions_2.csv"), "Region 4"),
        (os.path.join(data_dir, "transactions_3.csv"), "root"),
    ]
    for fp, label in tx_files:
        if os.path.exists(fp):
            n = upload_csv(fp, "RAW_TRANSACTIONS_LOAD")
            print(f"  {label}: {n} rows")

    # Returns (2 JSON files)
    print("\nReturns:")
    ret_files = [
        (os.path.join(data_dir, "Region 6", "returns_1.json"), "Region 6"),
        (os.path.join(data_dir, "returns_2.json"), "root"),
    ]
    for fp, label in ret_files:
        if os.path.exists(fp):
            n = upload_json(fp, "RAW_RETURNS_LOAD")
            print(f"  {label}: {n} rows")

    # Products (1 JSON file)
    print("\nProducts:")
    fp = os.path.join(data_dir, "products.json")
    if os.path.exists(fp):
        n = upload_json(fp, "RAW_PRODUCTS_LOAD")
        print(f"  products.json: {n} rows")

    # Vendors (1 CSV file)
    print("\nVendors:")
    fp = os.path.join(data_dir, "vendors.csv")
    if os.path.exists(fp):
        n = upload_csv(fp, "RAW_VENDORS_LOAD")
        print(f"  vendors.csv: {n} rows")

    # Final counts
    print("\n" + "=" * 50)
    print("Final RAW table counts:")
    result = run_sql(
        """
        SELECT 'RAW_CUSTOMERS_LOAD' AS tbl, COUNT(*) AS cnt FROM GLOBALMART.RAW.RAW_CUSTOMERS_LOAD
        UNION ALL SELECT 'RAW_ORDERS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_ORDERS_LOAD
        UNION ALL SELECT 'RAW_TRANSACTIONS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_TRANSACTIONS_LOAD
        UNION ALL SELECT 'RAW_RETURNS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_RETURNS_LOAD
        UNION ALL SELECT 'RAW_PRODUCTS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_PRODUCTS_LOAD
        UNION ALL SELECT 'RAW_VENDORS_LOAD', COUNT(*) FROM GLOBALMART.RAW.RAW_VENDORS_LOAD
    """
    )
    for row in result.get("data", []):
        print(f"  {row[0]}: {row[1]} rows")
    print("=" * 50)


if __name__ == "__main__":
    main()
