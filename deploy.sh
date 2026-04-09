#!/bin/bash
set -e

echo "=== GlobalMart Snowflake + dbt Deployment ==="

# Read token if available
if [ -f "Connect-token-secret.txt" ]; then
    export SNOWFLAKE_TOKEN=$(cat Connect-token-secret.txt)
    echo "Token loaded from Connect-token-secret.txt"
fi

export SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT:-chc70950.us-east-1}"
export SNOWFLAKE_ROLE="${SNOWFLAKE_ROLE:-ACCOUNTADMIN}"
export SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-COMPUTE_WH}"
export SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-GLOBALMART}"

# Step 1: Run setup SQL scripts
echo "[1/7] Creating infrastructure..."
snowsql -f setup/01_create_infrastructure.sql

echo "[2/7] Creating stage and uploading files..."
snowsql -f setup/02_create_stage_and_upload.sql

echo "[3/7] Creating landing tables..."
snowsql -f setup/03_create_landing_tables.sql

echo "[4/7] Running initial data seed..."
snowsql -f setup/04_initial_seed.sql

echo "[5/7] Creating Snowpipes..."
snowsql -f setup/05_create_snowpipes.sql

echo "[6/7] Creating streams and tasks..."
snowsql -f setup/06_create_streams_and_tasks.sql

# Step 2: Run dbt
echo "[7/7] Running dbt..."
cd globalmart_dbt
dbt deps
dbt run --full-refresh
dbt test

# Step 3: GenAI setup + run
echo "[8/8] Setting up GenAI..."
cd ..
snowsql -f setup/07_genai_cortex.sql
cd globalmart_dbt
dbt run --select genai

echo ""
echo "=== Deployment Complete ==="
echo "Objects created:"
echo "  - 6 RAW landing tables"
echo "  - 6 Bronze models (incremental)"
echo "  - 14 Silver models (6 ephemeral + 6 clean + rejected_records + audit_log)"
echo "  - 10 Gold models (4 dims + 2 facts + 1 bridge + 3 MVs)"
echo "  - 6 Metrics views"
echo "  - 4 GenAI models"
echo "  - Total: 40 data objects"
