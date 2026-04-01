#!/usr/bin/env bash
# Bani Docker Smoke Test
# ======================
# Starts all services via docker-compose, seeds a PostgreSQL table,
# runs a PG→MSSQL migration via the Bani container, and verifies
# the row count on the target.
#
# Usage:
#   ./scripts/docker/smoke_test.sh
#
# Prerequisites:
#   - Docker and docker-compose installed
#   - Run from the repo root

set -euo pipefail

COMPOSE="docker compose"
PROJECT="bani-smoke"

cleanup() {
    echo "==> Cleaning up..."
    $COMPOSE -p "$PROJECT" down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

echo "==> Starting database services..."
$COMPOSE -p "$PROJECT" up -d postgres mssql
echo "==> Waiting for databases to be healthy..."
$COMPOSE -p "$PROJECT" up -d --wait postgres mssql

# Seed PostgreSQL with test data
echo "==> Seeding PostgreSQL..."
$COMPOSE -p "$PROJECT" exec -T postgres psql -U bani_test -d bani_test <<'SQL'
DROP TABLE IF EXISTS smoke_test;
CREATE TABLE smoke_test (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE
);
INSERT INTO smoke_test (name, value, active)
SELECT
    'row_' || i,
    random() * 1000,
    (i % 3 != 0)
FROM generate_series(1, 1000) AS i;
SQL

# Create MSSQL target database
echo "==> Creating MSSQL target database..."
$COMPOSE -p "$PROJECT" exec -T mssql /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "BaniTest123!" -C \
    -Q "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name='smoke_test') CREATE DATABASE smoke_test"

# Create BDL project file
echo "==> Creating BDL project..."
BDL_CONTENT='<?xml version="1.0" encoding="UTF-8"?>
<bani schemaVersion="1.0">
  <project name="smoke-test" description="Docker smoke test"/>
  <source connector="postgresql">
    <connection
      host="postgres"
      port="5432"
      database="bani_test"
      username="${env:PG_USER}"
      password="${env:PG_PASS}"
    />
  </source>
  <target connector="mssql">
    <connection
      host="mssql"
      port="1433"
      database="smoke_test"
      username="${env:MSSQL_USER}"
      password="${env:MSSQL_PASS}"
    />
  </target>
  <tables>
    <table sourceName="smoke_test"/>
  </tables>
</bani>'

# Build bani image
echo "==> Building Bani image..."
# First build the UI
(cd ui && npm run build 2>/dev/null) || echo "UI build skipped (node not available)"
$COMPOSE -p "$PROJECT" build bani

# Run migration
echo "==> Running migration..."
$COMPOSE -p "$PROJECT" run --rm \
    -e PG_USER=bani_test \
    -e PG_PASS=bani_test \
    -e MSSQL_USER=sa \
    -e MSSQL_PASS="BaniTest123!" \
    bani sh -c "echo '$BDL_CONTENT' > /tmp/smoke.bdl && bani run /tmp/smoke.bdl --output json"

# Verify row count on MSSQL
echo "==> Verifying target row count..."
ROW_COUNT=$($COMPOSE -p "$PROJECT" exec -T mssql /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P "BaniTest123!" -C \
    -d smoke_test \
    -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM dbo.smoke_test" \
    -h -1 -W | tr -d '[:space:]')

echo "==> Target row count: $ROW_COUNT"

if [ "$ROW_COUNT" -eq 1000 ]; then
    echo ""
    echo "========================================="
    echo "  SMOKE TEST PASSED: 1000 rows migrated"
    echo "========================================="
    exit 0
else
    echo ""
    echo "========================================="
    echo "  SMOKE TEST FAILED: expected 1000, got $ROW_COUNT"
    echo "========================================="
    exit 1
fi
