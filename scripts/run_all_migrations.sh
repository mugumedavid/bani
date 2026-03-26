#!/usr/bin/env bash
# ============================================================================
# Run All Migration Scripts — Tests every connector pair
# ============================================================================
# Prerequisites:
#   docker compose up -d postgres mysql mssql oracle
#
# Credentials (defaults matching docker-compose.yml):
#   export PG_USER=bani_test       PG_PASS=bani_test
#   export MYSQL_USER=bani_test    MYSQL_PASS=bani_test
#   export MSSQL_USER=sa           MSSQL_PASS='BaniTest123!'
#   export ORACLE_USER=bani_test   ORACLE_PASS=bani_test
#
# SQLite scripts need no Docker or credentials.
# ============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PASS=0
FAIL=0
SKIP=0
FAILED_SCRIPTS=()

# Default credentials if not set
export PG_USER="${PG_USER:-bani_test}"
export PG_PASS="${PG_PASS:-bani_test}"
export MYSQL_USER="${MYSQL_USER:-bani_test}"
export MYSQL_PASS="${MYSQL_PASS:-bani_test}"
export MSSQL_USER="${MSSQL_USER:-sa}"
export MSSQL_PASS="${MSSQL_PASS:-BaniTest123!}"
export ORACLE_USER="${ORACLE_USER:-bani_test}"
export ORACLE_PASS="${ORACLE_PASS:-bani_test}"

echo ""
echo "============================================"
echo "  Bani — Run All Migration Scripts"
echo "============================================"
echo ""

# Collect all numbered migration scripts (01_migrate_*.py .. 20_migrate_*.py)
# Sorted numerically by the prefix so they run in complexity order
SCRIPTS=$(find "$SCRIPT_DIR" -name '[0-9][0-9]_migrate_*_to_*.py' | sort)

TOTAL=$(echo "$SCRIPTS" | wc -l | tr -d ' ')
echo "Found $TOTAL migration scripts to run."
echo ""

IDX=0
for script in $SCRIPTS; do
    IDX=$((IDX + 1))
    name=$(basename "$script")
    echo "[$IDX/$TOTAL] $name"
    echo "  Running..."

    if python "$script" 2>&1 | tail -5 | sed 's/^/  | /'; then
        echo "  ✓ PASSED"
        PASS=$((PASS + 1))
    else
        echo "  ✗ FAILED"
        FAIL=$((FAIL + 1))
        FAILED_SCRIPTS+=("$name")
    fi
    echo ""
done

echo "============================================"
echo "  Results"
echo "============================================"
echo "  Total:  $TOTAL"
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo ""

if [ ${#FAILED_SCRIPTS[@]} -gt 0 ]; then
    echo "Failed scripts:"
    for s in "${FAILED_SCRIPTS[@]}"; do
        echo "  ✗ $s"
    done
    echo ""
fi

if [ "$FAIL" -eq 0 ]; then
    echo "All migrations passed! ✓"
    exit 0
else
    echo "Some migrations failed. See above for details."
    exit 1
fi
