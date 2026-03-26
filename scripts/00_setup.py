#!/usr/bin/env python3
"""Set up the MySQL source database for Bani migration scripts (01–20).

Seeds MySQL with the reference 5-table e-commerce schema plus a
data_types_showcase table exercising every commonly used MySQL data type.
Run this once before running any migration script.

Prerequisites:
    docker compose up -d mysql postgres mssql oracle

Environment variables (all scripts):
    export MYSQL_USER=bani_test  MYSQL_PASS='bani_test'
    export PG_USER=bani_test     PG_PASS='bani_test'
    export MSSQL_USER=sa         MSSQL_PASS='BaniTest123!'
    export ORACLE_USER=bani_test ORACLE_PASS='bani_test'

Usage:
    python scripts/00_setup.py

Script → required containers → env vars:
    00  (this)  Setup          mysql              MYSQL_*
    --- MySQL → others ---
    01  MySQL   → PG           mysql postgres     MYSQL_* PG_*
    02  MySQL   → MSSQL        mysql mssql        MYSQL_* MSSQL_*
    03  MySQL   → Oracle       mysql oracle        MYSQL_* ORACLE_*
    04  MySQL   → SQLite       mysql              MYSQL_*
    --- PG / MSSQL / Oracle cross-migrations ---
    05  PG      → MSSQL        postgres mssql     PG_* MSSQL_*
    06  PG      → Oracle       postgres oracle     PG_* ORACLE_*
    07  MSSQL   → PG           mssql postgres     MSSQL_* PG_*
    08  MSSQL   → Oracle       mssql oracle        MSSQL_* ORACLE_*
    09  Oracle  → PG           oracle postgres     ORACLE_* PG_*
    10  Oracle  → MSSQL        oracle mssql        ORACLE_* MSSQL_*
    --- Back to MySQL ---
    11  PG      → MySQL        postgres mysql     PG_* MYSQL_*
    12  MSSQL   → MySQL        mssql mysql        MSSQL_* MYSQL_*
    13  Oracle  → MySQL        oracle mysql        ORACLE_* MYSQL_*
    --- From SQLite ---
    14  SQLite  → PG           postgres           PG_*
    15  SQLite  → MySQL        mysql              MYSQL_*
    16  SQLite  → MSSQL        mssql              MSSQL_*
    17  SQLite  → Oracle       oracle              ORACLE_*
    --- To SQLite ---
    18  PG      → SQLite       postgres           PG_*
    19  MSSQL   → SQLite       mssql              MSSQL_*
    20  Oracle  → SQLite       oracle              ORACLE_*
"""
from __future__ import annotations

import os
import sys

import pymysql

# ═══════════════════════════════════════════════════════════════════════════
# DDL — drop in reverse-dependency order, create in dependency order
# ═══════════════════════════════════════════════════════════════════════════

DROP_TABLES = [
    "order_items",
    "orders",
    "products",
    "customers",
    "categories",
    "data_types_showcase",
]

CREATE_ECOMMERCE = """
-- Categories
CREATE TABLE categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Products
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    sku CHAR(12) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    weight_kg DOUBLE,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    metadata JSON,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES categories(id),
    UNIQUE INDEX idx_products_sku (sku),
    INDEX idx_products_category (category_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Customers
CREATE TABLE customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(200) NOT NULL,
    notes TEXT,
    registered_at DATE NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Orders
CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(12,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customers(id),
    INDEX idx_orders_customer (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Order items (junction)
CREATE TABLE order_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    CONSTRAINT fk_oi_order FOREIGN KEY (order_id) REFERENCES orders(id),
    CONSTRAINT fk_oi_product FOREIGN KEY (product_id) REFERENCES products(id),
    INDEX idx_order_items_order (order_id),
    INDEX idx_order_items_product (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_SHOWCASE = """
-- Showcase table: one column per commonly used MySQL type
CREATE TABLE data_types_showcase (
    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Integer family
    col_tinyint         TINYINT,
    col_tinyint_u       TINYINT UNSIGNED,
    col_smallint        SMALLINT,
    col_smallint_u      SMALLINT UNSIGNED,
    col_mediumint       MEDIUMINT,
    col_mediumint_u     MEDIUMINT UNSIGNED,
    col_int             INT,
    col_int_u           INT UNSIGNED,
    col_bigint          BIGINT,
    col_bigint_u        BIGINT UNSIGNED,

    -- Fixed / floating point
    col_decimal         DECIMAL(18,4),
    col_float           FLOAT,
    col_double          DOUBLE,

    -- Bit & boolean
    col_bit1            BIT(1),
    col_bit8            BIT(8),
    col_bool            BOOLEAN,

    -- Date & time family
    col_date            DATE,
    col_time            TIME,
    col_datetime        DATETIME,
    col_timestamp       TIMESTAMP NULL,
    col_year            YEAR,

    -- Character strings
    col_char            CHAR(36),
    col_varchar         VARCHAR(500),
    col_tinytext        TINYTEXT,
    col_text            TEXT,
    col_mediumtext      MEDIUMTEXT,
    col_longtext        LONGTEXT,

    -- Binary strings
    col_binary          BINARY(16),
    col_varbinary       VARBINARY(255),
    col_tinyblob        TINYBLOB,
    col_blob            BLOB,
    col_mediumblob      MEDIUMBLOB,
    col_longblob        LONGBLOB,

    -- Enum & set
    col_enum            ENUM('small', 'medium', 'large', 'extra-large'),
    col_set             SET('read', 'write', 'execute', 'admin'),

    -- JSON
    col_json            JSON
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ═══════════════════════════════════════════════════════════════════════════
# DML — e-commerce data (matches conftest.py fixtures exactly)
# ═══════════════════════════════════════════════════════════════════════════

INSERT_ECOMMERCE = [
    """INSERT INTO categories (id, name, description) VALUES
        (1, 'Electronics', 'Consumer electronics and gadgets'),
        (2, '日本語カテゴリ', NULL),
        (3, 'Émojis & Spëcial ✨', '')""",
    """INSERT INTO products
        (id, category_id, name, sku, price, weight_kg, is_active, metadata)
    VALUES
        (1, 1, 'Laptop Pro 16"', 'LAPTOP-PRO16', 2499.99, 2.1, 1,
         '{"brand": "TechCo", "specs": {"ram": 32}}'),
        (2, 1, 'USB-C Cable', 'USBC-CABLE01', 0.01, 0.05, 1, NULL),
        (3, 2, '抹茶セット', 'MATCHA-SET01', 9999999.99, NULL, 1, '{"origin": "京都"}'),
        (4, 3, 'Emoji Product 🎉', 'EMOJI-PROD01', 0.00, 0.0, 0, '{}')""",
    """INSERT INTO customers (id, email, full_name, notes, registered_at) VALUES
        (1, 'alice@example.com', 'Alice Müller', 'VIP customer', '2020-01-15'),
        (2, 'bob@例え.jp', '田中太郎', NULL, '2024-12-31'),
        (3, 'charlie@test.com', 'Charlie O''Brien', '', '2000-01-01')""",
    """INSERT INTO orders (id, customer_id, order_date, total_amount, status) VALUES
        (1, 1, '2024-06-15 10:30:00', 2500.00, 'completed'),
        (2, 2, '2024-12-31 23:59:59', 9999999.99, 'pending'),
        (3, 3, '2024-01-01 00:00:00', 0.01, 'shipped')""",
    """INSERT INTO order_items (id, order_id, product_id, quantity, unit_price) VALUES
        (1, 1, 1, 1, 2499.99),
        (2, 1, 2, 1, 0.01),
        (3, 2, 3, 999999, 9999999.99),
        (4, 3, 4, 1, 0.00)""",
]

# ═══════════════════════════════════════════════════════════════════════════
# DML — data_types_showcase rows
# ═══════════════════════════════════════════════════════════════════════════

INSERT_SHOWCASE = [
    # Row 1: typical mid-range values
    """INSERT INTO data_types_showcase (
        col_tinyint, col_tinyint_u, col_smallint, col_smallint_u,
        col_mediumint, col_mediumint_u, col_int, col_int_u,
        col_bigint, col_bigint_u,
        col_decimal, col_float, col_double,
        col_bit1, col_bit8, col_bool,
        col_date, col_time, col_datetime, col_timestamp, col_year,
        col_char, col_varchar, col_tinytext, col_text, col_mediumtext, col_longtext,
        col_binary, col_varbinary, col_tinyblob, col_blob, col_mediumblob, col_longblob,
        col_enum, col_set, col_json
    ) VALUES (
        42, 200, 1000, 50000,
        100000, 8000000, 2147483647, 4294967295,
        9223372036854775807, 18446744073709551615,
        12345.6789, 3.14, 2.718281828459045,
        b'1', b'11001010', TRUE,
        '2024-06-15', '14:30:59', '2024-06-15 14:30:59', '2024-06-15 14:30:59', 2024,
        'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
        'The quick brown fox jumps over the lazy dog',
        'tiny text value',
        'Regular text content with Unicode: café résumé naïve',
        'Medium text with CJK: 你好世界 こんにちは세계',
        'Long text — this could be megabytes in production',
        UNHEX('0102030405060708090A0B0C0D0E0F10'),
        UNHEX('DEADBEEF'),
        X'48656C6C6F',
        X'576F726C6421',
        X'CAFEBABE',
        X'00FF00FF',
        'medium', 'read,write', '{"key": "value", "nested": {"n": 1}}'
    )""",

    # Row 2: minimum / boundary values
    """INSERT INTO data_types_showcase (
        col_tinyint, col_tinyint_u, col_smallint, col_smallint_u,
        col_mediumint, col_mediumint_u, col_int, col_int_u,
        col_bigint, col_bigint_u,
        col_decimal, col_float, col_double,
        col_bit1, col_bit8, col_bool,
        col_date, col_time, col_datetime, col_timestamp, col_year,
        col_char, col_varchar, col_tinytext, col_text, col_mediumtext, col_longtext,
        col_binary, col_varbinary, col_tinyblob, col_blob, col_mediumblob, col_longblob,
        col_enum, col_set, col_json
    ) VALUES (
        -128, 0, -32768, 0,
        -8388608, 0, -2147483648, 0,
        -9223372036854775808, 0,
        -99999999999999.9999, -3.402E+38, -1.7976931348623157E+308,
        b'0', b'00000000', FALSE,
        '1000-01-01', '-838:59:59', '1000-01-01 00:00:00', '1970-01-01 00:00:01', 1901,
        '', '', '', '', '', '',
        UNHEX('00000000000000000000000000000000'),
        X'00',
        X'',
        X'',
        X'',
        X'',
        'small', 'read', '[]'
    )""",

    # Row 3: maximum / boundary values
    """INSERT INTO data_types_showcase (
        col_tinyint, col_tinyint_u, col_smallint, col_smallint_u,
        col_mediumint, col_mediumint_u, col_int, col_int_u,
        col_bigint, col_bigint_u,
        col_decimal, col_float, col_double,
        col_bit1, col_bit8, col_bool,
        col_date, col_time, col_datetime, col_timestamp, col_year,
        col_char, col_varchar, col_tinytext, col_text, col_mediumtext, col_longtext,
        col_binary, col_varbinary, col_tinyblob, col_blob, col_mediumblob, col_longblob,
        col_enum, col_set, col_json
    ) VALUES (
        127, 255, 32767, 65535,
        8388607, 16777215, 2147483647, 4294967295,
        9223372036854775807, 18446744073709551615,
        99999999999999.9999, 3.402E+38, 1.7976931348623157E+308,
        b'1', b'11111111', TRUE,
        '9999-12-31', '838:59:59', '9999-12-31 23:59:59', '2038-01-19 03:14:07', 2155,
        'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ',
        'Ünïcödé strîng with émojis 🚀🌍🎶 and symbols ©®™±§',
        'Tiny: 日本語テスト',
        'Text with newlines:\nLine 2\nLine 3\tTabbed',
        REPEAT('M', 1000),
        REPEAT('L', 2000),
        UNHEX('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'),
        UNHEX('DEADBEEFCAFEBABE0123456789ABCDEF'),
        REPEAT(X'FF', 100),
        REPEAT(X'AB', 200),
        REPEAT(X'CD', 300),
        REPEAT(X'EF', 400),
        'extra-large', 'read,write,execute,admin',
        '{"emoji": "🎉", "cjk": "漢字", "array": [1, 2, 3], "bool": true, "null_val": null}'
    )""",

    # Row 4: all NULLs (except the auto-increment PK)
    """INSERT INTO data_types_showcase (
        col_tinyint, col_tinyint_u, col_smallint, col_smallint_u,
        col_mediumint, col_mediumint_u, col_int, col_int_u,
        col_bigint, col_bigint_u,
        col_decimal, col_float, col_double,
        col_bit1, col_bit8, col_bool,
        col_date, col_time, col_datetime, col_timestamp, col_year,
        col_char, col_varchar, col_tinytext, col_text, col_mediumtext, col_longtext,
        col_binary, col_varbinary, col_tinyblob, col_blob, col_mediumblob, col_longblob,
        col_enum, col_set, col_json
    ) VALUES (
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL,
        NULL, NULL, NULL,
        NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL
    )""",

    # Row 5: zeros, empty-ish, and special float values
    """INSERT INTO data_types_showcase (
        col_tinyint, col_tinyint_u, col_smallint, col_smallint_u,
        col_mediumint, col_mediumint_u, col_int, col_int_u,
        col_bigint, col_bigint_u,
        col_decimal, col_float, col_double,
        col_bit1, col_bit8, col_bool,
        col_date, col_time, col_datetime, col_timestamp, col_year,
        col_char, col_varchar, col_tinytext, col_text, col_mediumtext, col_longtext,
        col_binary, col_varbinary, col_tinyblob, col_blob, col_mediumblob, col_longblob,
        col_enum, col_set, col_json
    ) VALUES (
        0, 0, 0, 0,
        0, 0, 0, 0,
        0, 0,
        0.0000, 0.0, 0.0,
        b'0', b'00000000', FALSE,
        '2000-02-29', '00:00:00', '2000-02-29 00:00:00', '2000-02-29 00:00:00', 2000,
        ' ', ' ', ' ', ' ', ' ', ' ',
        UNHEX('20202020202020202020202020202020'),
        X'20',
        X'20',
        X'20',
        X'20',
        X'20',
        'large', '', '{"empty_obj": {}, "empty_arr": [], "zero": 0, "false": false}'
    )""",
]

EXPECTED = {
    "categories": 3,
    "products": 4,
    "customers": 3,
    "orders": 3,
    "order_items": 4,
    "data_types_showcase": 5,
}


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════


def _exec_stmts(cur: pymysql.cursors.Cursor, sql: str) -> None:
    """Split a multi-statement SQL string on ';' and execute each."""
    for stmt in sql.split(";"):
        # Strip leading SQL comment lines (-- ...) so the real DDL remains
        lines = [ln for ln in stmt.splitlines() if not ln.strip().startswith("--")]
        stmt = "\n".join(lines).strip()
        if stmt:
            cur.execute(stmt)


def main() -> int:
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = int(os.environ.get("MYSQL_PORT", "3306"))
    user = os.environ.get("MYSQL_USER", "bani_test")
    password = os.environ.get("MYSQL_PASS", "bani_test")
    database = os.environ.get("MYSQL_DB", "bani_test")

    try:
        conn = pymysql.connect(
            host=host, port=port, user=user, password=password,
            database=database, charset="utf8mb4", autocommit=True,
        )
    except Exception as e:
        print(f"Cannot connect to MySQL ({host}:{port}): {e}", file=sys.stderr)
        print(
            "Make sure the container is running: docker compose up -d mysql",
            file=sys.stderr,
        )
        return 1

    cur = conn.cursor()

    print(f"Seeding MySQL {host}:{port}/{database}")
    print("=" * 50)

    # Drop existing tables
    print("Dropping existing tables...")
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    for table in DROP_TABLES:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    # Create e-commerce schema
    print("Creating e-commerce schema (5 tables)...")
    _exec_stmts(cur, CREATE_ECOMMERCE)

    # Create data types showcase
    print("Creating data_types_showcase table...")
    _exec_stmts(cur, CREATE_SHOWCASE)

    # Insert e-commerce data
    print("Inserting e-commerce data...")
    for stmt in INSERT_ECOMMERCE:
        cur.execute(stmt)

    # Insert showcase data
    print("Inserting data_types_showcase rows...")
    for stmt in INSERT_SHOWCASE:
        cur.execute(stmt)

    # Verify
    print("\n--- Verification ---")
    ok = True
    for table, expected in EXPECTED.items():
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        row = cur.fetchone()
        actual = row[0] if row else 0
        status = "ok" if actual == expected else "MISMATCH"
        if actual != expected:
            ok = False
        print(f"  {table}: {actual} rows ({status})")

    cur.close()
    conn.close()

    print("\n" + "=" * 50)
    if ok:
        print("Setup complete!")
        return 0
    else:
        print("ERROR: Row count mismatch — check output above.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
