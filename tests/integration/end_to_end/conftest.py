"""Shared fixtures for cross-database integration tests.

These tests require Docker containers running via docker-compose.
Mark all tests with @pytest.mark.integration so they are skipped
by default and only run when explicitly requested.

Container environment variables:
  PG_HOST, PG_PORT, PG_USER, PG_PASS, PG_DB
  MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASS, MYSQL_DB
  MYSQL55_HOST, MYSQL55_PORT, MYSQL55_USER, MYSQL55_PASS, MYSQL55_DB
"""

from __future__ import annotations

import os
from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

from bani.connectors.postgresql.connector import PostgreSQLConnector
from bani.domain.project import ConnectionConfig

try:
    from bani.connectors.mysql.connector import MySQLConnector

    _HAS_MYSQL = True
except ImportError:
    _HAS_MYSQL = False
    if TYPE_CHECKING:
        from bani.connectors.mysql.connector import MySQLConnector

# ---------------------------------------------------------------------------
# Environment-based connection configs (defaults match docker-compose.yml)
# ---------------------------------------------------------------------------


def _pg_config() -> ConnectionConfig:
    """Build a PostgreSQL ConnectionConfig from environment."""
    return ConnectionConfig(
        dialect="postgresql",
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5433")),
        database=os.environ.get("PG_DB", "bani_test"),
        username_env="PG_USER",
        password_env="PG_PASS",
    )


def _mysql_config(
    port_var: str = "MYSQL_PORT",
    default_port: str = "3306",
) -> ConnectionConfig:
    """Build a MySQL ConnectionConfig from environment."""
    return ConnectionConfig(
        dialect="mysql",
        host=os.environ.get("MYSQL_HOST", "localhost"),
        port=int(os.environ.get(port_var, default_port)),
        database=os.environ.get("MYSQL_DB", "bani_test"),
        username_env="MYSQL_USER",
        password_env="MYSQL_PASS",
    )


def _mysql55_config() -> ConnectionConfig:
    """Build a MySQL 5.7 ConnectionConfig from environment."""
    return ConnectionConfig(
        dialect="mysql",
        host=os.environ.get("MYSQL55_HOST", "localhost"),
        port=int(os.environ.get("MYSQL55_PORT", "3307")),
        database=os.environ.get("MYSQL55_DB", "bani_test"),
        username_env="MYSQL55_USER",
        password_env="MYSQL55_PASS",
    )


# ---------------------------------------------------------------------------
# SQL statements to create the reference 5-table schema
# ---------------------------------------------------------------------------

PG_CREATE_SCHEMA = """
-- Categories
CREATE TABLE IF NOT EXISTS public.categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Products
CREATE TABLE IF NOT EXISTS public.products (
    id SERIAL PRIMARY KEY,
    category_id INTEGER NOT NULL REFERENCES public.categories(id),
    name VARCHAR(255) NOT NULL,
    sku CHAR(12) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    weight_kg DOUBLE PRECISION,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_products_category ON public.products(category_id);
CREATE UNIQUE INDEX idx_products_sku ON public.products(sku);

-- Customers
CREATE TABLE IF NOT EXISTS public.customers (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(200) NOT NULL,
    notes TEXT,
    registered_at DATE NOT NULL DEFAULT CURRENT_DATE
);

-- Orders
CREATE TABLE IF NOT EXISTS public.orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES public.customers(id),
    order_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    total_amount NUMERIC(12,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
);
CREATE INDEX idx_orders_customer ON public.orders(customer_id);

-- Order items (junction)
CREATE TABLE IF NOT EXISTS public.order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES public.orders(id),
    product_id INTEGER NOT NULL REFERENCES public.products(id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL
);
CREATE INDEX idx_order_items_order ON public.order_items(order_id);
CREATE INDEX idx_order_items_product ON public.order_items(product_id);
"""


MYSQL_CREATE_SCHEMA = """
-- Categories
CREATE TABLE IF NOT EXISTS categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Products
CREATE TABLE IF NOT EXISTS products (
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
CREATE TABLE IF NOT EXISTS customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(200) NOT NULL,
    notes TEXT,
    registered_at DATE NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Orders
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(12,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customers(id),
    INDEX idx_orders_customer (customer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Order items (junction)
CREATE TABLE IF NOT EXISTS order_items (
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


# ---------------------------------------------------------------------------
# Fixture data — covers NULLs, empty strings, Unicode (CJK, emoji),
# large text, boundary numeric values, edge-case dates
# ---------------------------------------------------------------------------

PG_INSERT_DATA = """
INSERT INTO public.categories (id, name, description) VALUES
    (1, 'Electronics', 'Consumer electronics and gadgets'),
    (2, '日本語カテゴリ', NULL),
    (3, 'Émojis & Spëcial ✨', '');

INSERT INTO public.products
    (id, category_id, name, sku, price, weight_kg, is_active, metadata)
VALUES
    (1, 1, 'Laptop Pro 16"', 'LAPTOP-PRO16', 2499.99, 2.1, TRUE,
     '{"brand": "TechCo", "specs": {"ram": 32}}'),
    (2, 1, 'USB-C Cable', 'USBC-CABLE01', 0.01, 0.05, TRUE, NULL),
    (3, 2, '抹茶セット', 'MATCHA-SET01', 9999999.99, NULL, TRUE, '{"origin": "京都"}'),
    (4, 3, 'Emoji Product 🎉', 'EMOJI-PROD01', 0.00, 0.0, FALSE, '{}');

INSERT INTO public.customers (id, email, full_name, notes, registered_at) VALUES
    (1, 'alice@example.com', 'Alice Müller', 'VIP customer', '2020-01-15'),
    (2, 'bob@例え.jp', '田中太郎', NULL, '2024-12-31'),
    (3, 'charlie@test.com', 'Charlie O''Brien', '', '2000-01-01');

INSERT INTO public.orders (id, customer_id, order_date, total_amount, status) VALUES
    (1, 1, '2024-06-15T10:30:00+00:00', 2500.00, 'completed'),
    (2, 2, '2024-12-31T23:59:59+00:00', 9999999.99, 'pending'),
    (3, 3, '2024-01-01T00:00:00+00:00', 0.01, 'shipped');

INSERT INTO public.order_items (id, order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 1, 2499.99),
    (2, 1, 2, 1, 0.01),
    (3, 2, 3, 999999, 9999999.99),
    (4, 3, 4, 1, 0.00);

-- Reset sequences
SELECT setval('categories_id_seq', 3);
SELECT setval('products_id_seq', 4);
SELECT setval('customers_id_seq', 3);
SELECT setval('orders_id_seq', 3);
SELECT setval('order_items_id_seq', 4);
"""


MYSQL_INSERT_DATA = """
INSERT INTO categories (id, name, description) VALUES
    (1, 'Electronics', 'Consumer electronics and gadgets'),
    (2, '日本語カテゴリ', NULL),
    (3, 'Émojis & Spëcial ✨', '');

INSERT INTO products
    (id, category_id, name, sku, price, weight_kg, is_active, metadata)
VALUES
    (1, 1, 'Laptop Pro 16"', 'LAPTOP-PRO16', 2499.99, 2.1, 1,
     '{"brand": "TechCo", "specs": {"ram": 32}}'),
    (2, 1, 'USB-C Cable', 'USBC-CABLE01', 0.01, 0.05, 1, NULL),
    (3, 2, '抹茶セット', 'MATCHA-SET01', 9999999.99, NULL, 1, '{"origin": "京都"}'),
    (4, 3, 'Emoji Product 🎉', 'EMOJI-PROD01', 0.00, 0.0, 0, '{}');

INSERT INTO customers (id, email, full_name, notes, registered_at) VALUES
    (1, 'alice@example.com', 'Alice Müller', 'VIP customer', '2020-01-15'),
    (2, 'bob@例え.jp', '田中太郎', NULL, '2024-12-31'),
    (3, 'charlie@test.com', 'Charlie O''Brien', '', '2000-01-01');

INSERT INTO orders (id, customer_id, order_date, total_amount, status) VALUES
    (1, 1, '2024-06-15 10:30:00', 2500.00, 'completed'),
    (2, 2, '2024-12-31 23:59:59', 9999999.99, 'pending'),
    (3, 3, '2024-01-01 00:00:00', 0.01, 'shipped');

INSERT INTO order_items (id, order_id, product_id, quantity, unit_price) VALUES
    (1, 1, 1, 1, 2499.99),
    (2, 1, 2, 1, 0.01),
    (3, 2, 3, 999999, 9999999.99),
    (4, 3, 4, 1, 0.00);
"""


# Table names in dependency order (parents first)
TABLE_NAMES = ("categories", "products", "customers", "orders", "order_items")

# Expected row counts per table
EXPECTED_ROW_COUNTS: dict[str, int] = {
    "categories": 3,
    "products": 4,
    "customers": 3,
    "orders": 3,
    "order_items": 4,
}


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def pg_config() -> ConnectionConfig:
    """PostgreSQL connection config from env vars."""
    return _pg_config()


@pytest.fixture()
def mysql_config() -> ConnectionConfig:
    """MySQL 8.x connection config from env vars."""
    return _mysql_config()


@pytest.fixture()
def mysql55_config() -> ConnectionConfig:
    """MySQL 5.7 connection config from env vars."""
    return _mysql55_config()


@pytest.fixture()
def pg_source(
    pg_config: ConnectionConfig,
) -> Generator[PostgreSQLConnector, None, None]:
    """A connected PostgreSQL connector with test schema and data."""
    os.environ.setdefault("PG_USER", "bani_test")
    os.environ.setdefault("PG_PASS", "bani_test")

    connector = PostgreSQLConnector()
    try:
        connector.connect(pg_config)
    except Exception as exc:
        pytest.skip(f"PostgreSQL not available: {exc}")

    # Create schema and insert fixture data
    assert connector.connection is not None
    with connector.connection.cursor() as cur:
        for stmt in PG_CREATE_SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

        for stmt in PG_INSERT_DATA.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

    yield connector
    connector.disconnect()


@pytest.fixture()
def mysql_source(
    mysql_config: ConnectionConfig,
) -> Generator[MySQLConnector, None, None]:
    """A connected MySQL 8.x connector with test schema and data."""
    os.environ.setdefault("MYSQL_USER", "bani_test")
    os.environ.setdefault("MYSQL_PASS", "bani_test")

    connector = MySQLConnector()
    try:
        connector.connect(mysql_config)
    except Exception as exc:
        pytest.skip(f"MySQL not available: {exc}")

    # Create schema and insert fixture data
    assert connector.connection is not None
    with connector.connection.cursor() as cur:
        for stmt in MYSQL_CREATE_SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

        for stmt in MYSQL_INSERT_DATA.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

    yield connector
    connector.disconnect()


@pytest.fixture()
def pg_sink(pg_config: ConnectionConfig) -> Generator[PostgreSQLConnector, None, None]:
    """A connected PostgreSQL connector for use as a sink (clean DB)."""
    os.environ.setdefault("PG_USER", "bani_test")
    os.environ.setdefault("PG_PASS", "bani_test")

    connector = PostgreSQLConnector()
    try:
        connector.connect(pg_config)
    except Exception as exc:
        pytest.skip(f"PostgreSQL not available: {exc}")
    yield connector
    connector.disconnect()


@pytest.fixture()
def mysql_sink(mysql_config: ConnectionConfig) -> Generator[MySQLConnector, None, None]:
    """A connected MySQL 8.x connector for use as a sink (clean DB)."""
    os.environ.setdefault("MYSQL_USER", "bani_test")
    os.environ.setdefault("MYSQL_PASS", "bani_test")

    connector = MySQLConnector()
    try:
        connector.connect(mysql_config)
    except Exception as exc:
        pytest.skip(f"MySQL not available: {exc}")
    yield connector
    connector.disconnect()


@pytest.fixture()
def mysql55_source(
    mysql55_config: ConnectionConfig,
) -> Generator[MySQLConnector, None, None]:
    """A connected MySQL 5.7 connector with test schema and data."""
    os.environ.setdefault("MYSQL55_USER", "bani_test")
    os.environ.setdefault("MYSQL55_PASS", "bani_test")

    connector = MySQLConnector()
    try:
        connector.connect(mysql55_config)
    except Exception as exc:
        pytest.skip(f"MySQL 5.7 not available: {exc}")

    # Create schema and insert fixture data
    assert connector.connection is not None
    with connector.connection.cursor() as cur:
        for stmt in MYSQL_CREATE_SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

        for stmt in MYSQL_INSERT_DATA.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)

    yield connector
    connector.disconnect()
