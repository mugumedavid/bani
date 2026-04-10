# Building a Connector

This guide walks through creating a new database connector for Bani. Follow the SQLite connector as the simplest reference implementation.

---

## Step 1: Create the Package Structure

Create a new subpackage under `src/bani/connectors/`:

```
src/bani/connectors/mydb/
├── __init__.py
├── connector.py
├── schema_reader.py
├── data_reader.py
├── data_writer.py
├── type_mapper.py
└── tests/
    ├── __init__.py
    ├── test_connector.py
    └── test_type_mapper.py
```

In `__init__.py`, export your connector class:

```python
from bani.connectors.mydb.connector import MyDBConnector

__all__ = ["MyDBConnector"]
```

---

## Step 2: Implement SourceConnector

Your connector class must inherit from both `SourceConnector` and `SinkConnector` (or just one if you only support one direction).

```python
from bani.connectors.base import SourceConnector, SinkConnector
from bani.domain.project import ConnectionConfig
from bani.domain.schema import DatabaseSchema

class MyDBConnector(SourceConnector, SinkConnector):
    """MyDB database connector."""

    def connect(self, config: ConnectionConfig, pool_size: int = 1) -> None:
        """Establish a connection."""
        username = self._resolve_env_var(config.username_env)
        password = self._resolve_env_var(config.password_env)
        # Create your connection here
        self._config = config
        self._pool_size = pool_size

    def disconnect(self) -> None:
        """Close the connection."""
        # Clean up connections

    def introspect_schema(self) -> DatabaseSchema:
        """Read all tables, columns, indexes, FKs from the database."""
        # Return a DatabaseSchema with TableDefinitions

    def read_table(
        self,
        table_name: str,
        schema_name: str,
        columns: list[str] | None = None,
        filter_sql: str | None = None,
        batch_size: int = 100_000,
    ) -> Iterator[pa.RecordBatch]:
        """Yield Arrow RecordBatches from a table."""
        # Use a server-side cursor for memory efficiency
        # Convert rows to pyarrow.RecordBatch

    def estimate_row_count(self, table_name: str, schema_name: str) -> int:
        """Return an estimated row count."""
        # Use statistics if available, or COUNT(*)

    @staticmethod
    def _resolve_env_var(env_ref: str) -> str | None:
        """Resolve ${env:VAR} references."""
        if not env_ref:
            return None
        if env_ref.startswith("${env:") and env_ref.endswith("}"):
            var_name = env_ref[6:-1]
        else:
            var_name = env_ref
        return os.environ.get(var_name)
```

### Schema Introspection

The `introspect_schema()` method should:

1. Query the database's information schema or system tables.
2. Build `ColumnDefinition` objects with `arrow_type_str` populated via your type mapper.
3. Collect indexes, foreign keys, primary keys, and check constraints.
4. Return a `DatabaseSchema` with `TableDefinition` tuples.

!!! tip "Bulk queries"
    Use bulk schema queries (one query for all columns, one for all indexes, etc.) instead of N+1 per-table queries. Group results by `(schema_name, table_name)` using `defaultdict`.

### Reading Data

The `read_table()` method should:

1. Build a SELECT query with optional column list and WHERE clause.
2. Use a server-side cursor to stream results.
3. Fetch `batch_size` rows at a time.
4. Convert each batch to a `pyarrow.RecordBatch` and yield it.

```python
import pyarrow as pa

def read_table(self, table_name, schema_name, columns=None,
               filter_sql=None, batch_size=100_000):
    col_list = ", ".join(columns) if columns else "*"
    sql = f"SELECT {col_list} FROM {schema_name}.{table_name}"
    if filter_sql:
        sql += f" WHERE {filter_sql}"

    with self.checkout() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            # Build Arrow arrays from rows
            arrays = []
            names = [desc[0] for desc in cursor.description]
            for i, name in enumerate(names):
                col_values = [row[i] for row in rows]
                arrays.append(pa.array(col_values))
            yield pa.RecordBatch.from_arrays(arrays, names=names)
```

---

## Step 3: Implement SinkConnector

```python
def create_table(self, table_def: TableDefinition) -> None:
    """Create a table with all columns and primary key."""
    if not table_def.columns:
        raise ValueError(f"Table {table_def.table_name} has no columns")

    col_defs = []
    for col in table_def.columns:
        if col.arrow_type_str:
            db_type = MyDBTypeMapper.from_arrow_type(col.arrow_type_str)
        else:
            db_type = col.data_type
        col_def = f'"{col.name}" {db_type}'
        if not col.nullable:
            col_def += " NOT NULL"
        col_defs.append(col_def)

    if table_def.primary_key:
        pk_cols = ", ".join(f'"{c}"' for c in table_def.primary_key)
        col_defs.append(f"PRIMARY KEY ({pk_cols})")

    create_sql = f'CREATE TABLE "{table_def.table_name}" ({", ".join(col_defs)})'
    # Execute the DDL

def write_batch(self, table_name: str, schema_name: str,
                batch: pa.RecordBatch) -> int:
    """Write an Arrow RecordBatch to a table."""
    # Use the most efficient bulk insert method available
    # Return the number of rows written

def create_indexes(self, table_name, schema_name, indexes):
    """Create indexes on a table."""
    for index in indexes:
        # Build and execute CREATE INDEX

def create_foreign_keys(self, fks):
    """Create foreign key constraints."""
    for fk in fks:
        # Build and execute ALTER TABLE ADD CONSTRAINT

def execute_sql(self, sql: str) -> None:
    """Execute arbitrary SQL."""
    # Execute the statement
```

---

## Step 4: Create the TypeMapper

The type mapper has two directions:

```python
class MyDBTypeMapper:
    """Maps MyDB types to Arrow types and vice versa."""

    def map_mydb_type_name(self, type_name: str) -> pa.DataType:
        """Map a MyDB type name to an Arrow type."""
        mapping = {
            "INTEGER": pa.int64(),
            "TEXT": pa.string(),
            "REAL": pa.float64(),
            "BLOB": pa.binary(),
            # ... more mappings
        }
        return mapping.get(type_name.upper(), pa.string())

    @staticmethod
    def from_arrow_type(arrow_type_str: str) -> str:
        """Convert an Arrow type string to a MyDB DDL type."""
        mapping = {
            "bool": "BOOLEAN",
            "int32": "INTEGER",
            "int64": "BIGINT",
            "string": "TEXT",
            "binary": "BLOB",
            # ... more mappings
        }
        ts = arrow_type_str.strip()
        if ts in mapping:
            return mapping[ts]
        # Handle parameterized types
        if ts.startswith("timestamp"):
            return "TIMESTAMP"
        if ts.startswith("decimal128"):
            return "NUMERIC"
        return "TEXT"  # Fallback
```

!!! warning "Arrow type string format"
    The `arrow_type_str` uses the format produced by `str(pa_type)`. PyArrow emits `"float"` (not `"float32"`), `"double"` (not `"float64"`), and `"date32[day]"` (not `"date32"`). Include both forms in your mapping.

---

## Step 5: Register via Entry Point

Add your connector to `pyproject.toml`:

```toml
[project.entry-points."bani.connectors"]
mydb = "bani.connectors.mydb:MyDBConnector"
```

After installation, the connector will be automatically discovered by `ConnectorRegistry.discover()`.

---

## Step 6: Write Tests

Create tests for:

1. **Type mapper** -- Test every type mapping in both directions.
2. **Connector** -- Test connect/disconnect, schema introspection, data read/write.
3. **Integration** -- Test full migration with real database containers.

```python
import pytest
from bani.connectors.mydb.type_mapper import MyDBTypeMapper

class TestMyDBTypeMapper:
    def test_integer_mapping(self):
        mapper = MyDBTypeMapper()
        assert mapper.map_mydb_type_name("INTEGER") == pa.int64()

    def test_from_arrow_int32(self):
        assert MyDBTypeMapper.from_arrow_type("int32") == "INTEGER"

    def test_from_arrow_string(self):
        assert MyDBTypeMapper.from_arrow_type("string") == "TEXT"
```

---

## Reference: SQLite Connector

The SQLite connector at `src/bani/connectors/sqlite/` is the simplest implementation to follow:

- `connector.py` -- 400 lines, implements all methods
- `schema_reader.py` -- Uses `PRAGMA table_info` and `PRAGMA index_list`
- `data_reader.py` -- Simple cursor-based reads
- `data_writer.py` -- `executemany` with parameter binding
- `type_mapper.py` -- Follows SQLite's 5 type affinity rules

Look at the PostgreSQL connector for a more complex example with COPY protocol writes and streaming cursors.
