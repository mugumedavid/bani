# Type Mappings

Bani uses Apache Arrow as a **canonical type intermediate** between source and target databases. This gives N type mappers (one per connector) instead of N*N cross-database translation tables.

---

## How It Works

```
Source DB Type  -->  Arrow Type  -->  Target DB Type
   (read)        (interchange)        (write)
```

1. **Source introspection:** The source connector maps each column's native type to an Arrow type and stores it in `ColumnDefinition.arrow_type_str`.
2. **Data transfer:** Data flows as `pyarrow.RecordBatch` between connectors.
3. **Target DDL generation:** The sink connector calls `from_arrow_type()` on its type mapper to convert the Arrow type string to native DDL.

Each connector implements two directions:

- **Source side:** `map_*_type_name()` or `map_*_type_oid()` -- native type to Arrow type
- **Sink side:** `from_arrow_type()` -- Arrow type string to native DDL type

---

## Overriding Mappings

Use `<typeMappings>` in BDL to override the automatic mapping for specific types:

```xml
<typeMappings>
  <mapping sourceType="MEDIUMTEXT" targetType="TEXT" />
  <mapping sourceType="TINYINT(1)" targetType="BOOLEAN" />
</typeMappings>
```

Or via the SDK:

```python
builder.type_mapping("MEDIUMTEXT", "TEXT")
```

---

## PostgreSQL Type Mappings

### Source: PostgreSQL to Arrow

| PostgreSQL Type | Arrow Type |
|---|---|
| `boolean` | `bool` |
| `smallint`, `smallserial` | `int16` |
| `integer`, `serial` | `int32` |
| `bigint`, `bigserial` | `int64` |
| `real` | `float32` |
| `double precision` | `float64` |
| `numeric`, `decimal` | `decimal128(38, 10)` |
| `text`, `varchar`, `char`, `name` | `string` |
| `bytea` | `binary` |
| `date` | `date32` |
| `time` | `time64[us]` |
| `timestamp` | `timestamp[us]` |
| `timestamptz` | `timestamp[us, tz=UTC]` |
| `interval` | `duration[us]` |
| `uuid` | `string` |
| `json`, `jsonb` | `string` |
| `inet`, `cidr`, `macaddr` | `string` |

### Sink: Arrow to PostgreSQL DDL

| Arrow Type | PostgreSQL DDL |
|---|---|
| `bool` | `boolean` |
| `int8`, `int16` | `smallint` |
| `int32` | `integer` |
| `int64` | `bigint` |
| `float`, `float32` | `real` |
| `double`, `float64` | `double precision` |
| `string`, `utf8` | `text` |
| `binary` | `bytea` |
| `date32`, `date64` | `date` |
| `timestamp[us]` | `timestamp` |
| `timestamp[us, tz=UTC]` | `timestamp with time zone` |
| `time32`, `time64` | `time` |
| `duration` | `interval` |
| `decimal128(p, s)` | `numeric(p, s)` |

---

## MySQL Type Mappings

### Source: MySQL to Arrow

| MySQL Type | Arrow Type |
|---|---|
| `TINYINT` | `int8` (unsigned: `int16`) |
| `SMALLINT` | `int16` (unsigned: `int32`) |
| `INT`, `MEDIUMINT` | `int32` (unsigned: `int64`) |
| `BIGINT` | `int64` (unsigned: `decimal128(20, 0)`) |
| `FLOAT` | `float32` |
| `DOUBLE` | `float64` |
| `DECIMAL`, `NUMERIC` | `decimal128(38, 10)` |
| `BIT`, `BOOLEAN` | `bool` |
| `CHAR`, `VARCHAR`, `TEXT`, `*TEXT` | `string` |
| `BINARY`, `VARBINARY`, `*BLOB` | `binary` |
| `DATE` | `date32` |
| `TIME` | `time64[us]` |
| `DATETIME` | `timestamp[us]` |
| `TIMESTAMP` | `timestamp[us, tz=UTC]` |
| `YEAR` | `int16` |
| `JSON` | `string` |
| `ENUM`, `SET` | `string` |

!!! note "Unsigned integers"
    MySQL unsigned integers are promoted to the next wider signed type to avoid overflow. Unsigned `BIGINT` maps to `decimal128(20, 0)`.

### Sink: Arrow to MySQL DDL

| Arrow Type | MySQL DDL |
|---|---|
| `bool` | `TINYINT(1)` |
| `int8` | `TINYINT` |
| `int16` | `SMALLINT` |
| `int32` | `INT` |
| `int64` | `BIGINT` |
| `float`, `float32` | `FLOAT` |
| `double`, `float64` | `DOUBLE` |
| `string` | `TEXT` |
| `binary` | `BLOB` |
| `date32` | `DATE` |
| `timestamp[us]` | `DATETIME` |
| `timestamp[us, tz=*]` | `TIMESTAMP` |
| `time32`, `time64` | `TIME` |
| `decimal128(p, s)` | `DECIMAL(p, s)` |

---

## SQL Server Type Mappings

### Source: MSSQL to Arrow

| MSSQL Type | Arrow Type |
|---|---|
| `tinyint` | `uint8` |
| `smallint` | `int16` |
| `int` | `int32` |
| `bigint` | `int64` |
| `decimal`, `numeric` | `decimal128(38, 10)` |
| `money` | `decimal128(19, 4)` |
| `smallmoney` | `decimal128(10, 4)` |
| `float` | `float64` |
| `real` | `float32` |
| `bit` | `bool` |
| `char`, `varchar`, `text` | `string` |
| `nchar`, `nvarchar`, `ntext` | `string` |
| `binary`, `varbinary`, `image` | `binary` |
| `date` | `date32` |
| `time` | `time64[us]` |
| `datetime`, `datetime2`, `smalldatetime` | `timestamp[us]` |
| `datetimeoffset` | `timestamp[us, tz=UTC]` |
| `uniqueidentifier`, `xml`, `json` | `string` |
| `rowversion`, `timestamp` | `binary` |

### Sink: Arrow to MSSQL DDL

| Arrow Type | MSSQL DDL |
|---|---|
| `bool` | `BIT` |
| `int8`, `int16` | `SMALLINT` |
| `int32` | `INT` |
| `int64` | `BIGINT` |
| `float`, `float32` | `REAL` |
| `double`, `float64` | `FLOAT` |
| `string` | `NVARCHAR(MAX)` |
| `binary` | `VARBINARY(MAX)` |
| `date32` | `DATE` |
| `timestamp[us]` | `DATETIME2` |
| `timestamp[us, tz=*]` | `DATETIMEOFFSET` |
| `time32`, `time64` | `TIME` |
| `decimal128(p, s)` | `DECIMAL(p, s)` |

!!! note "NVARCHAR(MAX) and indexes"
    Arrow `string` maps to `NVARCHAR(MAX)`, which cannot be indexed in SQL Server. The MSSQL connector automatically narrows indexed columns to `NVARCHAR(4000)` when creating indexes. When the source `data_type` carries a length (e.g. `varchar(255)`), the connector recovers it and uses `NVARCHAR(255)` instead.

---

## Oracle Type Mappings

### Source: Oracle to Arrow

| Oracle Type | Arrow Type |
|---|---|
| `NUMBER` (no params) | `decimal128(38, 10)` |
| `NUMBER(p, 0)` (p <= 9) | `int32` |
| `NUMBER(p, 0)` (p <= 18) | `int64` |
| `NUMBER(p, s)` | `decimal128(p, s)` |
| `INTEGER` | `int64` |
| `BINARY_FLOAT` | `float32` |
| `BINARY_DOUBLE` | `float64` |
| `VARCHAR2`, `NVARCHAR2`, `CHAR`, `CLOB` | `string` |
| `RAW`, `BLOB` | `binary` |
| `DATE` | `timestamp[us]` (Oracle DATE includes time) |
| `TIMESTAMP` | `timestamp[us]` |
| `TIMESTAMP WITH TIME ZONE` | `timestamp[us, tz=UTC]` |

### Sink: Arrow to Oracle DDL

| Arrow Type | Oracle DDL |
|---|---|
| `bool` | `NUMBER(1,0)` |
| `int8` | `NUMBER(3,0)` |
| `int16` | `NUMBER(5,0)` |
| `int32` | `NUMBER(10,0)` |
| `int64` | `NUMBER(19,0)` |
| `float`, `float32` | `BINARY_FLOAT` |
| `double`, `float64` | `BINARY_DOUBLE` |
| `string` | `CLOB` |
| `binary` | `BLOB` |
| `date32` | `DATE` |
| `timestamp[us]` | `TIMESTAMP` |
| `timestamp[us, tz=*]` | `TIMESTAMP WITH TIME ZONE` |
| `time32`, `time64` | `VARCHAR2(20)` |
| `decimal128(p, s)` | `NUMBER(p, s)` |

---

## SQLite Type Mappings

SQLite uses a type affinity system with only 5 storage classes: NULL, INTEGER, REAL, TEXT, BLOB. The mapper follows the [official affinity rules](https://www.sqlite.org/datatype3.html#type_affinity).

### Source: SQLite to Arrow

| SQLite Declared Type | Arrow Type |
|---|---|
| `INTEGER`, `INT`, `BIGINT` | `int64` |
| `TINYINT` | `int8` |
| `SMALLINT` | `int16` |
| `MEDIUMINT` | `int32` |
| `REAL`, `DOUBLE`, `FLOAT` | `float64` |
| `TEXT`, `VARCHAR`, `CHAR`, `CLOB` | `string` |
| `BLOB` | `binary` |
| `BOOLEAN`, `BOOL` | `bool` |
| `DATE` | `date32` |
| `DATETIME`, `TIMESTAMP` | `timestamp[us]` |
| `NUMERIC`, `DECIMAL` | `decimal128(38, 10)` |
| (no type declared) | `binary` |

### Sink: Arrow to SQLite DDL

| Arrow Type | SQLite DDL |
|---|---|
| `bool` | `BOOLEAN` |
| `int8` through `int64`, `uint*` | `INTEGER` |
| `float`, `double`, `float32`, `float64` | `REAL` |
| `string` | `TEXT` |
| `binary` | `BLOB` |
| `date32`, `timestamp`, `time*`, `duration` | `TEXT` |
| `decimal128` | `NUMERIC` |

---

## Known Limitations

- **Precision loss:** `DECIMAL(10,2)` becomes `decimal128(38,10)` in Arrow, then `numeric(38,10)` in PostgreSQL. The original precision is not preserved through the Arrow intermediate.
- **VARCHAR length loss:** `VARCHAR(255)` maps to Arrow `string`, then to `TEXT` (PostgreSQL/MySQL) or `NVARCHAR(MAX)` (MSSQL). The MSSQL connector recovers the length from the source `data_type` when it is <= 4000 characters.
- **Oracle DATE includes time:** Oracle's `DATE` type stores both date and time, so it maps to `timestamp[us]`, not `date32`.
- **SQLite dates as TEXT:** SQLite stores dates as ISO 8601 text strings. The type mapper handles coercion during reads.
