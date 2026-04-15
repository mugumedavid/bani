---
title: "Why Apache Arrow is the Right Foundation for Database Migrations"
description: "A deep dive into why Bani uses Apache Arrow as its internal data format, how columnar processing improves migration performance, and the N-mapper architecture that makes any-to-any migration practical."
date: 2026-04-09
author: "David Mugume"
tags: ["architecture", "apache-arrow", "performance", "technical"]
---

When I started building Bani, one of the earliest and most consequential design decisions was choosing the internal data representation. The data format you use between reading from the source and writing to the target determines everything: performance characteristics, memory usage, type mapping complexity, and how many database pairs you can practically support.

I chose Apache Arrow, and this post explains why.

## The row-by-row problem

The simplest approach to database migration is to read one row at a time from the source and write it to the target. Most ad-hoc migration scripts work this way:

```python
for row in source_cursor.fetchall():
    target_cursor.execute(insert_sql, row)
```

This approach has several problems at scale:

**Overhead per row.** Each row requires a Python function call, a database round-trip, and parameter binding. With millions of rows, this overhead dominates the actual data transfer time.

**Memory unpredictability.** `fetchall()` loads the entire result set into memory. Switching to `fetchone()` or `fetchmany()` helps but adds complexity and still processes data one row at a time.

**No vectorized operations.** Modern databases and storage systems are optimized for batch operations. Row-by-row processing leaves performance on the table.

**Type mapping sprawl.** If you support N source databases and M target databases, you need N * M type mapping paths. With 5 databases, that is 20 custom type converters to maintain.

## Why columnar formats help

Apache Arrow is a columnar in-memory data format. Instead of storing data as a sequence of rows, it stores data as a sequence of columns. A table with 1 million rows and 10 columns is represented as 10 arrays of 1 million values each.

This matters for migrations because:

### Batch processing

Arrow's `RecordBatch` is designed for batch operations. You read thousands of rows into a single RecordBatch and write them to the target in one operation. This amortizes the per-operation overhead across many rows.

```python
# Instead of 1 million individual inserts:
# We get ~1,000 batch inserts of 1,000 rows each
for batch in reader.read_batches(batch_size=1000):
    writer.write_batch(batch)
```

### Memory efficiency

Arrow uses a fixed-size memory layout with no per-value overhead. A column of 64-bit integers uses exactly 8 bytes per value, with no object headers, no pointer indirection, and no GC pressure. For large migrations, this means predictable memory usage and the ability to process datasets larger than available RAM by streaming batches.

### Zero-copy when possible

Arrow's standardized memory layout means that data read into Arrow format by one library can be consumed by another library without copying. In Bani's case, the pyarrow library handles the in-memory representation, and both the source reader and target writer operate directly on Arrow arrays.

### Consistent type system

Arrow defines a rich type system that maps naturally to database types: integers (8/16/32/64-bit, signed and unsigned), floating point, decimal, strings (UTF-8), binary, timestamps (with timezone), dates, times, intervals, and nested types. This type system serves as the lingua franca between databases.

## The N-mapper architecture

The most important architectural benefit of using Arrow is what I call the N-mapper architecture.

Without an intermediate format, supporting N databases requires N * (N-1) direct conversion paths. With 5 databases, that is 20 conversion paths, each with its own type mapping rules and edge cases.

With Arrow as an intermediate format, you need only N readers (source -> Arrow) and N writers (Arrow -> target). With 5 databases, that is 10 components instead of 20, and each one can be developed, tested, and maintained independently.

```
PostgreSQL ──read──> Arrow ──write──> MySQL
MySQL      ──read──> Arrow ──write──> PostgreSQL
MSSQL      ──read──> Arrow ──write──> Oracle
Oracle     ──read──> Arrow ──write──> SQLite
SQLite     ──read──> Arrow ──write──> MSSQL
```

Adding a sixth database (say MariaDB) requires writing only one reader and one writer, and it immediately works with all five existing databases. Without the intermediate format, adding a sixth database would require 10 new conversion paths.

## How Bani uses Arrow in practice

Here is how a migration flows through Bani's Arrow-based engine:

### 1. Schema reading

The source connector reads the database schema (tables, columns, types, indexes, constraints) and converts it into Bani's internal schema representation. This step does not use Arrow — it is metadata, not data.

### 2. Schema writing

The target connector takes the internal schema representation and creates the corresponding tables, columns, and constraints in the target database. Type mapping happens here: a PostgreSQL `SERIAL` becomes a MySQL `AUTO_INCREMENT`, an Oracle `NUMBER(10)` becomes a PostgreSQL `INTEGER`, and so on.

### 3. Data reading

The source connector reads data from each table into Arrow RecordBatches. Each batch contains a configurable number of rows (default: 10,000). The connector maps source database types to Arrow types:

- `INTEGER` -> `arrow.int32()`
- `VARCHAR(255)` -> `arrow.string()`
- `TIMESTAMP WITH TIME ZONE` -> `arrow.timestamp('us', tz='UTC')`
- `DECIMAL(10,2)` -> `arrow.decimal128(10, 2)`
- `BLOB` -> `arrow.binary()`

### 4. Data writing

The target connector receives Arrow RecordBatches and writes them to the target database. It maps Arrow types back to target database types and uses the target's most efficient bulk insert mechanism (PostgreSQL's `COPY`, MySQL's `LOAD DATA`, MSSQL's bulk insert, etc.).

### 5. Index creation

After data is loaded, the target connector creates indexes. This is done last because creating indexes on an empty table and then bulk-loading data is significantly slower than loading data first and creating indexes afterward.

## Real-world performance characteristics

Arrow's batch processing model gives Bani several performance advantages over row-by-row approaches:

**Throughput.** Batch inserts of 1,000-10,000 rows are typically 10-100x faster than individual row inserts. The exact speedup depends on the database, network latency, and row size.

**Memory.** Arrow's fixed-size memory layout and batch streaming mean that Bani can migrate a 100 GB table using only a few hundred megabytes of RAM. The batch size controls the memory-throughput tradeoff.

**CPU.** Columnar layouts enable SIMD operations for type conversions and data validation. While Bani does not yet exploit this directly, the pyarrow library does for many operations.

**Network.** Fewer, larger database operations mean fewer network round-trips. This matters especially for migrations over WAN connections.

## Conclusion

Apache Arrow is not the only way to build a database migration tool. You can build a perfectly functional tool using row-by-row processing, DataFrames, CSV files, or any other intermediate format. But Arrow provides a combination of performance, memory efficiency, type system richness, and architectural cleanliness that makes it the right choice for Bani.

The N-mapper architecture means that adding new database support is O(1) in complexity rather than O(N). The batch processing model means that performance scales with data volume. And the standardized type system means that type mapping is consistent and predictable.

If you want to see this architecture in action, [install Bani](https://docs.bani.tools/getting-started) and try a migration. The source code is available on [GitHub](https://github.com/mugumedavid/bani) under the Apache-2.0 license.
