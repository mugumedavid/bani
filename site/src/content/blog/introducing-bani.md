---
title: "Introducing Bani: Open-Source Database Migrations Powered by Apache Arrow"
description: "Bani is a new open-source database migration engine that uses Apache Arrow as a universal interchange format. Migrate schema, data, and indexes across PostgreSQL, MySQL, MSSQL, Oracle, and SQLite."
date: 2026-04-09
author: "David Mugume"
tags: ["announcement", "open-source", "database", "migration"]
---

Database migrations are one of those tasks that every engineering team faces but nobody enjoys. Whether you are switching database vendors, upgrading to a newer version, consolidating multiple databases into one, or splitting a monolith into microservices, at some point you need to move data from point A to point B.

Today I am announcing **Bani**, an open-source database migration engine designed to make this process straightforward, repeatable, and automatable.

## The problem with existing tools

I built Bani because the existing landscape left a lot to be desired.

**Commercial tools** like FullConvert work well but cost $499 or more, run only on Windows, and are closed source. If you need to script your migration or run it in a CI/CD pipeline, you are out of luck.

**pgloader** is an excellent free tool, but it only writes to PostgreSQL. If your target is MySQL, MSSQL, Oracle, or SQLite, pgloader cannot help you.

**Custom scripts** are what most teams end up writing. A hundred lines of Python that read from the source and write to the target, with ad-hoc type mappings, no error handling, and no tests. These scripts are written once, used once, and thrown away. When the next migration comes around, you start from scratch.

Bani eliminates all three of these problems. It is free and open source, supports five databases as both source and target, and provides a complete toolkit so you never need to write one-off scripts again.

## What Bani does

At its core, Bani reads data from a source database into Apache Arrow RecordBatches and writes those RecordBatches to a target database. This architecture gives you **20 migration paths** from just 5 connectors (any-to-any), rather than the 20 custom converters you would need with a direct approach.

Here is what ships in v1.0:

### Five database connectors

PostgreSQL, MySQL, Microsoft SQL Server, Oracle, and SQLite. Each connector handles schema reading, data reading (into Arrow), schema writing, and data writing (from Arrow). Every connector works as both source and target.

### Apache Arrow engine

Data flows through Arrow RecordBatches — a columnar, memory-efficient in-memory format. This means batch processing instead of row-by-row, efficient memory usage for large tables, and a consistent intermediate representation regardless of the source or target database.

### Bani Definition Language (BDL)

Migrations are defined declaratively in XML or JSON. A BDL file specifies your source connection, target connection, table mappings, column transformations, and index handling. BDL files are designed to be checked into version control alongside your application code.

### CLI with 11 commands

Initialize projects, inspect schemas, validate configurations, preview migrations, run them, check status, and more. Every command supports JSON output for scripting and CI/CD integration.

### Python SDK

The `ProjectBuilder` API lets you construct and run migrations programmatically. `SchemaInspector` lets you analyze and compare database schemas. Use the SDK in scripts, Jupyter notebooks, or your own applications.

### MCP server with 10 tools

Bani ships with a built-in MCP (Model Context Protocol) server. Connect it to Claude Desktop, Cursor, or any MCP-compatible AI agent to drive migrations through natural language. The agent can inspect schemas, generate BDL configurations, validate them, and execute migrations.

### React web dashboard

A browser-based dashboard provides real-time migration monitoring with Server-Sent Events. Track progress per table, view logs, and manage multiple projects.

### Cross-platform

macOS menu bar app, Docker image, Linux packages (.deb, .rpm), and a Windows installer. Bani runs wherever your databases live.

## Getting started

Install Bani with pip:

```bash
pip install bani
```

Initialize a new migration project:

```bash
bani init my-migration
cd my-migration
```

Inspect your source database:

```bash
bani schema inspect --dsn "postgresql://user:pass@host/db"
```

Edit the generated BDL configuration, then run the migration:

```bash
bani run project.bdl.xml
```

For the full getting-started guide, visit [docs.bani.tools/en/latest/getting-started/](https://docs.bani.tools/en/latest/getting-started/).

## What's next

Bani v1.0 is just the beginning. Here is what is on the roadmap:

- **More connectors:** MariaDB, MongoDB, and CSV/Parquet support
- **REST and gRPC API:** Run Bani as a long-running server
- **Schema diff:** Detect drift between databases
- **Data masking:** Transform sensitive data during migration
- **Arrow Flight:** Distributed migration using Apache Arrow Flight

All of these features will be open source under the Apache-2.0 license.

## Get involved

Bani is open source and community-driven. Here is how you can get involved:

- **Star the repo** on [GitHub](https://github.com/mugumedavid/bani)
- **Report bugs** or **request features** via GitHub Issues
- **Contribute code** — see the [Contributing Guide](https://github.com/mugumedavid/bani/blob/main/CONTRIBUTING.md)
- **Join the discussion** on [GitHub Discussions](https://github.com/mugumedavid/bani/discussions)

I built Bani because I needed it. I hope you find it useful too.
