# MCP Server

Bani exposes 10 tools via the [Model Context Protocol (MCP)](https://modelcontextprotocol.io), allowing AI agents to discover databases, generate migration definitions, and execute migrations.

---

## Starting the Server

```bash
bani mcp serve
```

This starts the MCP server over stdio, which is the transport used by Claude Desktop and other MCP clients.

---

## Claude Desktop Configuration

Add Bani to your Claude Desktop configuration at `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or the equivalent path on your OS:

```json
{
  "mcpServers": {
    "bani": {
      "command": "bani",
      "args": ["mcp", "serve"]
    }
  }
}
```

!!! tip "Database connections"
    The MCP server uses the connections you have saved locally. Use the Web UI to create and save database connections (Connections page), and the MCP server will automatically know about them by name during AI interactions. For example, if you save a connection called "production-mysql" in the UI, an AI agent can reference it directly when inspecting schemas or running migrations.

---

## Credentials

Bani resolves database credentials from three places, in this order:

1. **Saved connections** -- if you reference a connection by key (e.g. `prod-mysql`), Bani looks it up in `~/.bani/connections.json`. This is the most secure and recommended path. Saved connections are managed via the Web UI.
2. **Environment variables** -- BDL files can use `${env:VAR_NAME}` to reference a credential held in an env var, or use a bare `username="MY_VAR"` which Bani resolves as an env var if one exists with that name.
3. **Plaintext in BDL** -- if neither a saved connection nor an env var is found, Bani uses the literal value from the BDL. Useful for ad-hoc migrations and the Web UI's direct password entry, but avoid checking plaintext credentials into version control.

### MCP-specific rules

- The `bani_connections` tool returns connection metadata (host, port, database) but **never** credentials.
- Tools that take connection parameters accept either a saved `connection` key (preferred) or individual fields (`host`, `port`, `database`, `username_env`, `password_env`).
- Any parameter named `password` or `credentials` (passed as plaintext to a tool) is **rejected** with a `SecurityError` -- AI agents must use connection keys or env var references, not raw passwords.
- BDL content passed to `bani_validate_bdl`, `bani_save_project`, `bani_preview`, and `bani_run` may contain plaintext credentials in connection elements, since BDL is a user document. Resolution follows the order above when the migration runs.

---

## Typical Agent Workflow

The recommended workflow for an AI agent:

```
bani_connections          -- Discover available databases
     |
bani_schema_inspect       -- Examine source/target schemas
     |
bani_generate_bdl         -- Generate a BDL template
     |
bani_validate_bdl         -- Validate the BDL
     |
bani_save_project         -- Save to ~/.bani/projects/
     |
bani_run                  -- Execute the migration
     |
bani_status               -- Check checkpoint status
```

---

## Tool Reference

### 1. `bani_connections`

List all named database connections from the Bani connections registry (`~/.bani/connections.json`).

**Parameters:** None

**Returns:** Connection key, display name, connector type, host, port, and database for each entry. Credentials are NOT included.

```json
{
  "connections": {
    "prod-mysql": {
      "key": "prod-mysql",
      "name": "Production MySQL",
      "connector": "mysql",
      "host": "db.example.com",
      "port": 3306,
      "database": "erp"
    }
  },
  "count": 1
}
```

Call this **first** to discover available databases, then pass connection keys to other tools.

---

### 2. `bani_connectors_list`

List all available connector engines.

**Parameters:** None

**Returns:** Sorted list of connector names.

```json
{
  "connectors": ["mssql", "mysql", "oracle", "postgresql", "sqlite"],
  "count": 5
}
```

---

### 3. `bani_connector_info`

Get details about a specific connector -- capabilities (source, sink, or both) and documentation.

**Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `connector_name` | `string` | Yes | Connector name (e.g. `"postgresql"`). |

**Returns:** Name, class, module, capabilities list, and docstring.

---

### 4. `bani_schema_inspect`

Introspect a live database and return its schema (tables, columns, indexes, foreign keys, row count estimates).

**Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `connection` | `string` | Preferred | Connection key from `bani_connections`. |
| `connector` | `string` | Alternative | Connector name (when `connection` is not provided). |
| `host` | `string` | -- | Database host. |
| `port` | `integer` | -- | Database port. |
| `database` | `string` | -- | Database name. |
| `username_env` | `string` | -- | Env var name holding the username. |
| `password_env` | `string` | -- | Env var name holding the password. |

Use `connection` when available (from `bani_connections`). Fall back to individual parameters for ad-hoc connections.

---

### 5. `bani_generate_bdl`

Generate a BDL XML document for a source-to-target migration.

**Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `source_connection` | `string` | Preferred | Source connection key from `bani_connections`. |
| `target_connection` | `string` | Preferred | Target connection key from `bani_connections`. |
| `source_connector` | `string` | Alternative | Source connector name. |
| `target_connector` | `string` | Alternative | Target connector name. |
| `tables` | `array[string]` | No | Optional list of table names to include. |

When connection keys are provided, the generated BDL is pre-filled with real connection details.

---

### 6. `bani_validate_bdl`

Validate a BDL document (XML or JSON) and return any errors.

**Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `bdl_content` | `string` | Yes | BDL document content (XML or JSON string). |

**Returns:**

```json
{"valid": true, "errors": []}
```

or

```json
{"valid": false, "errors": ["Missing required element: <source>"]}
```

Call this before `bani_save_project` to catch problems early.

---

### 7. `bani_save_project`

Save a BDL document to the Bani projects directory (`~/.bani/projects/<name>.bdl`).

**Parameters:**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | `string` | Yes | -- | Project name (used as filename). |
| `bdl_content` | `string` | Yes | -- | BDL document content. |
| `overwrite` | `boolean` | No | `false` | Overwrite an existing project. |

The BDL is validated before saving. Invalid documents are rejected. The saved project becomes visible in the Bani Web UI and can be executed with `bani_run`.

---

### 8. `bani_preview`

Preview sample rows from a migration source defined in BDL content.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `bdl_content` | `string` | Yes | -- | BDL document content. |
| `sample_size` | `integer` | No | `10` | Number of sample rows per table. |

Useful for verifying connectivity and inspecting data before running a full migration.

---

### 9. `bani_run`

Execute a saved migration project. The project must already exist in `~/.bani/projects/`.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `project_name` | `string` | Yes | -- | Name of the saved project (matches `bani_save_project`). |
| `dry_run` | `boolean` | No | `false` | Validate without executing. |
| `resume` | `boolean` | No | `false` | Resume from the last checkpoint. |
| `table_names` | `array[string]` | No | All | Specific tables to migrate (`schema.table` or `table`). |

**Returns:**

```json
{
  "success": true,
  "project_name": "my-migration",
  "tables_completed": 5,
  "tables_failed": 0,
  "total_rows_read": 150000,
  "total_rows_written": 150000,
  "duration_seconds": 12.3,
  "errors": [],
  "warnings": []
}
```

The tool supports MCP progress notifications. If the client provides a `progressToken`, real-time updates are sent for each table and batch.

---

### 10. `bani_status`

Check the checkpoint status of a migration project.

**Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `project_name` | `string` | Yes | Name of the migration project. |

**Returns:** Whether a checkpoint exists, and if so, which tables are completed, failed, or pending. Useful before deciding whether to resume with `bani_run`.

---

## Progress Notifications

When `bani_run` is called with a `progressToken`, the server emits MCP progress notifications at these stages:

- **Migration started** -- total table count and source/target dialects
- **Schema introspection complete** -- number of tables discovered
- **Phase change** -- current migration phase
- **Table started** -- table name and estimated row count
- **Batch complete** -- rows written and batch number
- **Table complete** -- total rows written for the table
- **Table failed** -- failure reason
- **Migration complete** -- final summary with duration

Progress uses `(tables_done, total_tables)` as the numeric progress value, with human-readable detail in the message field.
