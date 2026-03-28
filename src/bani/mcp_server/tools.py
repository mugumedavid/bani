"""MCP tool definitions and handlers (Section 18.6).

Defines the 8 tools that the Bani MCP server exposes to AI agents:

1. bani_schema_inspect  — introspect a database schema
2. bani_validate_bdl    — validate a BDL document
3. bani_preview         — preview sample rows from a source
4. bani_run             — execute a migration
5. bani_status          — check checkpoint status for a project
6. bani_connectors_list — list all available connectors
7. bani_connector_info  — get details about a specific connector
8. bani_generate_bdl    — generate a BDL XML template

Each tool handler is a synchronous function that receives a ``params`` dict
and returns a ``ToolResult``.  Credential security is enforced: tools that
need database access accept environment variable *names* (not values), and
any parameter named ``password`` or ``credentials`` is rejected.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

# ---------------------------------------------------------------------------
# Result / definition types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToolDefinition:
    """Describes a single MCP tool."""

    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass(frozen=True)
class ToolResult:
    """Result returned from a tool handler."""

    content: list[dict[str, Any]]
    is_error: bool = False


# Convenience type alias
ToolHandler = Callable[[dict[str, Any]], ToolResult]


def _text_result(text: str, *, is_error: bool = False) -> ToolResult:
    """Build a ToolResult containing a single text content block."""
    return ToolResult(
        content=[{"type": "text", "text": text}],
        is_error=is_error,
    )


def _json_result(data: Any, *, is_error: bool = False) -> ToolResult:
    """Build a ToolResult containing JSON-serialized text."""
    return _text_result(
        json.dumps(data, indent=2, default=str),
        is_error=is_error,
    )


def _reject_plaintext_credentials(params: dict[str, Any]) -> ToolResult | None:
    """Return an error ToolResult if the caller passed raw credentials.

    MCP clients must reference environment variable *names*, never plaintext
    values.  If ``password`` or ``credentials`` appears as a parameter key the
    call is rejected.
    """
    forbidden = {"password", "credentials"}
    found = forbidden & set(params.keys())
    if found:
        return _text_result(
            f"SecurityError: plaintext credential fields are not allowed: "
            f"{', '.join(sorted(found))}. "
            f"Use username_env / password_env to reference environment variables.",
            is_error=True,
        )
    return None


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------


def handle_schema_inspect(params: dict[str, Any]) -> ToolResult:
    """Introspect a database and return its schema as JSON."""
    security_err = _reject_plaintext_credentials(params)
    if security_err is not None:
        return security_err

    try:
        from bani.sdk.schema_inspector import SchemaInspector

        connector = params.get("connector", "")
        if not connector:
            return _text_result("Error: 'connector' parameter is required.", is_error=True)

        schema = SchemaInspector.inspect(
            dialect=connector,
            host=params.get("host", ""),
            port=int(params.get("port", 0)),
            database=params.get("database", ""),
            username_env=params.get("username_env", ""),
            password_env=params.get("password_env", ""),
        )

        # Serialize the DatabaseSchema to a JSON-friendly dict
        tables_data: list[dict[str, Any]] = []
        for table in schema.tables:
            columns_data = [
                {
                    "name": col.name,
                    "data_type": col.data_type,
                    "nullable": col.nullable,
                    "default_value": col.default_value,
                    "is_auto_increment": col.is_auto_increment,
                    "arrow_type": col.arrow_type_str,
                }
                for col in table.columns
            ]
            indexes_data = [
                {
                    "name": idx.name,
                    "columns": list(idx.columns),
                    "is_unique": idx.is_unique,
                }
                for idx in table.indexes
            ]
            fks_data = [
                {
                    "name": fk.name,
                    "source_table": fk.source_table,
                    "source_columns": list(fk.source_columns),
                    "referenced_table": fk.referenced_table,
                    "referenced_columns": list(fk.referenced_columns),
                }
                for fk in table.foreign_keys
            ]
            tables_data.append(
                {
                    "schema_name": table.schema_name,
                    "table_name": table.table_name,
                    "fully_qualified_name": table.fully_qualified_name,
                    "columns": columns_data,
                    "primary_key": list(table.primary_key),
                    "indexes": indexes_data,
                    "foreign_keys": fks_data,
                    "row_count_estimate": table.row_count_estimate,
                }
            )

        result_data = {
            "source_dialect": schema.source_dialect,
            "tables": tables_data,
            "table_count": len(tables_data),
        }
        return _json_result(result_data)
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


def handle_validate_bdl(params: dict[str, Any]) -> ToolResult:
    """Validate a BDL document (XML or JSON)."""
    try:
        bdl_content = params.get("bdl_content", "")
        if not bdl_content:
            return _text_result(
                "Error: 'bdl_content' parameter is required.", is_error=True
            )

        content = str(bdl_content).strip()
        if content.startswith("<") or content.startswith("<?"):
            from bani.bdl.validator import validate_xml

            errors = validate_xml(content)
        else:
            from bani.bdl.validator import validate_json

            errors = validate_json(content)

        if errors:
            return _json_result(
                {"valid": False, "errors": errors},
                is_error=False,
            )
        return _json_result({"valid": True, "errors": []})
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


def handle_preview(params: dict[str, Any]) -> ToolResult:
    """Preview sample rows from a migration source."""
    try:
        from typing import cast

        from bani.application.preview import preview_source
        from bani.bdl.parser import parse
        from bani.connectors.base import SourceConnector
        from bani.connectors.registry import ConnectorRegistry

        bdl_content = params.get("bdl_content", "")
        if not bdl_content:
            return _text_result(
                "Error: 'bdl_content' parameter is required.", is_error=True
            )

        sample_size = int(params.get("sample_size", 10))

        # Parse BDL to get source config
        project = parse(str(bdl_content))
        if project.source is None:
            return _text_result(
                "Error: BDL has no source connection defined.", is_error=True
            )

        source_connector_class = ConnectorRegistry.get(project.source.dialect)
        source = cast(type[SourceConnector], source_connector_class)()
        source.connect(project.source)

        try:
            result = preview_source(source, sample_size=sample_size)

            tables_data = []
            for tp in result.tables:
                tables_data.append(
                    {
                        "table_name": tp.table_name,
                        "schema_name": tp.schema_name,
                        "row_count_estimate": tp.row_count_estimate,
                        "columns": [
                            {
                                "name": c.name,
                                "data_type": c.data_type,
                                "nullable": c.nullable,
                            }
                            for c in tp.columns
                        ],
                        "sample_rows": list(tp.sample_rows),
                    }
                )
            return _json_result(
                {
                    "source_dialect": result.source_dialect,
                    "tables": tables_data,
                }
            )
        finally:
            source.disconnect()
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


def handle_run(params: dict[str, Any]) -> ToolResult:
    """Execute a migration from BDL content."""
    try:
        from bani.bdl.parser import parse
        from bani.sdk.bani import Bani, BaniProject

        bdl_content = params.get("bdl_content", "")
        if not bdl_content:
            return _text_result(
                "Error: 'bdl_content' parameter is required.", is_error=True
            )

        dry_run = bool(params.get("dry_run", False))

        # Parse BDL
        project = parse(str(bdl_content))
        bani_project = BaniProject(project)

        # Validate
        is_valid, errors = bani_project.validate()
        if not is_valid:
            return _json_result(
                {"success": False, "errors": errors},
                is_error=True,
            )

        if dry_run:
            return _json_result(
                {
                    "success": True,
                    "dry_run": True,
                    "message": "Validation passed. No migration executed.",
                    "project_name": project.name,
                }
            )

        # Execute
        result = bani_project.run()
        return _json_result(
            {
                "success": True,
                "project_name": result.project_name,
                "tables_completed": result.tables_completed,
                "tables_failed": result.tables_failed,
                "total_rows_read": result.total_rows_read,
                "total_rows_written": result.total_rows_written,
                "duration_seconds": result.duration_seconds,
                "errors": list(result.errors),
            }
        )
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


def handle_status(params: dict[str, Any]) -> ToolResult:
    """Check checkpoint status for a project."""
    try:
        from bani.application.checkpoint import CheckpointManager

        project_name = params.get("project_name", "")
        if not project_name:
            return _text_result(
                "Error: 'project_name' parameter is required.", is_error=True
            )

        mgr = CheckpointManager()
        checkpoint = mgr.load(str(project_name))

        if checkpoint is None:
            return _json_result(
                {
                    "found": False,
                    "project_name": project_name,
                    "message": "No checkpoint found for this project.",
                }
            )

        return _json_result(
            {
                "found": True,
                "project_name": project_name,
                "checkpoint": checkpoint,
            }
        )
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


def handle_connectors_list(params: dict[str, Any]) -> ToolResult:
    """List all available connectors."""
    try:
        from bani.connectors.registry import ConnectorRegistry

        connectors = ConnectorRegistry.discover()

        connector_names = sorted(connectors.keys())
        return _json_result(
            {
                "connectors": connector_names,
                "count": len(connector_names),
            }
        )
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


def handle_connector_info(params: dict[str, Any]) -> ToolResult:
    """Get details about a specific connector."""
    try:
        from bani.connectors.base import SinkConnector, SourceConnector
        from bani.connectors.registry import ConnectorRegistry

        connector_name = params.get("connector_name", "")
        if not connector_name:
            return _text_result(
                "Error: 'connector_name' parameter is required.", is_error=True
            )

        connector_class = ConnectorRegistry.get(str(connector_name))

        is_source = issubclass(connector_class, SourceConnector)
        is_sink = issubclass(connector_class, SinkConnector)

        capabilities: list[str] = []
        if is_source:
            capabilities.append("source")
        if is_sink:
            capabilities.append("sink")

        return _json_result(
            {
                "name": connector_name,
                "class": connector_class.__name__,
                "module": connector_class.__module__,
                "capabilities": capabilities,
                "docstring": (connector_class.__doc__ or "").strip(),
            }
        )
    except ValueError as exc:
        return _text_result(f"Error: {exc}", is_error=True)
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


def handle_generate_bdl(params: dict[str, Any]) -> ToolResult:
    """Generate a BDL XML template for a source/target pair."""
    try:
        source_connector = params.get("source_connector", "")
        target_connector = params.get("target_connector", "")

        if not source_connector or not target_connector:
            return _text_result(
                "Error: 'source_connector' and 'target_connector' are required.",
                is_error=True,
            )

        tables_param = params.get("tables")
        table_list: list[str] = []
        if isinstance(tables_param, list):
            table_list = [str(t) for t in tables_param]

        # Generate BDL XML template
        tables_xml = ""
        if table_list:
            table_entries = "\n".join(
                f'      <table sourceSchema="public" sourceTable="{t}" '
                f'targetSchema="public" targetTable="{t}" />'
                for t in table_list
            )
            tables_xml = f"\n    <tables>\n{table_entries}\n    </tables>"

        bdl_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<baniProject xmlns="https://bani.dev/bdl/1.0"
             name="{source_connector}-to-{target_connector}-migration">
    <description>Migration from {source_connector} to {target_connector}</description>

    <source dialect="{source_connector}">
        <host>localhost</host>
        <port>0</port>
        <database>source_db</database>
        <usernameEnv>SOURCE_USER</usernameEnv>
        <passwordEnv>SOURCE_PASS</passwordEnv>
    </source>

    <target dialect="{target_connector}">
        <host>localhost</host>
        <port>0</port>
        <database>target_db</database>
        <usernameEnv>TARGET_USER</usernameEnv>
        <passwordEnv>TARGET_PASS</passwordEnv>
    </target>{tables_xml}

    <options>
        <batchSize>100000</batchSize>
        <parallelWorkers>4</parallelWorkers>
    </options>
</baniProject>
"""
        return _text_result(bdl_xml.strip())
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS: tuple[ToolDefinition, ...] = (
    ToolDefinition(
        name="bani_schema_inspect",
        description=(
            "Introspect a database and return its schema (tables, columns, "
            "indexes, foreign keys)."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "connector": {
                    "type": "string",
                    "description": (
                        "Connector name, e.g. 'postgresql', 'mysql', 'mssql', "
                        "'oracle', 'sqlite'."
                    ),
                },
                "host": {
                    "type": "string",
                    "description": "Database host.",
                    "default": "",
                },
                "port": {
                    "type": "integer",
                    "description": "Database port.",
                    "default": 0,
                },
                "database": {
                    "type": "string",
                    "description": "Database name.",
                    "default": "",
                },
                "username_env": {
                    "type": "string",
                    "description": (
                        "Environment variable name holding the database username."
                    ),
                    "default": "",
                },
                "password_env": {
                    "type": "string",
                    "description": (
                        "Environment variable name holding the database password."
                    ),
                    "default": "",
                },
            },
            "required": ["connector"],
        },
    ),
    ToolDefinition(
        name="bani_validate_bdl",
        description="Validate a BDL document (XML or JSON) and return any errors.",
        input_schema={
            "type": "object",
            "properties": {
                "bdl_content": {
                    "type": "string",
                    "description": "BDL document content (XML or JSON string).",
                },
            },
            "required": ["bdl_content"],
        },
    ),
    ToolDefinition(
        name="bani_preview",
        description=(
            "Preview sample rows from a migration source defined in BDL content."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "bdl_content": {
                    "type": "string",
                    "description": "BDL document content (XML or JSON string).",
                },
                "sample_size": {
                    "type": "integer",
                    "description": "Number of sample rows per table.",
                    "default": 10,
                },
            },
            "required": ["bdl_content"],
        },
    ),
    ToolDefinition(
        name="bani_run",
        description="Execute a database migration defined in BDL content.",
        input_schema={
            "type": "object",
            "properties": {
                "bdl_content": {
                    "type": "string",
                    "description": "BDL document content (XML or JSON string).",
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "Validate without executing.",
                    "default": False,
                },
                "resume": {
                    "type": "boolean",
                    "description": "Resume from last checkpoint.",
                    "default": False,
                },
            },
            "required": ["bdl_content"],
        },
    ),
    ToolDefinition(
        name="bani_status",
        description="Check the checkpoint status of a migration project.",
        input_schema={
            "type": "object",
            "properties": {
                "project_name": {
                    "type": "string",
                    "description": "Name of the migration project.",
                },
            },
            "required": ["project_name"],
        },
    ),
    ToolDefinition(
        name="bani_connectors_list",
        description="List all available database connectors.",
        input_schema={
            "type": "object",
            "properties": {},
        },
    ),
    ToolDefinition(
        name="bani_connector_info",
        description="Get detailed information about a specific database connector.",
        input_schema={
            "type": "object",
            "properties": {
                "connector_name": {
                    "type": "string",
                    "description": (
                        "Name of the connector, e.g. 'postgresql', 'mysql'."
                    ),
                },
            },
            "required": ["connector_name"],
        },
    ),
    ToolDefinition(
        name="bani_generate_bdl",
        description=(
            "Generate a BDL XML template for a source-to-target migration."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "source_connector": {
                    "type": "string",
                    "description": "Source database connector name.",
                },
                "target_connector": {
                    "type": "string",
                    "description": "Target database connector name.",
                },
                "tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of table names to include.",
                },
            },
            "required": ["source_connector", "target_connector"],
        },
    ),
)

TOOL_HANDLERS: dict[str, ToolHandler] = {
    "bani_schema_inspect": handle_schema_inspect,
    "bani_validate_bdl": handle_validate_bdl,
    "bani_preview": handle_preview,
    "bani_run": handle_run,
    "bani_status": handle_status,
    "bani_connectors_list": handle_connectors_list,
    "bani_connector_info": handle_connector_info,
    "bani_generate_bdl": handle_generate_bdl,
}
