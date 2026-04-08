"""MCP tool definitions and handlers (Section 18.6).

Defines the 10 tools that the Bani MCP server exposes to AI agents:

 1. bani_connections     — list named database connections
 2. bani_connectors_list — list available connector engines
 3. bani_connector_info  — details about a specific connector
 4. bani_schema_inspect  — introspect a database schema
 5. bani_generate_bdl    — generate a BDL XML template
 6. bani_validate_bdl    — validate a BDL document
 7. bani_save_project    — save BDL to the projects directory
 8. bani_preview         — preview sample rows from a source
 9. bani_run             — execute a saved migration project
10. bani_status          — check checkpoint status for a project

**Typical workflow:**  bani_connections → bani_generate_bdl →
bani_validate_bdl → bani_save_project → bani_run.

``bani_connections`` returns named connection keys that are passed to
``bani_schema_inspect`` and ``bani_generate_bdl``.  ``bani_run`` loads
the project from disk by name, so ``bani_save_project`` must be called
first.

Each tool handler is a synchronous function that receives a ``params`` dict
and returns a ``ToolResult``.  Credential security is enforced: tools that
need database access accept environment variable *names* (not values), and
any parameter named ``password`` or ``credentials`` is rejected.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_DEFAULT_PROJECTS_DIR = "~/.bani/projects"

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


def handle_connections(params: dict[str, Any]) -> ToolResult:
    """List all named connections from the registry."""
    try:
        from bani.infra.connections import ConnectionRegistry

        connections = ConnectionRegistry.load()
        summaries = {
            key: ConnectionRegistry.safe_summary(conn)
            for key, conn in connections.items()
        }
        return _json_result(
            {"connections": summaries, "count": len(summaries)}
        )
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


def handle_schema_inspect(params: dict[str, Any]) -> ToolResult:
    """Introspect a database and return its schema as JSON."""
    try:
        from bani.sdk.schema_inspector import SchemaInspector

        connection_key = params.get("connection")
        if connection_key:
            # Resolve from the connections registry.
            from bani.infra.connections import ConnectionRegistry

            conn = ConnectionRegistry.get(str(connection_key))
            config = ConnectionRegistry.to_connection_config(conn)
            extra_kwargs = dict(config.extra)
            schema = SchemaInspector.inspect(
                dialect=config.dialect,
                host=config.host,
                port=config.port,
                database=config.database,
                username_env=config.username_env,
                password_env=config.password_env,
                **extra_kwargs,
            )
        else:
            # Fallback: individual parameters + env var names.
            security_err = _reject_plaintext_credentials(params)
            if security_err is not None:
                return security_err

            connector = params.get("connector", "")
            if not connector:
                return _text_result(
                    "Error: 'connection' or 'connector' "
                    "parameter is required.",
                    is_error=True,
                )

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


def handle_save_project(params: dict[str, Any]) -> ToolResult:
    """Save a BDL document to the projects directory."""
    try:
        name = str(params.get("name", "")).strip()
        if not name:
            return _text_result(
                "Error: 'name' parameter is required.",
                is_error=True,
            )

        bdl_content = str(params.get("bdl_content", "")).strip()
        if not bdl_content:
            return _text_result(
                "Error: 'bdl_content' parameter is required.",
                is_error=True,
            )

        overwrite = bool(params.get("overwrite", False))

        # Validate BDL before saving.
        if bdl_content.startswith("<") or bdl_content.startswith("<?"):
            from bani.bdl.validator import validate_xml

            errors = validate_xml(bdl_content)
        else:
            from bani.bdl.validator import validate_json

            errors = validate_json(bdl_content)

        if errors:
            return _json_result(
                {
                    "saved": False,
                    "errors": errors,
                    "message": "BDL validation failed. "
                    "Fix the errors and try again.",
                },
                is_error=True,
            )

        # Resolve projects directory and ensure it exists.
        projects_dir = Path(
            _DEFAULT_PROJECTS_DIR
        ).expanduser()
        projects_dir.mkdir(parents=True, exist_ok=True)

        file_path = projects_dir / f"{name}.bdl"
        if file_path.exists() and not overwrite:
            return _json_result(
                {
                    "saved": False,
                    "message": (
                        f"Project '{name}' already exists. "
                        f"Set overwrite=true to replace it."
                    ),
                    "path": str(file_path),
                },
                is_error=True,
            )

        file_path.write_text(bdl_content, encoding="utf-8")
        return _json_result(
            {
                "saved": True,
                "name": name,
                "path": str(file_path),
            }
        )
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


def handle_run(params: dict[str, Any]) -> ToolResult:
    """Execute a saved migration project by name."""
    try:
        from bani.application.progress import ProgressEvent
        from bani.sdk.bani import Bani

        # Pop the progress notifier injected by the server (not a user param).
        progress_notifier = params.pop("_progress_notifier", None)

        project_name = str(params.get("project_name", "")).strip()
        if not project_name:
            return _text_result(
                "Error: 'project_name' is required. "
                "Save your BDL first with bani_save_project, "
                "then run it by name.",
                is_error=True,
            )

        dry_run = bool(params.get("dry_run", False))
        resume = bool(params.get("resume", False))
        table_names = params.get("table_names")

        # Resolve project file from the projects directory.
        projects_dir = Path(
            _DEFAULT_PROJECTS_DIR
        ).expanduser()
        project_path = projects_dir / f"{project_name}.bdl"

        if not project_path.exists():
            return _text_result(
                f"Error: project '{project_name}' not found "
                f"at {project_path}. "
                f"Use bani_save_project to save it first.",
                is_error=True,
            )

        # Load project from disk (same path the UI uses).
        bani_project = Bani.load(str(project_path))
        project = bani_project._project

        # Apply table filter if provided.
        if isinstance(table_names, list) and table_names:
            from bani.domain.project import TableMapping

            mappings = []
            for name in table_names:
                parts = str(name).split(".", 1)
                if len(parts) == 2:
                    mappings.append(
                        TableMapping(
                            source_schema=parts[0],
                            source_table=parts[1],
                        )
                    )
                else:
                    mappings.append(
                        TableMapping(
                            source_schema="",
                            source_table=str(name),
                        )
                    )
            object.__setattr__(
                project, "table_mappings", tuple(mappings)
            )

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
                    "message": "Validation passed. "
                    "No migration executed.",
                    "project_name": project.name,
                }
            )

        # Build progress callback if the client provided a
        # progressToken.
        on_progress: Callable[[ProgressEvent], None] | None = None
        if progress_notifier is not None:
            on_progress = _make_event_callback(progress_notifier)

        # Execute — pass projects_dir so hooks and run-log
        # resolve correctly.
        result = bani_project.run(
            on_progress=on_progress,
            resume=resume,
            projects_dir=str(projects_dir),
        )
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
                "warnings": list(result.warnings),
            }
        )
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


def _make_event_callback(
    notifier: Callable[[int, int | None, str], None],
) -> Callable[..., None]:
    """Create an ``on_progress`` callback that emits MCP progress notifications.

    Uses **Option D** progress strategy:
    * ``progress`` / ``total`` = tables completed / total tables.
    * ``message`` carries human-readable detail including row-level info.

    Args:
        notifier: The ``(progress, total, message) -> None`` closure
            created by :pymethod:`McpServer._make_progress_notifier`.
    """
    # Mutable state shared across callback invocations.  Access is
    # thread-safe because the ProgressTracker serialises listener
    # calls behind its own lock.
    state: dict[str, int] = {"tables_done": 0, "total": 0}

    def _on_event(event: Any) -> None:
        if isinstance(event, MigrationStarted):
            state["total"] = event.table_count
            notifier(
                0,
                event.table_count,
                f"Migration started: {event.project_name} "
                f"({event.source_dialect} \u2192 {event.target_dialect}, "
                f"{event.table_count} tables)",
            )
        elif isinstance(event, IntrospectionComplete):
            notifier(
                0,
                state["total"],
                f"Schema introspection complete: {len(event.tables)} tables "
                f"from {event.source_dialect}",
            )
        elif isinstance(event, PhaseChange):
            notifier(
                state["tables_done"],
                state["total"],
                f"Phase: {event.phase}",
            )
        elif isinstance(event, TableStarted):
            est = (
                f", ~{event.estimated_rows:,} rows"
                if event.estimated_rows
                else ""
            )
            notifier(
                state["tables_done"],
                state["total"],
                f"Starting table '{event.table_name}'{est}",
            )
        elif isinstance(event, BatchComplete):
            notifier(
                state["tables_done"],
                state["total"],
                f"Table '{event.table_name}': "
                f"{event.rows_written:,} rows written (batch {event.batch_number})",
            )
        elif isinstance(event, TableComplete):
            state["tables_done"] += 1
            notifier(
                state["tables_done"],
                state["total"],
                f"Table '{event.table_name}' complete: "
                f"{event.total_rows_written:,} rows",
            )
        elif isinstance(event, TableCreateFailed):
            state["tables_done"] += 1
            notifier(
                state["tables_done"],
                state["total"],
                f"Table '{event.table_name}' failed: {event.reason}",
            )
        elif isinstance(event, MigrationComplete):
            notifier(
                state["tables_done"],
                state["total"],
                f"Migration complete: {event.tables_completed} tables, "
                f"{event.total_rows_written:,} rows in "
                f"{event.duration_seconds:.1f}s",
            )

    # Import event types into the closure's scope so isinstance checks work.
    from bani.application.progress import (
        BatchComplete,
        IntrospectionComplete,
        MigrationComplete,
        MigrationStarted,
        PhaseChange,
        TableComplete,
        TableCreateFailed,
        TableStarted,
    )

    return _on_event


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
        from bani.infra.connections import ConnectionRegistry

        # Resolve source connection.
        source_key = params.get("source_connection")
        if source_key:
            src = ConnectionRegistry.get(str(source_key))
            source_connector = src.connector
            src_cfg = ConnectionRegistry.to_connection_config(src)
        else:
            source_connector = params.get("source_connector", "")
            src_cfg = None

        # Resolve target connection.
        target_key = params.get("target_connection")
        if target_key:
            tgt = ConnectionRegistry.get(str(target_key))
            target_connector = tgt.connector
            tgt_cfg = ConnectionRegistry.to_connection_config(tgt)
        else:
            target_connector = params.get("target_connector", "")
            tgt_cfg = None

        if not source_connector or not target_connector:
            return _text_result(
                "Error: provide 'source_connection' or "
                "'source_connector', and "
                "'target_connection' or 'target_connector'.",
                is_error=True,
            )

        tables_param = params.get("tables")
        table_list: list[str] = []
        if isinstance(tables_param, list):
            table_list = [str(t) for t in tables_param]

        tables_xml = ""
        if table_list:
            table_entries = "\n".join(
                f'    <table sourceName="{t}"/>' for t in table_list
            )
            tables_xml = f"\n  <tables>\n{table_entries}\n  </tables>"

        # Build connection XML fragments.
        if src_cfg:
            src_xml = (
                f'    <connection host="{src_cfg.host}" '
                f'port="{src_cfg.port}" '
                f'database="{src_cfg.database}"\n'
                f'                username="${{env:{src_cfg.username_env}}}" '
                f'password="${{env:{src_cfg.password_env}}}" />'
            )
        else:
            src_xml = (
                '    <connection host="localhost" port="0" '
                'database="source_db"\n'
                '                username="${env:SOURCE_USER}" '
                'password="${env:SOURCE_PASS}" />'
            )

        if tgt_cfg:
            tgt_xml = (
                f'    <connection host="{tgt_cfg.host}" '
                f'port="{tgt_cfg.port}" '
                f'database="{tgt_cfg.database}"\n'
                f'                username="${{env:{tgt_cfg.username_env}}}" '
                f'password="${{env:{tgt_cfg.password_env}}}" />'
            )
        else:
            tgt_xml = (
                '    <connection host="localhost" port="0" '
                'database="target_db"\n'
                '                username="${env:TARGET_USER}" '
                'password="${env:TARGET_PASS}" />'
            )

        project_name = f"{source_connector}-to-{target_connector}"
        bdl_xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<bani schemaVersion="1.0">\n'
            f'  <project name="{project_name}"\n'
            f'           description="Migration from '
            f'{source_connector} to {target_connector}"/>\n'
            f'  <source connector="{source_connector}">\n'
            f"{src_xml}\n"
            f"  </source>\n"
            f'  <target connector="{target_connector}">\n'
            f"{tgt_xml}\n"
            f"  </target>{tables_xml}\n"
            f"</bani>"
        )
        return _text_result(bdl_xml)
    except Exception as exc:
        return _text_result(f"Error: {exc}", is_error=True)


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS: tuple[ToolDefinition, ...] = (
    # ------------------------------------------------------------------
    # Discovery tools — learn what connections and connectors exist
    # ------------------------------------------------------------------
    ToolDefinition(
        name="bani_connections",
        description=(
            "List all named database connections from the Bani "
            "connections registry (~/.bani/connections.json). "
            "Returns the connection key, display name, connector "
            "type, host, port, and database for each entry. "
            "Credentials are NOT included. "
            "Call this FIRST to discover available databases, "
            "then pass connection keys to bani_schema_inspect "
            "and bani_generate_bdl."
        ),
        input_schema={
            "type": "object",
            "properties": {},
        },
    ),
    ToolDefinition(
        name="bani_connectors_list",
        description=(
            "List all available connector engines "
            "(e.g. postgresql, mysql, mssql, oracle, sqlite)."
        ),
        input_schema={
            "type": "object",
            "properties": {},
        },
    ),
    ToolDefinition(
        name="bani_connector_info",
        description=(
            "Get details about a specific connector — capabilities "
            "(source, sink, or both) and documentation."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "connector_name": {
                    "type": "string",
                    "description": (
                        "Connector name, e.g. 'postgresql', "
                        "'mysql'."
                    ),
                },
            },
            "required": ["connector_name"],
        },
    ),
    ToolDefinition(
        name="bani_schema_inspect",
        description=(
            "Introspect a live database and return its schema "
            "(tables, columns, indexes, foreign keys, row count "
            "estimates). Preferred: pass a 'connection' key from "
            "bani_connections. Alternative: pass individual "
            "connector/host/port/database/username_env/password_env "
            "parameters."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "connection": {
                    "type": "string",
                    "description": (
                        "Connection key from bani_connections. "
                        "When provided, all connection details "
                        "are resolved automatically."
                    ),
                },
                "connector": {
                    "type": "string",
                    "description": (
                        "Connector name (only needed when "
                        "'connection' is not provided)."
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
                        "Env var name holding the username."
                    ),
                    "default": "",
                },
                "password_env": {
                    "type": "string",
                    "description": (
                        "Env var name holding the password."
                    ),
                    "default": "",
                },
            },
        },
    ),
    # ------------------------------------------------------------------
    # BDL authoring tools — create and validate migration definitions
    # ------------------------------------------------------------------
    ToolDefinition(
        name="bani_generate_bdl",
        description=(
            "Generate a BDL XML document for a source-to-target "
            "migration. Preferred: pass 'source_connection' and "
            "'target_connection' keys from bani_connections — the "
            "BDL will be pre-filled with real connection details. "
            "Alternative: pass 'source_connector' and "
            "'target_connector' names for a generic template."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "source_connection": {
                    "type": "string",
                    "description": (
                        "Source connection key from "
                        "bani_connections."
                    ),
                },
                "target_connection": {
                    "type": "string",
                    "description": (
                        "Target connection key from "
                        "bani_connections."
                    ),
                },
                "source_connector": {
                    "type": "string",
                    "description": (
                        "Source connector name (only when "
                        "'source_connection' is not provided)."
                    ),
                },
                "target_connector": {
                    "type": "string",
                    "description": (
                        "Target connector name (only when "
                        "'target_connection' is not provided)."
                    ),
                },
                "tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Optional list of table names to include."
                    ),
                },
            },
        },
    ),
    ToolDefinition(
        name="bani_validate_bdl",
        description=(
            "Validate a BDL document (XML or JSON) and return any "
            "errors. Call this before bani_save_project to catch "
            "problems early."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "bdl_content": {
                    "type": "string",
                    "description": (
                        "BDL document content (XML or JSON string)."
                    ),
                },
            },
            "required": ["bdl_content"],
        },
    ),
    # ------------------------------------------------------------------
    # Project management — save, preview, execute, and monitor
    # ------------------------------------------------------------------
    ToolDefinition(
        name="bani_save_project",
        description=(
            "Save a BDL document to the Bani projects directory "
            "(~/.bani/projects/<name>.bdl). The project becomes "
            "visible in the Bani dashboard and can be executed "
            "with bani_run. The BDL is validated before saving — "
            "invalid documents are rejected."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": (
                        "Project name (used as the filename and "
                        "referenced by bani_run)."
                    ),
                },
                "bdl_content": {
                    "type": "string",
                    "description": (
                        "BDL document content (XML or JSON string)."
                    ),
                },
                "overwrite": {
                    "type": "boolean",
                    "description": (
                        "If true, overwrite an existing project "
                        "with the same name. Default false."
                    ),
                    "default": False,
                },
            },
            "required": ["name", "bdl_content"],
        },
    ),
    ToolDefinition(
        name="bani_preview",
        description=(
            "Preview sample rows from a migration source defined "
            "in BDL content. Useful for verifying connectivity and "
            "inspecting data before running a full migration."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "bdl_content": {
                    "type": "string",
                    "description": (
                        "BDL document content (XML or JSON string)."
                    ),
                },
                "sample_size": {
                    "type": "integer",
                    "description": (
                        "Number of sample rows per table."
                    ),
                    "default": 10,
                },
            },
            "required": ["bdl_content"],
        },
    ),
    ToolDefinition(
        name="bani_run",
        description=(
            "Execute a saved migration project. The project must "
            "already exist in ~/.bani/projects/ — use "
            "bani_save_project to save it first. "
            "Workflow: bani_generate_bdl → bani_validate_bdl → "
            "bani_save_project → bani_run."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "project_name": {
                    "type": "string",
                    "description": (
                        "Name of the saved project to run "
                        "(matches the name used in "
                        "bani_save_project)."
                    ),
                },
                "dry_run": {
                    "type": "boolean",
                    "description": (
                        "Validate the project without executing "
                        "the migration."
                    ),
                    "default": False,
                },
                "resume": {
                    "type": "boolean",
                    "description": (
                        "Resume from the last checkpoint. "
                        "Completed tables are skipped."
                    ),
                    "default": False,
                },
                "table_names": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Optional list of specific tables to "
                        "migrate (schema.table or table). "
                        "Omit to migrate all tables."
                    ),
                },
            },
            "required": ["project_name"],
        },
    ),
    ToolDefinition(
        name="bani_status",
        description=(
            "Check the checkpoint status of a migration project. "
            "Shows which tables have been completed, failed, or "
            "are pending — useful before deciding whether to "
            "resume a migration with bani_run."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "project_name": {
                    "type": "string",
                    "description": (
                        "Name of the migration project."
                    ),
                },
            },
            "required": ["project_name"],
        },
    ),
)

TOOL_HANDLERS: dict[str, ToolHandler] = {
    "bani_connections": handle_connections,
    "bani_connectors_list": handle_connectors_list,
    "bani_connector_info": handle_connector_info,
    "bani_schema_inspect": handle_schema_inspect,
    "bani_generate_bdl": handle_generate_bdl,
    "bani_validate_bdl": handle_validate_bdl,
    "bani_save_project": handle_save_project,
    "bani_preview": handle_preview,
    "bani_run": handle_run,
    "bani_status": handle_status,
}
