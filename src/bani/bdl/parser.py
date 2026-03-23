"""BDL XML and JSON parser."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any

from bani.bdl.interpolator import interpolate, interpolate_dict
from bani.domain.errors import BDLValidationError
from bani.domain.project import (
    ColumnMapping,
    ConnectionConfig,
    ErrorHandlingStrategy,
    HookConfig,
    ProjectModel,
    ProjectOptions,
    ScheduleConfig,
    SyncConfig,
    SyncStrategy,
    TableMapping,
    TypeMappingOverride,
)

NS = {"b": "https://bani.dev/bdl/1.0"}


def parse(source: str | Path) -> ProjectModel:
    """Parse BDL from file or string, auto-detecting format.

    Args:
        source: File path or BDL content string.

    Returns:
        Parsed ProjectModel.

    Raises:
        BDLValidationError: If parsing fails.
    """
    if isinstance(source, Path):
        with open(source) as f:
            content = f.read()
    else:
        content = source

    content = content.strip()
    if content.startswith("<") or content.startswith("<?"):
        return parse_xml(content)
    else:
        return parse_json(content)


def parse_xml(xml_content: str) -> ProjectModel:
    """Parse BDL XML to ProjectModel.

    Args:
        xml_content: XML content string.

    Returns:
        Parsed ProjectModel.

    Raises:
        BDLValidationError: If parsing fails.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        raise BDLValidationError(f"XML parsing error: {e}") from e

    schema_version = root.get("schemaVersion", "1.0")
    if schema_version != "1.0":
        raise BDLValidationError(f"Unsupported schema version: {schema_version}")

    project_elem = root.find("b:project", NS)
    if project_elem is None:
        project_elem = root.find("project")

    if project_elem is None:
        raise BDLValidationError("Missing required 'project' element")

    name = project_elem.get("name")
    if not name:
        raise BDLValidationError("Project 'name' attribute is required")

    description = project_elem.get("description", "")
    author = project_elem.get("author", "")
    created_str = project_elem.get("created")
    created: datetime | None = None
    if created_str:
        try:
            created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        except ValueError:
            created = None

    tags_elem = project_elem.find("b:tags", NS)
    if tags_elem is None:
        tags_elem = project_elem.find("tags")
    tags: tuple[str, ...] = ()
    if tags_elem is not None:
        tag_elems = tags_elem.findall("b:tag", NS)
        if not tag_elems:
            tag_elems = tags_elem.findall("tag")
        tags = tuple(t.text or "" for t in tag_elems if t.text)

    source_elem = root.find("b:source", NS)
    if source_elem is None:
        source_elem = root.find("source")
    source: ConnectionConfig | None = None
    if source_elem is not None:
        source = _parse_connection(source_elem)

    target_elem = root.find("b:target", NS)
    if target_elem is None:
        target_elem = root.find("target")
    target: ConnectionConfig | None = None
    if target_elem is not None:
        target = _parse_connection(target_elem)

    options_elem = root.find("b:options", NS)
    if options_elem is None:
        options_elem = root.find("options")
    if options_elem is not None:
        options = _parse_options(options_elem)
    else:
        options = ProjectOptions()

    type_mappings_elem = root.find("b:typeMappings", NS)
    if type_mappings_elem is None:
        type_mappings_elem = root.find("typeMappings")
    type_overrides: tuple[TypeMappingOverride, ...] = ()
    if type_mappings_elem is not None:
        mapping_elems = type_mappings_elem.findall("b:mapping", NS)
        if not mapping_elems:
            mapping_elems = type_mappings_elem.findall("mapping")
        type_overrides = tuple(
            TypeMappingOverride(
                source_type=m.get("source") or "",
                target_type=m.get("target") or "",
            )
            for m in mapping_elems
        )

    tables_elem = root.find("b:tables", NS)
    if tables_elem is None:
        tables_elem = root.find("tables")
    table_mappings: tuple[TableMapping, ...] = ()
    if tables_elem is not None:
        table_mappings = _parse_tables(tables_elem)

    hooks_elem = root.find("b:hooks", NS)
    if hooks_elem is None:
        hooks_elem = root.find("hooks")
    hooks: tuple[HookConfig, ...] = ()
    if hooks_elem is not None:
        hook_elems = hooks_elem.findall("b:hook", NS)
        if not hook_elems:
            hook_elems = hooks_elem.findall("hook")
        hooks = tuple(_parse_hook(h) for h in hook_elems)

    schedule_elem = root.find("b:schedule", NS)
    if schedule_elem is None:
        schedule_elem = root.find("schedule")
    if schedule_elem is not None:
        schedule = _parse_schedule(schedule_elem)
    else:
        schedule = ScheduleConfig()

    sync_elem = root.find("b:sync", NS)
    if sync_elem is None:
        sync_elem = root.find("sync")
    sync = _parse_sync(sync_elem) if sync_elem is not None else SyncConfig()

    return ProjectModel(
        name=name,
        description=description,
        author=author,
        created=created,
        tags=tags,
        source=source,
        target=target,
        table_mappings=table_mappings,
        type_overrides=type_overrides,
        options=options,
        hooks=hooks,
        schedule=schedule,
        sync=sync,
    )


def parse_json(json_content: str) -> ProjectModel:
    """Parse BDL JSON to ProjectModel.

    Args:
        json_content: JSON content string.

    Returns:
        Parsed ProjectModel.

    Raises:
        BDLValidationError: If parsing fails.
    """
    try:
        data = json.loads(json_content)
    except json.JSONDecodeError as e:
        raise BDLValidationError(f"JSON parsing error: {e}") from e

    # Interpolate environment variables
    data = interpolate_dict(data)

    schema_version = data.get("schemaVersion", "1.0")
    if schema_version != "1.0":
        raise BDLValidationError(f"Unsupported schema version: {schema_version}")

    project_data = data.get("project")
    if not project_data:
        raise BDLValidationError("Missing required 'project' key")

    name = project_data.get("name")
    if not name:
        raise BDLValidationError("Project 'name' is required")

    description = project_data.get("description", "")
    author = project_data.get("author", "")
    created_str = project_data.get("created")
    created: datetime | None = None
    if created_str:
        try:
            created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            created = None

    tags_data = project_data.get("tags", [])
    tags = tuple(tags_data) if isinstance(tags_data, list) else ()

    source_data = data.get("source")
    source = _parse_connection_json(source_data) if source_data else None

    target_data = data.get("target")
    target = _parse_connection_json(target_data) if target_data else None

    options_data = data.get("options", {})
    options = _parse_options_json(options_data)

    type_mappings_data = data.get("typeMappings", [])
    type_overrides = tuple(
        TypeMappingOverride(
            source_type=m.get("source", ""),
            target_type=m.get("target", ""),
        )
        for m in type_mappings_data
    )

    tables_data = data.get("tables", [])
    table_mappings = tuple(_parse_table_json(t) for t in tables_data)

    hooks_data = data.get("hooks", [])
    hooks = tuple(_parse_hook_json(h) for h in hooks_data)

    schedule_data = data.get("schedule")
    if schedule_data:
        schedule = _parse_schedule_json(schedule_data)
    else:
        schedule = ScheduleConfig()

    sync_data = data.get("sync")
    sync = _parse_sync_json(sync_data) if sync_data else SyncConfig()

    return ProjectModel(
        name=name,
        description=description,
        author=author,
        created=created,
        tags=tags,
        source=source,
        target=target,
        table_mappings=table_mappings,
        type_overrides=type_overrides,
        options=options,
        hooks=hooks,
        schedule=schedule,
        sync=sync,
    )


def _parse_connection(elem: ET.Element) -> ConnectionConfig:
    """Parse a connection element."""
    connector = elem.get("connector", "")
    conn_elem = elem.find("b:connection", NS)
    if conn_elem is None:
        conn_elem = elem.find("connection")

    if conn_elem is None:
        raise BDLValidationError("Missing 'connection' element")

    host = conn_elem.get("host", "")
    port = int(conn_elem.get("port", "0"))
    database = conn_elem.get("database", "")
    username_env = conn_elem.get("username", "")
    password_env = conn_elem.get("password", "")
    encrypt = conn_elem.get("encrypt", "false").lower() == "true"

    extra: dict[str, str] = {}
    config_elem = elem.find("b:connectorConfig", NS)
    if config_elem is None:
        config_elem = elem.find("connectorConfig")
    if config_elem is not None:
        option_elems = config_elem.findall("b:option", NS)
        if not option_elems:
            option_elems = config_elem.findall("option")
        for opt in option_elems:
            name = opt.get("name", "")
            value = opt.get("value", "")
            if name and value:
                extra[name] = interpolate(value)

    # Interpolate connection strings
    username_env = interpolate(username_env)
    password_env = interpolate(password_env)

    return ConnectionConfig(
        dialect=connector,
        host=host,
        port=port,
        database=database,
        username_env=username_env,
        password_env=password_env,
        encrypt=encrypt,
        extra=tuple(extra.items()),
    )


def _parse_connection_json(data: dict[str, Any]) -> ConnectionConfig:
    """Parse connection from JSON data."""
    connector = data.get("connector", "")
    conn_data = data.get("connection", {})

    host = conn_data.get("host", "")
    port = int(conn_data.get("port", 0))
    database = conn_data.get("database", "")
    username_env = conn_data.get("username", "")
    password_env = conn_data.get("password", "")
    encrypt = conn_data.get("encrypt", False)

    extra: dict[str, str] = {}
    config_data = data.get("connectorConfig", {})
    if config_data:
        for opt in config_data.get("option", []):
            name = opt.get("name", "")
            value = opt.get("value", "")
            if name:
                extra[name] = value

    return ConnectionConfig(
        dialect=connector,
        host=host,
        port=port,
        database=database,
        username_env=username_env,
        password_env=password_env,
        encrypt=encrypt,
        extra=tuple(extra.items()),
    )


def _parse_options(elem: ET.Element) -> ProjectOptions:
    """Parse options element."""

    def get_int(tag: str, default: int) -> int:
        e = elem.find(f"b:{tag}", NS)
        if e is None:
            e = elem.find(tag)
        return int(e.text or default) if e is not None and e.text else default

    def get_bool(tag: str, default: bool) -> bool:
        e = elem.find(f"b:{tag}", NS)
        if e is None:
            e = elem.find(tag)
        if e is None:
            return default
        text = (e.text or "").lower()
        return text in ("true", "1", "yes")

    def get_str(tag: str, default: str) -> str:
        e = elem.find(f"b:{tag}", NS)
        if e is None:
            e = elem.find(tag)
        return (e.text or default) if e is not None and e.text else default

    on_error_str = get_str("onError", "log-and-continue")
    on_error = ErrorHandlingStrategy.LOG_AND_CONTINUE
    if on_error_str == "fail-fast":
        on_error = ErrorHandlingStrategy.ABORT

    return ProjectOptions(
        batch_size=get_int("batchSize", 100000),
        parallel_workers=get_int("parallelWorkers", 4),
        memory_limit_mb=get_int("memoryLimitMB", 2048),
        on_error=on_error,
        create_target_schema=get_bool("createTargetSchema", True),
        drop_target_tables_first=get_bool("dropTargetTablesFirst", False),
        transfer_indexes=get_bool("transferIndexes", True),
        transfer_foreign_keys=get_bool("transferForeignKeys", True),
        transfer_defaults=get_bool("transferDefaults", True),
        transfer_check_constraints=get_bool("transferCheckConstraints", True),
    )


def _parse_options_json(data: dict[str, Any]) -> ProjectOptions:
    """Parse options from JSON data."""
    on_error_str = data.get("onError", "log-and-continue")
    on_error = ErrorHandlingStrategy.LOG_AND_CONTINUE
    if on_error_str == "fail-fast":
        on_error = ErrorHandlingStrategy.ABORT

    return ProjectOptions(
        batch_size=data.get("batchSize", 100000),
        parallel_workers=data.get("parallelWorkers", 4),
        memory_limit_mb=data.get("memoryLimitMB", 2048),
        on_error=on_error,
        create_target_schema=data.get("createTargetSchema", True),
        drop_target_tables_first=data.get("dropTargetTablesFirst", False),
        transfer_indexes=data.get("transferIndexes", True),
        transfer_foreign_keys=data.get("transferForeignKeys", True),
        transfer_defaults=data.get("transferDefaults", True),
        transfer_check_constraints=data.get("transferCheckConstraints", True),
    )


def _parse_tables(elem: ET.Element) -> tuple[TableMapping, ...]:
    """Parse tables element."""
    table_elems = elem.findall("b:table", NS)
    if not table_elems:
        table_elems = elem.findall("table")

    return tuple(_parse_table(t) for t in table_elems)


def _parse_table(elem: ET.Element) -> TableMapping:
    """Parse a single table element."""
    source_schema = elem.get("sourceSchema", "")
    source_name = elem.get("sourceName", "")
    target_name = elem.get("targetName", source_name)
    target_schema = elem.get("targetSchema", source_schema)

    filter_elem = elem.find("b:filter", NS)
    if filter_elem is None:
        filter_elem = elem.find("filter")
    filter_sql = filter_elem.text if filter_elem is not None else None

    column_mappings: tuple[ColumnMapping, ...] = ()
    col_mappings_elem = elem.find("b:columnMappings", NS)
    if col_mappings_elem is None:
        col_mappings_elem = elem.find("columnMappings")
    if col_mappings_elem is not None:
        col_elems = col_mappings_elem.findall("b:column", NS)
        if not col_elems:
            col_elems = col_mappings_elem.findall("column")
        column_mappings = tuple(
            ColumnMapping(
                source_name=c.get("source", ""),
                target_name=c.get("target", ""),
                target_type=c.get("targetType"),
            )
            for c in col_elems
        )

    return TableMapping(
        source_schema=source_schema,
        source_table=source_name,
        target_schema=target_schema,
        target_table=target_name,
        column_mappings=column_mappings,
        filter_sql=filter_sql,
    )


def _parse_table_json(data: dict[str, Any]) -> TableMapping:
    """Parse table from JSON data."""
    source_schema = data.get("sourceSchema", "")
    source_name = data.get("sourceName", "")
    target_name = data.get("targetName", source_name)
    target_schema = data.get("targetSchema", source_schema)
    filter_sql = data.get("filter")

    column_mappings: tuple[ColumnMapping, ...] = ()
    col_mappings = data.get("columnMappings", [])
    if col_mappings:
        column_mappings = tuple(
            ColumnMapping(
                source_name=c.get("source", ""),
                target_name=c.get("target", ""),
                target_type=c.get("targetType"),
            )
            for c in col_mappings
        )

    return TableMapping(
        source_schema=source_schema,
        source_table=source_name,
        target_schema=target_schema,
        target_table=target_name,
        column_mappings=column_mappings,
        filter_sql=filter_sql,
    )


def _parse_hook(elem: ET.Element) -> HookConfig:
    """Parse a hook element."""
    name = elem.get("name", "")
    event = elem.get("event", "")
    timeout = int(elem.get("timeout", "300"))
    on_failure = elem.get("onFailure", "fail")

    command = elem.text or ""

    return HookConfig(
        name=name,
        phase=event,
        command=command.strip(),
        timeout_seconds=timeout,
        on_failure=on_failure,
    )


def _parse_hook_json(data: dict[str, Any]) -> HookConfig:
    """Parse hook from JSON data."""
    return HookConfig(
        name=data.get("name", ""),
        phase=data.get("event", ""),
        command=data.get("command", ""),
        timeout_seconds=data.get("timeout", 300),
        on_failure=data.get("onFailure", "fail"),
    )


def _parse_schedule(elem: ET.Element) -> ScheduleConfig:
    """Parse schedule element."""
    enabled = elem.get("enabled", "false").lower() == "true"

    cron_elem = elem.find("b:cron", NS)
    if cron_elem is None:
        cron_elem = elem.find("cron")
    cron = cron_elem.text if cron_elem is not None else ""

    tz_elem = elem.find("b:timezone", NS)
    if tz_elem is None:
        tz_elem = elem.find("timezone")
    timezone = tz_elem.text or "UTC" if tz_elem is not None else "UTC"

    retry_elem = elem.find("b:retryOnFailure", NS)
    if retry_elem is None:
        retry_elem = elem.find("retryOnFailure")

    max_retries = 0
    retry_delay = 0
    if retry_elem is not None:
        max_retries = int(retry_elem.get("maxRetries", "0"))
        retry_delay = int(retry_elem.get("delaySeconds", "0"))

    return ScheduleConfig(
        enabled=enabled,
        cron=cron,
        timezone=timezone,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay,
    )


def _parse_schedule_json(data: dict[str, Any]) -> ScheduleConfig:
    """Parse schedule from JSON data."""
    enabled = data.get("enabled", False)
    cron = data.get("cron", "")
    timezone = data.get("timezone", "UTC")

    retry_data = data.get("retryOnFailure", {})
    max_retries = retry_data.get("maxRetries", 0) if retry_data else 0
    retry_delay = retry_data.get("delaySeconds", 0) if retry_data else 0

    return ScheduleConfig(
        enabled=enabled,
        cron=cron,
        timezone=timezone,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay,
    )


def _parse_sync(elem: ET.Element) -> SyncConfig:
    """Parse sync element."""
    enabled = elem.get("enabled", "false").lower() == "true"

    strategy_elem = elem.find("b:strategy", NS)
    if strategy_elem is None:
        strategy_elem = elem.find("strategy")
    strategy_str = strategy_elem.text or "full" if strategy_elem is not None else "full"

    # Convert string value to SyncStrategy enum
    strategy_map = {
        "full": SyncStrategy.FULL,
        "timestamp": SyncStrategy.TIMESTAMP,
        "rowversion": SyncStrategy.ROWVERSION,
        "checksum": SyncStrategy.CHECKSUM,
    }
    strategy = strategy_map.get(strategy_str.lower(), SyncStrategy.FULL)

    tracking_elems = elem.findall("b:trackingColumn", NS)
    if not tracking_elems:
        tracking_elems = elem.findall("trackingColumn")

    tracking_columns: tuple[tuple[str, str], ...] = tuple(
        (t.get("table", ""), t.get("column", "")) for t in tracking_elems
    )

    return SyncConfig(
        enabled=enabled,
        strategy=strategy,
        tracking_columns=tracking_columns,
    )


def _parse_sync_json(data: dict[str, Any]) -> SyncConfig:
    """Parse sync from JSON data."""
    enabled = data.get("enabled", False)
    strategy_str = data.get("strategy", "full")

    # Convert string value to SyncStrategy enum
    strategy_map = {
        "full": SyncStrategy.FULL,
        "timestamp": SyncStrategy.TIMESTAMP,
        "rowversion": SyncStrategy.ROWVERSION,
        "checksum": SyncStrategy.CHECKSUM,
    }
    strategy = strategy_map.get(strategy_str.lower(), SyncStrategy.FULL)

    tracking_data = data.get("trackingColumns", [])
    tracking_columns: tuple[tuple[str, str], ...] = tuple(
        (t.get("table", ""), t.get("column", "")) for t in tracking_data
    )

    return SyncConfig(
        enabled=enabled,
        strategy=strategy,
        tracking_columns=tracking_columns,
    )
