"""BDL serialization to XML."""

from __future__ import annotations

import xml.etree.ElementTree as ET

from bani.domain.project import ProjectModel


def serialize(project: ProjectModel) -> str:
    """Serialize ProjectModel to BDL XML string.

    Args:
        project: ProjectModel to serialize.

    Returns:
        XML string representation.
    """
    root = ET.Element("bani")
    root.set("schemaVersion", "1.0")
    root.set("xmlns", "https://bani.tools/bdl/1.0")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.set(
        "xsi:schemaLocation",
        "https://bani.tools/bdl/1.0 bdl-1.0.xsd",
    )

    # Project
    project_elem = ET.SubElement(root, "project")
    project_elem.set("name", project.name)
    if project.description:
        project_elem.set("description", project.description)
    if project.author:
        project_elem.set("author", project.author)
    if project.created is not None:
        created_str = project.created.isoformat()
        if created_str.endswith("+00:00"):
            created_str = created_str.replace("+00:00", "Z")
        project_elem.set("created", created_str)

    # Tags
    if project.tags:
        tags_elem = ET.SubElement(project_elem, "tags")
        for tag in project.tags:
            tag_elem = ET.SubElement(tags_elem, "tag")
            tag_elem.text = tag

    # Source
    if project.source:
        source_elem = ET.SubElement(root, "source")
        source_elem.set("connector", project.source.dialect)
        _serialize_connection(source_elem, project.source)

    # Target
    if project.target:
        target_elem = ET.SubElement(root, "target")
        target_elem.set("connector", project.target.dialect)
        _serialize_connection(target_elem, project.target)

    # Options
    if project.options is not None:
        options_elem = ET.SubElement(root, "options")
        _serialize_options(options_elem, project.options)

    # Type Mappings
    if project.type_overrides:
        type_mappings_elem = ET.SubElement(root, "typeMappings")
        for override in project.type_overrides:
            mapping_elem = ET.SubElement(type_mappings_elem, "mapping")
            mapping_elem.set("source", override.source_type)
            mapping_elem.set("target", override.target_type)

    # Tables
    if project.table_mappings:
        tables_elem = ET.SubElement(root, "tables")
        for table in project.table_mappings:
            _serialize_table(tables_elem, table)

    # Hooks
    if project.hooks:
        hooks_elem = ET.SubElement(root, "hooks")
        for hook in project.hooks:
            hook_elem = ET.SubElement(hooks_elem, "hook")
            if hook.name:
                hook_elem.set("name", hook.name)
            hook_elem.set("event", hook.event)
            hook_elem.set("type", hook.hook_type)
            if hook.target:
                hook_elem.set("target", hook.target)
            if hook.table_name:
                hook_elem.set("tableName", hook.table_name)
            if hook.on_failure != "abort":
                hook_elem.set("onFailure", hook.on_failure)
            hook_elem.text = hook.command

    # Schedule
    if project.schedule is not None:
        schedule_elem = ET.SubElement(root, "schedule")
        schedule_elem.set("enabled", "true" if project.schedule.enabled else "false")
        if project.schedule.cron:
            cron_elem = ET.SubElement(schedule_elem, "cron")
            cron_elem.text = project.schedule.cron
        tz_elem = ET.SubElement(schedule_elem, "timezone")
        tz_elem.text = project.schedule.timezone
        if project.schedule.max_retries > 0:
            retry_elem = ET.SubElement(schedule_elem, "retryOnFailure")
            retry_elem.set("maxRetries", str(project.schedule.max_retries))
            retry_elem.set("delaySeconds", str(project.schedule.retry_delay_seconds))

    # Sync
    if project.sync is not None:
        sync_elem = ET.SubElement(root, "sync")
        sync_elem.set("enabled", "true" if project.sync.enabled else "false")
        if project.sync.enabled:
            strategy_elem = ET.SubElement(sync_elem, "strategy")
            strategy_elem.text = project.sync.strategy.name.lower()
            for tracking_col in project.sync.tracking_columns:
                tracking_elem = ET.SubElement(sync_elem, "trackingColumn")
                tracking_elem.set("table", tracking_col[0])
                tracking_elem.set("column", tracking_col[1])

    # Pretty-print
    indent_tree(root)
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(
        root, encoding="unicode"
    )


def _serialize_connection(parent: ET.Element, conn: object) -> None:
    """Serialize connection config."""
    from bani.domain.project import ConnectionConfig

    if not isinstance(conn, ConnectionConfig):
        return

    conn_elem = ET.SubElement(parent, "connection")
    conn_elem.set("host", conn.host)
    conn_elem.set("port", str(conn.port))
    conn_elem.set("database", conn.database)
    conn_elem.set("username", conn.username_env)
    conn_elem.set("password", conn.password_env)
    if conn.encrypt:
        conn_elem.set("encrypt", "true")

    if conn.extra:
        config_elem = ET.SubElement(parent, "connectorConfig")
        for name, value in conn.extra:
            option_elem = ET.SubElement(config_elem, "option")
            option_elem.set("name", name)
            option_elem.set("value", value)


def _serialize_options(parent: ET.Element, opts: object) -> None:
    """Serialize options."""
    from bani.domain.project import ProjectOptions

    if not isinstance(opts, ProjectOptions):
        return

    batch_elem = ET.SubElement(parent, "batchSize")
    batch_elem.text = str(opts.batch_size)

    workers_elem = ET.SubElement(parent, "parallelWorkers")
    workers_elem.text = str(opts.parallel_workers)

    memory_elem = ET.SubElement(parent, "memoryLimitMB")
    memory_elem.text = str(opts.memory_limit_mb)

    error_elem = ET.SubElement(parent, "onError")
    error_elem.text = opts.on_error.value

    schema_elem = ET.SubElement(parent, "createTargetSchema")
    schema_elem.text = "true" if opts.create_target_schema else "false"

    drop_elem = ET.SubElement(parent, "dropTargetTablesFirst")
    drop_elem.text = "true" if opts.drop_target_tables_first else "false"

    indexes_elem = ET.SubElement(parent, "transferIndexes")
    indexes_elem.text = "true" if opts.transfer_indexes else "false"

    fks_elem = ET.SubElement(parent, "transferForeignKeys")
    fks_elem.text = "true" if opts.transfer_foreign_keys else "false"

    defaults_elem = ET.SubElement(parent, "transferDefaults")
    defaults_elem.text = "true" if opts.transfer_defaults else "false"

    checks_elem = ET.SubElement(parent, "transferCheckConstraints")
    checks_elem.text = "true" if opts.transfer_check_constraints else "false"


def _serialize_columns(parent: ET.Element, columns: tuple[object, ...]) -> None:
    """Serialize column mappings."""
    from bani.domain.project import ColumnMapping

    col_mappings_elem = ET.SubElement(parent, "columnMappings")
    for col in columns:
        if not isinstance(col, ColumnMapping):
            continue
        col_elem = ET.SubElement(col_mappings_elem, "column")
        col_elem.set("source", col.source_name)
        col_elem.set("target", col.target_name)
        if col.target_type:
            col_elem.set("targetType", col.target_type)


def _serialize_table(parent: ET.Element, table: object) -> None:
    """Serialize table mapping."""
    from bani.domain.project import ColumnMapping, TableMapping

    if not isinstance(table, TableMapping):
        return

    table_elem = ET.SubElement(parent, "table")
    table_elem.set("sourceSchema", table.source_schema)
    table_elem.set("sourceName", table.source_table)
    table_elem.set("targetName", table.target_table)
    if table.target_schema != table.source_schema:
        table_elem.set("targetSchema", table.target_schema)

    if table.column_mappings:
        col_mappings_elem = ET.SubElement(table_elem, "columnMappings")
        for column in table.column_mappings:
            if isinstance(column, ColumnMapping):
                col_elem = ET.SubElement(col_mappings_elem, "column")
                col_elem.set("source", column.source_name)
                col_elem.set("target", column.target_name)
                if column.target_type:
                    col_elem.set("targetType", column.target_type)

    if table.filter_sql:
        filter_elem = ET.SubElement(table_elem, "filter")
        filter_elem.text = table.filter_sql


def indent_tree(elem: ET.Element, level: int = 0) -> None:
    """Add indentation to XML tree for pretty-printing."""
    indent_str = "\n" + ("  " * level)
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = indent_str + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = indent_str
        for child in elem:
            indent_tree(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = indent_str
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = indent_str
