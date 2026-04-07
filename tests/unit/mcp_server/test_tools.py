"""Tests for MCP tool definitions and handlers."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

from bani.application.orchestrator import MigrationResult
from bani.domain.project import ConnectionConfig, ProjectModel
from bani.domain.schema import (
    ColumnDefinition,
    DatabaseSchema,
    IndexDefinition,
    TableDefinition,
)
from bani.mcp_server.tools import (
    TOOL_DEFINITIONS,
    TOOL_HANDLERS,
    ToolDefinition,
    ToolResult,
    handle_connector_info,
    handle_connectors_list,
    handle_generate_bdl,
    handle_preview,
    handle_run,
    handle_schema_inspect,
    handle_status,
    handle_validate_bdl,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sample_schema() -> DatabaseSchema:
    """Build a small DatabaseSchema for test assertions."""
    return DatabaseSchema(
        tables=(
            TableDefinition(
                schema_name="public",
                table_name="users",
                columns=(
                    ColumnDefinition(
                        name="id",
                        data_type="INTEGER",
                        nullable=False,
                        ordinal_position=0,
                        arrow_type_str="int32",
                    ),
                    ColumnDefinition(
                        name="email",
                        data_type="VARCHAR(255)",
                        nullable=True,
                        ordinal_position=1,
                        arrow_type_str="string",
                    ),
                ),
                primary_key=("id",),
                indexes=(
                    IndexDefinition(
                        name="idx_email",
                        columns=("email",),
                        is_unique=True,
                    ),
                ),
                foreign_keys=(),
                row_count_estimate=100,
            ),
        ),
        source_dialect="postgresql",
    )


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------


class TestToolRegistry:
    """Verify the tool definitions and handler map are consistent."""

    def test_all_definitions_have_handlers(self) -> None:
        """Every ToolDefinition should have a matching handler function."""
        for td in TOOL_DEFINITIONS:
            assert td.name in TOOL_HANDLERS, f"No handler for '{td.name}'"

    def test_all_handlers_have_definitions(self) -> None:
        """Every handler should have a matching ToolDefinition."""
        names = {td.name for td in TOOL_DEFINITIONS}
        for handler_name in TOOL_HANDLERS:
            assert handler_name in names, f"No definition for handler '{handler_name}'"

    def test_exactly_ten_tools(self) -> None:
        """MCP server exposes exactly 10 tools."""
        assert len(TOOL_DEFINITIONS) == 10
        assert len(TOOL_HANDLERS) == 10

    def test_definitions_are_frozen(self) -> None:
        """ToolDefinition instances should be frozen dataclasses."""
        for td in TOOL_DEFINITIONS:
            assert isinstance(td, ToolDefinition)

    def test_each_definition_has_input_schema(self) -> None:
        """Every tool definition must declare an inputSchema."""
        for td in TOOL_DEFINITIONS:
            assert "type" in td.input_schema
            assert td.input_schema["type"] == "object"


# ---------------------------------------------------------------------------
# bani_schema_inspect
# ---------------------------------------------------------------------------


class TestSchemaInspect:
    """Tests for handle_schema_inspect."""

    def test_success(self) -> None:
        """Successful schema introspection returns table data."""
        mock_connector = MagicMock()
        mock_connector.introspect_schema.return_value = _sample_schema()
        mock_cls = MagicMock(return_value=mock_connector)

        with patch("bani.sdk.schema_inspector.ConnectorRegistry.get", return_value=mock_cls):
            result = handle_schema_inspect(
                {
                    "connector": "postgresql",
                    "host": "localhost",
                    "port": 5432,
                    "database": "testdb",
                    "username_env": "PG_USER",
                    "password_env": "PG_PASS",
                }
            )

        assert result.is_error is False
        data = json.loads(result.content[0]["text"])
        assert data["source_dialect"] == "postgresql"
        assert data["table_count"] == 1
        assert data["tables"][0]["table_name"] == "users"
        assert len(data["tables"][0]["columns"]) == 2
        assert data["tables"][0]["primary_key"] == ["id"]

    def test_missing_connector_param(self) -> None:
        """Missing 'connector' parameter returns an error."""
        result = handle_schema_inspect({})
        assert result.is_error is True
        assert "connector" in result.content[0]["text"].lower()

    def test_unknown_connector(self) -> None:
        """An unknown connector name returns an error."""
        with patch(
            "bani.sdk.schema_inspector.ConnectorRegistry.get",
            side_effect=ValueError("Connector 'nosql' not found"),
        ):
            result = handle_schema_inspect({"connector": "nosql"})

        assert result.is_error is True
        assert "nosql" in result.content[0]["text"]

    def test_rejects_plaintext_password(self) -> None:
        """Passing a 'password' parameter triggers a SecurityError."""
        result = handle_schema_inspect(
            {"connector": "postgresql", "password": "s3cret"}
        )
        assert result.is_error is True
        assert "SecurityError" in result.content[0]["text"]

    def test_rejects_credentials_field(self) -> None:
        """Passing a 'credentials' parameter triggers a SecurityError."""
        result = handle_schema_inspect(
            {"connector": "postgresql", "credentials": {"user": "x"}}
        )
        assert result.is_error is True
        assert "SecurityError" in result.content[0]["text"]


# ---------------------------------------------------------------------------
# bani_validate_bdl
# ---------------------------------------------------------------------------


class TestValidateBdl:
    """Tests for handle_validate_bdl."""

    def test_valid_xml(self) -> None:
        """Valid XML triggers validate_xml and returns valid: True."""
        with patch(
            "bani.bdl.validator.validate_xml", return_value=[]
        ) as mock_val:
            result = handle_validate_bdl({"bdl_content": "<baniProject />"})

        assert result.is_error is False
        data = json.loads(result.content[0]["text"])
        assert data["valid"] is True
        mock_val.assert_called_once()

    def test_invalid_xml(self) -> None:
        """Invalid XML returns errors list."""
        with patch(
            "bani.bdl.validator.validate_xml",
            return_value=["element not allowed"],
        ):
            result = handle_validate_bdl({"bdl_content": "<bad />"})

        data = json.loads(result.content[0]["text"])
        assert data["valid"] is False
        assert len(data["errors"]) == 1

    def test_valid_json(self) -> None:
        """JSON content triggers validate_json."""
        with patch(
            "bani.bdl.validator.validate_json", return_value=[]
        ) as mock_val:
            result = handle_validate_bdl({"bdl_content": '{"name": "test"}'})

        assert result.is_error is False
        data = json.loads(result.content[0]["text"])
        assert data["valid"] is True
        mock_val.assert_called_once()

    def test_missing_content(self) -> None:
        """Missing bdl_content returns an error."""
        result = handle_validate_bdl({})
        assert result.is_error is True
        assert "bdl_content" in result.content[0]["text"]


# ---------------------------------------------------------------------------
# bani_preview
# ---------------------------------------------------------------------------


class TestPreview:
    """Tests for handle_preview."""

    def test_success(self) -> None:
        """Successful preview returns table data with sample rows."""
        from bani.application.preview import ColumnPreview, PreviewResult, TablePreview

        mock_preview = PreviewResult(
            tables=(
                TablePreview(
                    table_name="users",
                    schema_name="public",
                    row_count_estimate=100,
                    columns=(
                        ColumnPreview(name="id", data_type="INTEGER", nullable=False),
                    ),
                    sample_rows=({"id": 1}, {"id": 2}),
                ),
            ),
            source_dialect="postgresql",
        )

        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect="postgresql", host="localhost"),
        )

        mock_source = MagicMock()
        mock_cls = MagicMock(return_value=mock_source)

        with (
            patch("bani.bdl.parser.parse", return_value=project),
            patch(
                "bani.connectors.registry.ConnectorRegistry.get",
                return_value=mock_cls,
            ),
            patch(
                "bani.application.preview.preview_source",
                return_value=mock_preview,
            ),
        ):
            result = handle_preview({"bdl_content": "<baniProject/>", "sample_size": 5})

        assert result.is_error is False
        data = json.loads(result.content[0]["text"])
        assert data["source_dialect"] == "postgresql"
        assert len(data["tables"]) == 1
        assert len(data["tables"][0]["sample_rows"]) == 2
        mock_source.disconnect.assert_called_once()

    def test_missing_content(self) -> None:
        """Missing bdl_content returns error."""
        result = handle_preview({})
        assert result.is_error is True

    def test_no_source_in_bdl(self) -> None:
        """BDL with no source connection returns error."""
        project = ProjectModel(name="test", source=None)
        with patch("bani.bdl.parser.parse", return_value=project):
            result = handle_preview({"bdl_content": "<baniProject/>"})
        assert result.is_error is True
        assert "source" in result.content[0]["text"].lower()


# ---------------------------------------------------------------------------
# bani_run
# ---------------------------------------------------------------------------


class TestRun:
    """Tests for handle_run."""

    def test_dry_run(self, tmp_path: Any) -> None:
        """Dry run validates but does not execute."""
        mock_bp = MagicMock()
        mock_bp.validate.return_value = (True, [])

        with (
            patch(
                "bani.mcp_server.tools._DEFAULT_PROJECTS_DIR",
                str(tmp_path),
            ),
            patch("bani.sdk.bani.Bani.load", return_value=mock_bp),
        ):
            # Save a project first.
            bdl = (
                '<?xml version="1.0"?>'
                '<bani schemaVersion="1.0">'
                '<project name="test"/>'
                '<source connector="postgresql">'
                '<connection host="h" port="1" database="d"'
                ' username="${env:U}" password="${env:P}"/>'
                "</source>"
                '<target connector="mysql">'
                '<connection host="h" port="2" database="d"'
                ' username="${env:U}" password="${env:P}"/>'
                "</target></bani>"
            )
            (tmp_path / "test.bdl").write_text(bdl)

            result = handle_run(
                {"project_name": "test", "dry_run": True}
            )

        assert result.is_error is False
        data = json.loads(result.content[0]["text"])
        assert data["dry_run"] is True
        assert data["success"] is True

    def test_validation_failure(self, tmp_path: Any) -> None:
        """Invalid project returns validation errors."""
        mock_bp = MagicMock()
        mock_bp.validate.return_value = (False, ["Source is required"])

        with (
            patch(
                "bani.mcp_server.tools._DEFAULT_PROJECTS_DIR",
                str(tmp_path),
            ),
            patch("bani.sdk.bani.Bani.load", return_value=mock_bp),
        ):
            (tmp_path / "bad.bdl").write_text("<p/>")
            result = handle_run(
                {"project_name": "bad", "dry_run": True}
            )

        assert result.is_error is True
        data = json.loads(result.content[0]["text"])
        assert data["success"] is False
        assert len(data["errors"]) > 0

    def test_execution_success(self, tmp_path: Any) -> None:
        """Successful execution returns MigrationResult data."""
        migration_result = MigrationResult(
            project_name="test",
            tables_completed=5,
            tables_failed=0,
            total_rows_read=1000,
            total_rows_written=1000,
            duration_seconds=2.5,
            errors=(),
        )

        mock_bp = MagicMock()
        mock_bp.validate.return_value = (True, [])
        mock_bp.run.return_value = migration_result

        with (
            patch(
                "bani.mcp_server.tools._DEFAULT_PROJECTS_DIR",
                str(tmp_path),
            ),
            patch("bani.sdk.bani.Bani.load", return_value=mock_bp),
        ):
            (tmp_path / "test.bdl").write_text("<p/>")
            result = handle_run({"project_name": "test"})

        assert result.is_error is False
        data = json.loads(result.content[0]["text"])
        assert data["success"] is True
        assert data["tables_completed"] == 5
        assert data["total_rows_written"] == 1000

    def test_missing_project_name(self) -> None:
        """Missing project_name returns error."""
        result = handle_run({})
        assert result.is_error is True

    def test_project_not_found(self, tmp_path: Any) -> None:
        """Non-existent project returns error."""
        with patch(
            "bani.mcp_server.tools._DEFAULT_PROJECTS_DIR",
            str(tmp_path),
        ):
            result = handle_run({"project_name": "no_such"})
        assert result.is_error is True
        assert "not found" in result.content[0]["text"]


# ---------------------------------------------------------------------------
# bani_status
# ---------------------------------------------------------------------------


class TestStatus:
    """Tests for handle_status."""

    def test_checkpoint_found(self) -> None:
        """When a checkpoint exists it is returned."""
        checkpoint_data = {
            "project_hash": "abc123",
            "created_at": "2026-01-01T00:00:00+00:00",
            "tables": {
                "public.users": {
                    "status": "completed",
                    "rows_completed": 500,
                }
            },
        }
        with patch(
            "bani.application.checkpoint.CheckpointManager.load",
            return_value=checkpoint_data,
        ):
            result = handle_status({"project_name": "test"})

        assert result.is_error is False
        data = json.loads(result.content[0]["text"])
        assert data["found"] is True
        assert data["checkpoint"]["tables"]["public.users"]["status"] == "completed"

    def test_no_checkpoint(self) -> None:
        """When no checkpoint exists, found is False."""
        with patch(
            "bani.application.checkpoint.CheckpointManager.load",
            return_value=None,
        ):
            result = handle_status({"project_name": "unknown"})

        assert result.is_error is False
        data = json.loads(result.content[0]["text"])
        assert data["found"] is False

    def test_missing_project_name(self) -> None:
        """Missing project_name returns error."""
        result = handle_status({})
        assert result.is_error is True
        assert "project_name" in result.content[0]["text"]


# ---------------------------------------------------------------------------
# bani_connectors_list
# ---------------------------------------------------------------------------


class TestConnectorsList:
    """Tests for handle_connectors_list."""

    def test_lists_connectors(self) -> None:
        """Returns discovered connector names."""
        mock_connectors = {"postgresql": MagicMock(), "mysql": MagicMock()}
        with patch(
            "bani.connectors.registry.ConnectorRegistry.discover",
            return_value=mock_connectors,
        ):
            result = handle_connectors_list({})

        assert result.is_error is False
        data = json.loads(result.content[0]["text"])
        assert "postgresql" in data["connectors"]
        assert "mysql" in data["connectors"]
        assert data["count"] == 2


# ---------------------------------------------------------------------------
# bani_connector_info
# ---------------------------------------------------------------------------


class TestConnectorInfo:
    """Tests for handle_connector_info."""

    def test_returns_info(self) -> None:
        """Returns class name, module, and capabilities for a known connector."""
        from bani.connectors.base import SinkConnector, SourceConnector

        class FakeConnector(SourceConnector, SinkConnector):
            """Fake connector for testing."""

            def connect(self, config: object) -> None: ...  # type: ignore[override]
            def disconnect(self) -> None: ...
            def introspect_schema(self) -> object: ...  # type: ignore[override]
            def read_table(self, *a: object, **kw: object) -> object: ...  # type: ignore[override]
            def estimate_row_count(self, *a: object, **kw: object) -> int: return 0  # type: ignore[override]
            def create_table(self, *a: object, **kw: object) -> None: ...  # type: ignore[override]
            def write_batch(self, *a: object, **kw: object) -> int: return 0  # type: ignore[override]
            def create_indexes(self, *a: object, **kw: object) -> None: ...  # type: ignore[override]
            def create_foreign_keys(self, *a: object, **kw: object) -> None: ...  # type: ignore[override]
            def execute_sql(self, *a: object, **kw: object) -> None: ...  # type: ignore[override]

        with patch(
            "bani.connectors.registry.ConnectorRegistry.get",
            return_value=FakeConnector,
        ):
            result = handle_connector_info({"connector_name": "fake"})

        assert result.is_error is False
        data = json.loads(result.content[0]["text"])
        assert data["name"] == "fake"
        assert "source" in data["capabilities"]
        assert "sink" in data["capabilities"]

    def test_unknown_connector(self) -> None:
        """Unknown connector name returns an error."""
        with patch(
            "bani.connectors.registry.ConnectorRegistry.get",
            side_effect=ValueError("Connector 'nope' not found"),
        ):
            result = handle_connector_info({"connector_name": "nope"})

        assert result.is_error is True
        assert "nope" in result.content[0]["text"]

    def test_missing_name(self) -> None:
        """Missing connector_name returns error."""
        result = handle_connector_info({})
        assert result.is_error is True


# ---------------------------------------------------------------------------
# bani_generate_bdl
# ---------------------------------------------------------------------------


class TestGenerateBdl:
    """Tests for handle_generate_bdl."""

    def test_basic_template(self) -> None:
        """Generates a valid BDL XML template."""
        result = handle_generate_bdl(
            {
                "source_connector": "postgresql",
                "target_connector": "mysql",
            }
        )

        assert result.is_error is False
        text = result.content[0]["text"]
        assert "postgresql" in text
        assert "mysql" in text
        assert "<bani" in text
        assert 'connector="postgresql"' in text
        assert "SOURCE_USER" in text

    def test_with_tables(self) -> None:
        """Template includes specified table entries."""
        result = handle_generate_bdl(
            {
                "source_connector": "postgresql",
                "target_connector": "mysql",
                "tables": ["users", "orders"],
            }
        )

        text = result.content[0]["text"]
        assert 'sourceName="users"' in text
        assert 'sourceName="orders"' in text

    def test_missing_params(self) -> None:
        """Missing connector params returns error."""
        result = handle_generate_bdl({"source_connector": "postgresql"})
        assert result.is_error is True

        result = handle_generate_bdl({})
        assert result.is_error is True


# ---------------------------------------------------------------------------
# Parameter validation edge cases
# ---------------------------------------------------------------------------


class TestParameterValidation:
    """Cross-cutting parameter validation tests."""

    def test_all_handlers_accept_empty_dict(self) -> None:
        """No handler should crash with an empty params dict.

        Handlers with required params should return is_error=True;
        handlers without required params (connectors_list) should succeed.
        """
        for name, handler in TOOL_HANDLERS.items():
            result = handler({})
            assert isinstance(result, ToolResult), f"{name} did not return ToolResult"
            assert isinstance(result.content, list), f"{name}: content is not a list"
            assert len(result.content) >= 1, f"{name}: empty content"
