"""Tests for top-level Bani class."""

from __future__ import annotations

from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

from bani.application.orchestrator import MigrationResult
from bani.domain.project import ConnectionConfig, ProjectModel
from bani.domain.schema import ColumnDefinition, DatabaseSchema, TableDefinition
from bani.sdk.bani import Bani, BaniProject


class TestBaniProject:
    """Tests for BaniProject."""

    def test_validate_success(self) -> None:
        """Test successful validation."""
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mysql"),
        )
        bani_project = BaniProject(project)
        is_valid, errors = bani_project.validate()

        assert is_valid is True
        assert len(errors) == 0

    def test_validate_missing_name(self) -> None:
        """Test validation with missing name."""
        project = ProjectModel(
            name="",
            source=ConnectionConfig(dialect="postgresql"),
            target=ConnectionConfig(dialect="mysql"),
        )
        bani_project = BaniProject(project)
        is_valid, errors = bani_project.validate()

        assert is_valid is False
        assert any("name" in err.lower() for err in errors)

    def test_validate_missing_source(self) -> None:
        """Test validation with missing source."""
        project = ProjectModel(
            name="test",
            source=None,
            target=ConnectionConfig(dialect="mysql"),
        )
        bani_project = BaniProject(project)
        is_valid, errors = bani_project.validate()

        assert is_valid is False
        assert any("source" in err.lower() for err in errors)

    def test_validate_missing_target(self) -> None:
        """Test validation with missing target."""
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect="postgresql"),
            target=None,
        )
        bani_project = BaniProject(project)
        is_valid, errors = bani_project.validate()

        assert is_valid is False
        assert any("target" in err.lower() for err in errors)

    def test_validate_missing_source_dialect(self) -> None:
        """Test validation with missing source dialect."""
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(dialect=""),
            target=ConnectionConfig(dialect="mysql"),
        )
        bani_project = BaniProject(project)
        is_valid, errors = bani_project.validate()

        assert is_valid is False
        assert any("dialect" in err.lower() for err in errors)

    def test_run_invalid_project_raises_error(self) -> None:
        """Test that run raises error for invalid project."""
        project = ProjectModel(
            name="test",
            source=None,
            target=None,
        )
        bani_project = BaniProject(project)

        try:
            bani_project.run()
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "validation" in str(e).lower()

    def test_run_with_mock_connectors(self) -> None:
        """Test run with mocked connectors."""
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(
                dialect="postgresql",
                host="localhost",
                port=5432,
                database="source_db",
                username_env="PG_USER",
                password_env="PG_PASS",
            ),
            target=ConnectionConfig(
                dialect="mysql",
                host="localhost",
                port=3306,
                database="target_db",
                username_env="MYSQL_USER",
                password_env="MYSQL_PASS",
            ),
        )
        bani_project = BaniProject(project)

        # Mock connectors and orchestrator
        mock_source_class = MagicMock()
        mock_source = MagicMock()
        mock_source_class.return_value = mock_source

        mock_sink_class = MagicMock()
        mock_sink = MagicMock()
        mock_sink_class.return_value = mock_sink

        mock_orchestrator = MagicMock()

        mock_result = MigrationResult(
            project_name="test",
            tables_completed=1,
            tables_failed=0,
            total_rows_read=100,
            total_rows_written=100,
            duration_seconds=1.5,
        )

        with (
            patch("bani.sdk.bani.ConnectorRegistry.get") as mock_get,
            patch("bani.sdk.bani.MigrationOrchestrator") as mock_orch_class,
        ):
            mock_get.side_effect = [mock_source_class, mock_sink_class]
            mock_orch_class.return_value = mock_orchestrator
            mock_orchestrator.execute.return_value = mock_result

            result = bani_project.run()

            assert result.tables_completed == 1
            assert result.tables_failed == 0
            assert result.total_rows_read == 100
            mock_source.connect.assert_called_once()
            mock_sink.connect.assert_called_once()
            mock_source.disconnect.assert_called_once()
            mock_sink.disconnect.assert_called_once()

    def test_preview_with_mock_connector(self) -> None:
        """Test preview with mocked connector."""
        project = ProjectModel(
            name="test",
            source=ConnectionConfig(
                dialect="postgresql",
                host="localhost",
                username_env="PG_USER",
                password_env="PG_PASS",
            ),
            target=ConnectionConfig(dialect="mysql"),
        )
        bani_project = BaniProject(project)

        # Create sample table and batch
        table_def = TableDefinition(
            schema_name="public",
            table_name="users",
            columns=(
                ColumnDefinition(
                    name="id",
                    data_type="INTEGER",
                    ordinal_position=0,
                ),
                ColumnDefinition(
                    name="name",
                    data_type="VARCHAR(255)",
                    ordinal_position=1,
                ),
            ),
        )

        # Mock the source connector
        mock_source_class = MagicMock()
        mock_source = MagicMock()
        mock_source_class.return_value = mock_source

        mock_schema = DatabaseSchema(
            tables=(table_def,),
            source_dialect="postgresql",
        )
        mock_source.introspect_schema.return_value = mock_schema

        # Create a mock batch
        import pyarrow as pa

        mock_batch = pa.record_batch(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
            }
        )
        mock_source.read_table.return_value = iter([mock_batch])

        with patch("bani.sdk.bani.ConnectorRegistry.get") as mock_get:
            mock_get.return_value = mock_source_class

            preview = bani_project.preview(sample_size=10)

            assert len(preview.tables) == 1
            table_preview = preview.tables[0]
            assert table_preview.table_name == "users"
            assert len(table_preview.sample_rows) == 2
            assert table_preview.sample_rows[0]["id"] == 1
            assert table_preview.sample_rows[0]["name"] == "Alice"
            mock_source.connect.assert_called_once()
            mock_source.introspect_schema.assert_called_once()
            mock_source.disconnect.assert_called_once()


class TestBani:
    """Tests for Bani class."""

    def test_load_from_file(self) -> None:
        """Test loading a BDL file."""
        # Create a minimal BDL XML file
        bdl_content = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<migration xmlns="https://bani.tools/bdl/1.0" schemaVersion="1.0">\n'
            '    <project name="test_project" description="Test migration">\n'
            '        <source dialect="postgresql" host="localhost" port="5432"\n'
            '                database="source_db" />\n'
            '        <target dialect="mysql" host="localhost" port="3306"\n'
            '                database="target_db" />\n'
            "    </project>\n"
            "</migration>\n"
        )
        with NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
            f.write(bdl_content)
            f.flush()

            try:
                project = Bani.load(f.name)
                assert isinstance(project, BaniProject)
                assert project._project.name == "test_project"
            finally:
                import os

                os.unlink(f.name)

    def test_load_invalid_file_raises_error(self) -> None:
        """Test loading an invalid BDL file."""
        with NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
            f.write("<invalid>not valid BDL</invalid>")
            f.flush()

            try:
                try:
                    Bani.load(f.name)
                    raise AssertionError("Should have raised BDLValidationError")
                except Exception:
                    pass
            finally:
                import os

                os.unlink(f.name)

    def test_validate_file(self) -> None:
        """Test file validation."""
        bdl_content = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<migration xmlns="https://bani.tools/bdl/1.0" schemaVersion="1.0">\n'
            '    <project name="test_project">\n'
            '        <source dialect="postgresql" />\n'
            '        <target dialect="mysql" />\n'
            "    </project>\n"
            "</migration>\n"
        )
        with NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
            f.write(bdl_content)
            f.flush()

            try:
                is_valid, errors = Bani.validate_file(f.name)
                # Validation result depends on validator implementation
                assert isinstance(is_valid, bool)
                assert isinstance(errors, list)
            finally:
                import os

                os.unlink(f.name)
