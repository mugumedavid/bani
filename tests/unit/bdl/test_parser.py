"""Tests for BDL parser."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from bani.bdl.parser import parse, parse_json, parse_xml
from bani.domain.errors import BDLValidationError
from bani.domain.project import SyncStrategy

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "sample_bdl"
JSON_FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "sample_bdl_json"

# Set required environment variables
os.environ.setdefault("MSSQL_USER", "mssql_user")
os.environ.setdefault("MSSQL_PASS", "mssql_pass")
os.environ.setdefault("PG_USER", "pg_user")
os.environ.setdefault("PG_PASS", "pg_pass")
os.environ.setdefault("SRC_USER", "src_user")
os.environ.setdefault("SRC_PASS", "src_pass")
os.environ.setdefault("TGT_USER", "tgt_user")
os.environ.setdefault("TGT_PASS", "tgt_pass")
os.environ.setdefault("DB_USER", "db_user")
os.environ.setdefault("DB_PASS", "db_pass")


class TestParseDetection:
    """Tests for format detection."""

    def test_parse_xml_by_extension(self) -> None:
        """Test parsing XML file by path."""
        result = parse(FIXTURES_DIR / "minimal.bdl")
        assert result.name == "minimal-project"

    def test_parse_detects_xml_from_content(self) -> None:
        """Test auto-detection of XML format."""
        content = (
            '<?xml version="1.0"?>'
            '<bani schemaVersion="1.0"><project name="test"/></bani>'
        )
        result = parse(content)
        assert result.name == "test"

    def test_parse_detects_json_from_content(self) -> None:
        """Test auto-detection of JSON format."""
        content = '{"schemaVersion": "1.0", "project": {"name": "test-json"}}'
        result = parse(content)
        assert result.name == "test-json"


class TestParseXML:
    """Tests for XML parsing."""

    def test_parse_minimal_xml(self) -> None:
        """Test parsing minimal BDL document."""
        with open(FIXTURES_DIR / "minimal.bdl") as f:
            result = parse_xml(f.read())
        assert result.name == "minimal-project"
        assert result.description == "Minimal BDL document"
        assert result.author == "test-user"

    def test_parse_full_reference_xml(self) -> None:
        """Test parsing full reference document."""
        with open(FIXTURES_DIR / "full-reference.bdl") as f:
            result = parse_xml(f.read())
        assert result.name == "legacy-to-postgres"
        expected_desc = "Nightly migration from MSSQL ERP to PostgreSQL analytics"
        assert result.description == expected_desc
        assert len(result.tags) == 2
        assert "erp" in result.tags
        assert len(result.table_mappings) == 4
        assert len(result.type_overrides) == 4
        assert len(result.hooks) == 3
        assert result.schedule.enabled is True
        assert result.schedule.cron == "0 2 * * *"
        assert result.sync.enabled is False

    def test_parse_incremental_sync_xml(self) -> None:
        """Test parsing with sync configuration."""
        with open(FIXTURES_DIR / "incremental-sync.bdl") as f:
            result = parse_xml(f.read())
        assert result.sync.enabled is True
        assert result.sync.strategy == SyncStrategy.TIMESTAMP
        assert len(result.sync.tracking_columns) == 2

    def test_parse_multi_table_xml(self) -> None:
        """Test parsing with multiple tables."""
        with open(FIXTURES_DIR / "multi-table.bdl") as f:
            result = parse_xml(f.read())
        assert len(result.table_mappings) == 5

    def test_parse_hooks_and_schedule_xml(self) -> None:
        """Test parsing hooks and schedule."""
        with open(FIXTURES_DIR / "hooks-and-schedule.bdl") as f:
            result = parse_xml(f.read())
        assert len(result.hooks) == 2
        assert result.schedule.enabled is True
        assert result.schedule.max_retries == 2

    def test_parse_xml_missing_project(self) -> None:
        """Test error on missing project element."""
        content = '<?xml version="1.0"?><bani schemaVersion="1.0"></bani>'
        with pytest.raises(BDLValidationError) as exc_info:
            parse_xml(content)
        assert "project" in str(exc_info.value).lower()

    def test_parse_xml_empty_project(self) -> None:
        """Test error on empty project element."""
        with open(FIXTURES_DIR / "invalid-empty-project.bdl") as f:
            with pytest.raises(BDLValidationError) as exc_info:
                parse_xml(f.read())
        assert "name" in str(exc_info.value).lower()

    def test_parse_xml_bad_schema_version(self) -> None:
        """Test error on unsupported schema version."""
        with open(FIXTURES_DIR / "invalid-bad-schema-version.bdl") as f:
            with pytest.raises(BDLValidationError) as exc_info:
                parse_xml(f.read())
        assert "schema" in str(exc_info.value).lower()

    def test_parse_xml_invalid_xml(self) -> None:
        """Test error on invalid XML."""
        content = '<bani><project name="test"</bani>'
        with pytest.raises(BDLValidationError):
            parse_xml(content)


class TestParseJSON:
    """Tests for JSON parsing."""

    def test_parse_minimal_json(self) -> None:
        """Test parsing minimal JSON BDL."""
        with open(JSON_FIXTURES_DIR / "minimal.bdl.json") as f:
            result = parse_json(f.read())
        assert result.name == "minimal-json"

    def test_parse_full_reference_json(self) -> None:
        """Test parsing full reference JSON."""
        with open(JSON_FIXTURES_DIR / "full-reference.bdl.json") as f:
            result = parse_json(f.read())
        assert result.name == "legacy-to-postgres"
        assert len(result.tags) == 2
        assert len(result.table_mappings) == 4
        assert len(result.type_overrides) == 4

    def test_parse_json_missing_project(self) -> None:
        """Test error on missing project key."""
        content = '{"schemaVersion": "1.0"}'
        with pytest.raises(BDLValidationError) as exc_info:
            parse_json(content)
        assert "project" in str(exc_info.value).lower()

    def test_parse_json_bad_schema_version(self) -> None:
        """Test error on unsupported schema version."""
        content = '{"schemaVersion": "2.0", "project": {"name": "test"}}'
        with pytest.raises(BDLValidationError) as exc_info:
            parse_json(content)
        assert "schema" in str(exc_info.value).lower()

    def test_parse_json_invalid_json(self) -> None:
        """Test error on invalid JSON."""
        content = '{"schemaVersion": "1.0"'
        with pytest.raises(BDLValidationError):
            parse_json(content)


class TestParseConnectionConfigs:
    """Tests for connection parsing."""

    def test_parse_source_connection(self) -> None:
        """Test parsing source connection."""
        with open(FIXTURES_DIR / "minimal.bdl") as f:
            result = parse_xml(f.read())
        assert result.source is not None
        assert result.source.dialect == "mssql"
        assert result.source.host == "localhost"
        assert result.source.port == 1433

    def test_parse_target_connection(self) -> None:
        """Test parsing target connection."""
        with open(FIXTURES_DIR / "minimal.bdl") as f:
            result = parse_xml(f.read())
        assert result.target is not None
        assert result.target.dialect == "postgresql"
        assert result.target.host == "localhost"
        assert result.target.port == 5432


class TestParseTableMappings:
    """Tests for table mapping parsing."""

    def test_parse_table_with_columns(self) -> None:
        """Test parsing table with column mappings."""
        with open(FIXTURES_DIR / "full-reference.bdl") as f:
            result = parse_xml(f.read())
        customers_table = result.table_mappings[0]
        assert customers_table.source_table == "Customers"
        assert customers_table.target_table == "customers"
        assert len(customers_table.columns) == 3

    def test_parse_table_with_filter(self) -> None:
        """Test parsing table with filter."""
        with open(FIXTURES_DIR / "full-reference.bdl") as f:
            result = parse_xml(f.read())
        orders_table = result.table_mappings[1]
        assert orders_table.source_table == "Orders"
        assert "OrderDate" in (orders_table.filter_sql or "")


class TestParseOptions:
    """Tests for options parsing."""

    def test_parse_options_defaults(self) -> None:
        """Test default options are set."""
        content = (
            '<?xml version="1.0"?>'
            '<bani schemaVersion="1.0"><project name="test"/></bani>'
        )
        result = parse_xml(content)
        assert result.options.batch_size == 100000
        assert result.options.parallel_workers == 4

    def test_parse_options_custom(self) -> None:
        """Test custom options are parsed."""
        with open(FIXTURES_DIR / "multi-table.bdl") as f:
            result = parse_xml(f.read())
        assert result.options.batch_size == 50000
        assert result.options.parallel_workers == 8
