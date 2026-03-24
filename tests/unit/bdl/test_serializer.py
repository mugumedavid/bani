"""Tests for BDL serializer."""

from __future__ import annotations

import os
from pathlib import Path

from bani.bdl.parser import parse_xml
from bani.bdl.serializer import serialize
from bani.domain.project import ProjectModel

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "sample_bdl"

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


class TestSerialize:
    """Tests for serialization."""

    def test_serialize_roundtrip_minimal(self) -> None:
        """Test round-trip serialization of minimal document."""
        with open(FIXTURES_DIR / "minimal.bdl") as f:
            original = parse_xml(f.read())

        serialized = serialize(original)
        reparsed = parse_xml(serialized)

        assert reparsed.name == original.name
        assert reparsed.description == original.description
        assert reparsed.author == original.author

    def test_serialize_roundtrip_multi_table(self) -> None:
        """Test round-trip serialization with multiple tables."""
        with open(FIXTURES_DIR / "multi-table.bdl") as f:
            original = parse_xml(f.read())

        serialized = serialize(original)
        reparsed = parse_xml(serialized)

        assert reparsed.name == original.name
        assert len(reparsed.table_mappings) == len(original.table_mappings)
        assert reparsed.options is not None
        assert original.options is not None
        assert reparsed.options.batch_size == original.options.batch_size

    def test_serialize_includes_xml_declaration(self) -> None:
        """Test that serialized output includes XML declaration."""
        project = ProjectModel(name="test-project")
        serialized = serialize(project)
        assert serialized.startswith('<?xml version="1.0"')

    def test_serialize_includes_namespace(self) -> None:
        """Test that serialized output includes namespace."""
        project = ProjectModel(name="test-project")
        serialized = serialize(project)
        assert "xmlns=" in serialized

    def test_serialize_project_metadata(self) -> None:
        """Test serialization of project metadata."""
        with open(FIXTURES_DIR / "full-reference.bdl") as f:
            original = parse_xml(f.read())

        serialized = serialize(original)
        reparsed = parse_xml(serialized)

        assert reparsed.name == original.name
        assert reparsed.description == original.description
        assert reparsed.author == original.author
        assert reparsed.tags == original.tags

    def test_serialize_schedule(self) -> None:
        """Test serialization of schedule config."""
        with open(FIXTURES_DIR / "hooks-and-schedule.bdl") as f:
            original = parse_xml(f.read())

        serialized = serialize(original)
        reparsed = parse_xml(serialized)

        assert reparsed.schedule is not None
        assert original.schedule is not None
        assert reparsed.schedule.enabled == original.schedule.enabled
        assert reparsed.schedule.cron == original.schedule.cron
        assert reparsed.schedule.max_retries == original.schedule.max_retries
