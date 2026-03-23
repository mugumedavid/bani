"""Tests for type mapping engine."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from bani.domain.errors import TypeMappingError
from bani.domain.type_mapping import MappingRule, MappingRuleSet, TypeMapper


class TestMappingRule:
    """Tests for MappingRule."""

    def test_exact_match(self) -> None:
        rule = MappingRule(
            source_type="VARCHAR",
            target_type="TEXT",
        )
        assert rule.matches("VARCHAR(255)", "mysql", "postgresql") is True

    def test_case_insensitive(self) -> None:
        rule = MappingRule(source_type="varchar", target_type="TEXT")
        assert rule.matches("VARCHAR(100)", "mysql", "postgresql") is True

    def test_dialect_filter(self) -> None:
        rule = MappingRule(
            source_type="INT",
            target_type="INTEGER",
            source_dialect="mysql",
            target_dialect="sqlite",
        )
        assert rule.matches("INT", "mysql", "sqlite") is True
        assert rule.matches("INT", "mssql", "sqlite") is False
        assert rule.matches("INT", "mysql", "postgresql") is False

    def test_wildcard_dialects(self) -> None:
        rule = MappingRule(source_type="BOOLEAN", target_type="BOOL")
        assert rule.matches("BOOLEAN", "any_src", "any_tgt") is True

    def test_no_match(self) -> None:
        rule = MappingRule(source_type="INT", target_type="INTEGER")
        assert rule.matches("VARCHAR(255)", "mysql", "postgresql") is False


class TestMappingRuleSet:
    """Tests for MappingRuleSet."""

    def test_empty(self) -> None:
        rs = MappingRuleSet()
        assert len(rs) == 0
        assert list(rs) == []

    def test_iteration(self) -> None:
        r1 = MappingRule(source_type="INT", target_type="INTEGER")
        r2 = MappingRule(source_type="VARCHAR", target_type="TEXT")
        rs = MappingRuleSet(rules=(r1, r2), name="defaults")
        assert len(rs) == 2
        assert list(rs) == [r1, r2]


class TestTypeMapper:
    """Tests for TypeMapper."""

    def test_basic_mapping(self) -> None:
        defaults = MappingRuleSet(
            rules=(
                MappingRule(source_type="INT", target_type="INTEGER"),
                MappingRule(source_type="VARCHAR", target_type="TEXT"),
            ),
        )
        mapper = TypeMapper(default_rules=defaults)
        assert mapper.map_type("INT", "mysql", "sqlite") == "INTEGER"
        assert mapper.map_type("VARCHAR(255)", "mysql", "sqlite") == "TEXT"

    def test_override_takes_priority(self) -> None:
        defaults = MappingRuleSet(
            rules=(MappingRule(source_type="INT", target_type="INTEGER"),),
        )
        overrides = MappingRuleSet(
            rules=(MappingRule(source_type="INT", target_type="BIGINT"),),
        )
        mapper = TypeMapper(default_rules=defaults, override_rules=overrides)
        assert mapper.map_type("INT", "mysql", "postgresql") == "BIGINT"

    def test_unmapped_raises(self) -> None:
        mapper = TypeMapper()
        with pytest.raises(TypeMappingError, match="No mapping found"):
            mapper.map_type("GEOGRAPHY", "mssql", "postgresql")

    def test_from_json_file(self, tmp_path: Path) -> None:
        data = [
            {"source_type": "INT", "target_type": "INTEGER"},
            {
                "source_type": "DATETIME",
                "target_type": "TIMESTAMP",
                "source_dialect": "mysql",
                "target_dialect": "postgresql",
            },
        ]
        json_path = tmp_path / "type_defaults.json"
        json_path.write_text(json.dumps(data))

        mapper = TypeMapper.from_json_file(json_path)
        assert mapper.map_type("INT", "mysql", "postgresql") == "INTEGER"
        assert mapper.map_type("DATETIME", "mysql", "postgresql") == "TIMESTAMP"

    def test_first_match_wins(self) -> None:
        defaults = MappingRuleSet(
            rules=(
                MappingRule(source_type="INT", target_type="FIRST"),
                MappingRule(source_type="INT", target_type="SECOND"),
            ),
        )
        mapper = TypeMapper(default_rules=defaults)
        assert mapper.map_type("INT", "mysql", "postgresql") == "FIRST"

    def test_properties(self) -> None:
        defaults = MappingRuleSet(rules=(), name="d")
        overrides = MappingRuleSet(rules=(), name="o")
        mapper = TypeMapper(default_rules=defaults, override_rules=overrides)
        assert mapper.default_rules.name == "d"
        assert mapper.override_rules.name == "o"
