"""Tests for the named connections registry."""

from __future__ import annotations

import json
from typing import Any

from bani.infra.connections import ConnectionRegistry, RegisteredConnection


class TestLoad:
    """Tests for ConnectionRegistry.load()."""

    def test_returns_empty_when_file_missing(self, tmp_path: Any) -> None:
        result = ConnectionRegistry.load(tmp_path / "nope.json")
        assert result == {}

    def test_parses_valid_json(self, tmp_path: Any) -> None:
        data = {
            "pg": {
                "name": "PostgreSQL",
                "connector": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "mydb",
                "username": "user",
                "password": "pass",
            },
            "mysql": {
                "name": "MySQL 8",
                "connector": "mysql",
                "host": "remotehost",
                "port": 3306,
                "database": "app",
                "username": "${env:MY_USER}",
                "password": "${env:MY_PASS}",
            },
        }
        path = tmp_path / "connections.json"
        path.write_text(json.dumps(data))

        result = ConnectionRegistry.load(path)
        assert len(result) == 2
        assert result["pg"].name == "PostgreSQL"
        assert result["pg"].connector == "postgresql"
        assert result["mysql"].port == 3306

    def test_skips_malformed_entries(self, tmp_path: Any) -> None:
        data = {
            "good": {
                "name": "Good",
                "connector": "postgresql",
                "host": "h",
                "port": 1,
                "database": "d",
                "username": "u",
                "password": "p",
            },
            "bad": "not a dict",
            "missing_connector": {"name": "No connector"},
        }
        path = tmp_path / "connections.json"
        path.write_text(json.dumps(data))

        result = ConnectionRegistry.load(path)
        assert len(result) == 1
        assert "good" in result

    def test_returns_empty_on_invalid_json(self, tmp_path: Any) -> None:
        path = tmp_path / "connections.json"
        path.write_text("not json at all")
        assert ConnectionRegistry.load(path) == {}


class TestGet:
    """Tests for ConnectionRegistry.get()."""

    def test_returns_connection(self, tmp_path: Any) -> None:
        data = {
            "mydb": {
                "name": "My DB",
                "connector": "mysql",
                "host": "h",
                "port": 3306,
                "database": "d",
                "username": "u",
                "password": "p",
            }
        }
        path = tmp_path / "connections.json"
        path.write_text(json.dumps(data))

        conn = ConnectionRegistry.get("mydb", path)
        assert conn.key == "mydb"
        assert conn.name == "My DB"

    def test_raises_on_unknown_key(self, tmp_path: Any) -> None:
        path = tmp_path / "connections.json"
        path.write_text("{}")
        try:
            ConnectionRegistry.get("nope", path)
            raise AssertionError("Expected ValueError")
        except ValueError as exc:
            assert "nope" in str(exc)


class TestToConnectionConfig:
    """Tests for ConnectionRegistry.to_connection_config()."""

    def test_env_ref_passthrough(self) -> None:
        conn = RegisteredConnection(
            key="x",
            name="X",
            connector="postgresql",
            host="h",
            port=5432,
            database="d",
            username="${env:PG_USER}",
            password="${env:PG_PASS}",
        )
        config = ConnectionRegistry.to_connection_config(conn)
        assert config.dialect == "postgresql"
        assert config.username_env == "${env:PG_USER}"
        assert config.password_env == "${env:PG_PASS}"

    def test_options_flow_to_extra(self) -> None:
        conn = RegisteredConnection(
            key="ora",
            name="Oracle",
            connector="oracle",
            host="h",
            port=1521,
            database="",
            username="sys",
            password="pass",
            options=(("service_name", "FREEPDB1"),),
        )
        config = ConnectionRegistry.to_connection_config(conn)
        assert config.extra == (("service_name", "FREEPDB1"),)

    def test_options_parsed_from_json(self, tmp_path: Any) -> None:
        data = {
            "ora": {
                "name": "Oracle",
                "connector": "oracle",
                "host": "h",
                "port": 1521,
                "database": "",
                "username": "u",
                "password": "p",
                "options": {
                    "service_name": "FREEPDB1",
                    "ssl_cert_path": "/tmp/cert.pem",
                },
            }
        }
        path = tmp_path / "connections.json"
        path.write_text(json.dumps(data))

        result = ConnectionRegistry.load(path)
        conn = result["ora"]
        assert ("service_name", "FREEPDB1") in conn.options
        assert ("ssl_cert_path", "/tmp/cert.pem") in conn.options

    def test_literal_values_injected(self) -> None:
        conn = RegisteredConnection(
            key="lit",
            name="Literal",
            connector="mysql",
            host="h",
            port=3306,
            database="d",
            username="admin",
            password="secret",
        )
        config = ConnectionRegistry.to_connection_config(conn)
        assert config.username_env == "_BANI_CONN_lit_USER"
        assert config.password_env == "_BANI_CONN_lit_PASS"
        import os

        assert os.environ["_BANI_CONN_lit_USER"] == "admin"
        assert os.environ["_BANI_CONN_lit_PASS"] == "secret"


class TestSafeSummary:
    """Tests for ConnectionRegistry.safe_summary()."""

    def test_excludes_credentials(self) -> None:
        conn = RegisteredConnection(
            key="k",
            name="N",
            connector="mysql",
            host="h",
            port=3306,
            database="d",
            username="secret_user",
            password="secret_pass",
        )
        summary = ConnectionRegistry.safe_summary(conn)
        assert "username" not in summary
        assert "password" not in summary
        assert summary["key"] == "k"
        assert summary["name"] == "N"
        assert summary["connector"] == "mysql"
