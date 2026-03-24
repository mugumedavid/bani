"""Tests for ProjectBuilder fluent API."""

from __future__ import annotations

from bani.sdk.project_builder import ProjectBuilder


class TestProjectBuilder:
    """Tests for ProjectBuilder."""

    def test_minimal_build(self) -> None:
        """Test building a minimal project."""
        builder = ProjectBuilder("test_project")
        project = builder.build()

        assert project.name == "test_project"
        assert project.source is None
        assert project.target is None
        assert project.options is not None

    def test_source_configuration(self) -> None:
        """Test configuring source."""
        builder = ProjectBuilder("test")
        builder.source(
            "postgresql",
            host="localhost",
            port=5432,
            database="source_db",
            username_env="PG_USER",
            password_env="PG_PASS",
        )
        project = builder.build()

        assert project.source is not None
        assert project.source.dialect == "postgresql"
        assert project.source.host == "localhost"
        assert project.source.port == 5432
        assert project.source.database == "source_db"
        assert project.source.username_env == "PG_USER"
        assert project.source.password_env == "PG_PASS"

    def test_target_configuration(self) -> None:
        """Test configuring target."""
        builder = ProjectBuilder("test")
        builder.target(
            "mysql",
            host="db.example.com",
            port=3306,
            database="target_db",
            username_env="MYSQL_USER",
            password_env="MYSQL_PASS",
        )
        project = builder.build()

        assert project.target is not None
        assert project.target.dialect == "mysql"
        assert project.target.host == "db.example.com"
        assert project.target.port == 3306
        assert project.target.database == "target_db"

    def test_include_tables(self) -> None:
        """Test including specific tables."""
        builder = ProjectBuilder("test")
        builder.include_tables(["public.users", "public.orders"])
        project = builder.build()

        assert len(project.table_mappings) == 2
        assert project.table_mappings[0].source_schema == "public"
        assert project.table_mappings[0].source_table == "users"
        assert project.table_mappings[1].source_schema == "public"
        assert project.table_mappings[1].source_table == "orders"

    def test_type_mapping(self) -> None:
        """Test adding type mappings."""
        builder = ProjectBuilder("test")
        builder.type_mapping("VARCHAR(255)", "TEXT")
        builder.type_mapping("INT", "BIGINT")
        project = builder.build()

        assert len(project.type_overrides) == 2
        assert project.type_overrides[0].source_type == "VARCHAR(255)"
        assert project.type_overrides[0].target_type == "TEXT"

    def test_batch_size(self) -> None:
        """Test setting batch size."""
        builder = ProjectBuilder("test")
        builder.batch_size(50000)
        project = builder.build()

        assert project.options is not None
        assert project.options.batch_size == 50000

    def test_parallel_workers(self) -> None:
        """Test setting parallel workers."""
        builder = ProjectBuilder("test")
        builder.parallel_workers(8)
        project = builder.build()

        assert project.options is not None
        assert project.options.parallel_workers == 8

    def test_memory_limit(self) -> None:
        """Test setting memory limit."""
        builder = ProjectBuilder("test")
        builder.memory_limit(4096)
        project = builder.build()

        assert project.options is not None
        assert project.options.memory_limit_mb == 4096

    def test_fluent_chaining(self) -> None:
        """Test fluent API chaining."""
        builder = ProjectBuilder("test_project")
        project = (
            builder.source(
                "postgresql",
                host="localhost",
                port=5432,
                database="source_db",
                username_env="PG_USER",
                password_env="PG_PASS",
            )
            .target(
                "mysql",
                host="localhost",
                port=3306,
                database="target_db",
                username_env="MYSQL_USER",
                password_env="MYSQL_PASS",
            )
            .include_tables(["public.users"])
            .batch_size(25000)
            .parallel_workers(2)
            .description("Test migration")
            .author("Test Author")
            .tags(["test", "example"])
            .build()
        )

        assert project.name == "test_project"
        assert project.source is not None
        assert project.source.dialect == "postgresql"
        assert project.target is not None
        assert project.target.dialect == "mysql"
        assert len(project.table_mappings) == 1
        assert project.options is not None
        assert project.options.batch_size == 25000
        assert project.options.parallel_workers == 2
        assert project.description == "Test migration"
        assert project.author == "Test Author"
        assert project.tags == ("test", "example")

    def test_extra_parameters(self) -> None:
        """Test passing extra connector parameters."""
        builder = ProjectBuilder("test")
        builder.source(
            "postgresql",
            host="localhost",
            username_env="PG_USER",
            password_env="PG_PASS",
            ssl_mode="require",
            application_name="bani",
        )
        project = builder.build()

        assert project.source is not None
        assert ("application_name", "bani") in project.source.extra
        assert ("ssl_mode", "require") in project.source.extra

    def test_created_timestamp(self) -> None:
        """Test that created timestamp is set."""
        builder = ProjectBuilder("test")
        project = builder.build()

        assert project.created is not None

    def test_exclude_tables_not_in_mappings(self) -> None:
        """Test that exclude_tables doesn't add table mappings."""
        builder = ProjectBuilder("test")
        builder.exclude_tables(["public.temp_table"])
        project = builder.build()

        # exclude_tables doesn't create mappings, just stores them for later use
        assert len(project.table_mappings) == 0
