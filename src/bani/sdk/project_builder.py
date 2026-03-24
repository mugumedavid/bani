"""Fluent builder for ProjectModel construction (Section 18.3)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from bani.domain.project import (
    ConnectionConfig,
    ProjectModel,
    ProjectOptions,
    TableMapping,
    TypeMappingOverride,
)

if TYPE_CHECKING:
    pass


class ProjectBuilder:
    """Fluent builder for constructing ProjectModel instances."""

    def __init__(self, name: str) -> None:
        """Initialize a new project builder.

        Args:
            name: The project name.
        """
        self._name = name
        self._source_config: ConnectionConfig | None = None
        self._target_config: ConnectionConfig | None = None
        self._description = ""
        self._author = ""
        self._tags: list[str] = []
        self._include_tables: list[str] = []
        self._exclude_tables: list[str] = []
        self._type_overrides: list[TypeMappingOverride] = []
        self._batch_size = 100_000
        self._parallel_workers = 4
        self._memory_limit_mb = 2048
        self._created: datetime | None = None

    def source(
        self,
        dialect: str,
        host: str = "",
        port: int = 0,
        database: str = "",
        username_env: str = "",
        password_env: str = "",
        **extra: str,
    ) -> ProjectBuilder:
        """Configure the source database.

        Args:
            dialect: Database dialect (e.g. "postgresql", "mysql").
            host: Database host.
            port: Database port.
            database: Database name.
            username_env: Environment variable name for username.
            password_env: Environment variable name for password.
            **extra: Additional connector-specific configuration.

        Returns:
            This builder for method chaining.
        """
        extra_tuple = tuple(sorted(extra.items()))
        self._source_config = ConnectionConfig(
            dialect=dialect,
            host=host,
            port=port,
            database=database,
            username_env=username_env,
            password_env=password_env,
            extra=extra_tuple,
        )
        return self

    def target(
        self,
        dialect: str,
        host: str = "",
        port: int = 0,
        database: str = "",
        username_env: str = "",
        password_env: str = "",
        **extra: str,
    ) -> ProjectBuilder:
        """Configure the target database.

        Args:
            dialect: Database dialect (e.g. "postgresql", "mysql").
            host: Database host.
            port: Database port.
            database: Database name.
            username_env: Environment variable name for username.
            password_env: Environment variable name for password.
            **extra: Additional connector-specific configuration.

        Returns:
            This builder for method chaining.
        """
        extra_tuple = tuple(sorted(extra.items()))
        self._target_config = ConnectionConfig(
            dialect=dialect,
            host=host,
            port=port,
            database=database,
            username_env=username_env,
            password_env=password_env,
            extra=extra_tuple,
        )
        return self

    def include_tables(self, tables: list[str]) -> ProjectBuilder:
        """Include only these tables in the migration.

        Tables are specified in "schema.table" format.

        Args:
            tables: List of fully qualified table names to include.

        Returns:
            This builder for method chaining.
        """
        self._include_tables = tables
        return self

    def exclude_tables(self, tables: list[str]) -> ProjectBuilder:
        """Exclude these tables from the migration.

        Tables are specified in "schema.table" format.

        Args:
            tables: List of fully qualified table names to exclude.

        Returns:
            This builder for method chaining.
        """
        self._exclude_tables = tables
        return self

    def type_mapping(self, source_type: str, target_type: str) -> ProjectBuilder:
        """Add a type mapping override.

        Args:
            source_type: Source database type.
            target_type: Target database type.

        Returns:
            This builder for method chaining.
        """
        self._type_overrides.append(
            TypeMappingOverride(source_type=source_type, target_type=target_type)
        )
        return self

    def batch_size(self, size: int) -> ProjectBuilder:
        """Set the batch size for data transfer.

        Args:
            size: Number of rows per batch.

        Returns:
            This builder for method chaining.
        """
        self._batch_size = size
        return self

    def parallel_workers(self, workers: int) -> ProjectBuilder:
        """Set the number of parallel workers.

        Args:
            workers: Number of parallel workers.

        Returns:
            This builder for method chaining.
        """
        self._parallel_workers = workers
        return self

    def memory_limit(self, mb: int) -> ProjectBuilder:
        """Set the memory limit in MB.

        Args:
            mb: Memory limit in megabytes.

        Returns:
            This builder for method chaining.
        """
        self._memory_limit_mb = mb
        return self

    def description(self, desc: str) -> ProjectBuilder:
        """Set the project description.

        Args:
            desc: Project description.

        Returns:
            This builder for method chaining.
        """
        self._description = desc
        return self

    def author(self, name: str) -> ProjectBuilder:
        """Set the project author.

        Args:
            name: Author name.

        Returns:
            This builder for method chaining.
        """
        self._author = name
        return self

    def tags(self, tags: list[str]) -> ProjectBuilder:
        """Set project tags.

        Args:
            tags: List of tags.

        Returns:
            This builder for method chaining.
        """
        self._tags = tags
        return self

    def build(self) -> ProjectModel:
        """Build and return the ProjectModel.

        Returns:
            The constructed ProjectModel.
        """
        # Build table mappings from include/exclude lists
        table_mappings: list[TableMapping] = []

        if self._include_tables:
            for table_spec in self._include_tables:
                parts = table_spec.split(".", 1)
                if len(parts) == 2:
                    schema, table = parts
                    table_mappings.append(
                        TableMapping(
                            source_schema=schema,
                            source_table=table,
                            target_schema=schema,
                            target_table=table,
                        )
                    )

        # Build options
        options = ProjectOptions(
            batch_size=self._batch_size,
            parallel_workers=self._parallel_workers,
            memory_limit_mb=self._memory_limit_mb,
        )

        # If created timestamp not set, use current time
        created = self._created or datetime.now(timezone.utc)

        return ProjectModel(
            name=self._name,
            source=self._source_config,
            target=self._target_config,
            description=self._description,
            author=self._author,
            created=created,
            tags=tuple(self._tags),
            table_mappings=tuple(table_mappings),
            type_overrides=tuple(self._type_overrides),
            options=options,
        )
