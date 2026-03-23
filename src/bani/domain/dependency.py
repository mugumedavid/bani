"""Dependency resolution via topological sort on foreign key relationships.

The ``DependencyResolver`` determines a safe ordering for table creation and
data transfer so that foreign key constraints are satisfied. When circular
dependencies are detected, the resolver returns a valid ordering and reports
which FK constraints must be deferred (created after data transfer).
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass

from bani.domain.schema import DatabaseSchema, ForeignKeyDefinition


@dataclass(frozen=True)
class ResolutionResult:
    """Result of dependency resolution.

    Attributes:
        ordered_tables: Tables in dependency order (parents before children).
        deferred_fks: Foreign keys that must be created after data transfer
            due to circular dependencies.
    """

    ordered_tables: tuple[str, ...]
    deferred_fks: tuple[ForeignKeyDefinition, ...]


class DependencyResolver:
    """Resolves table creation order based on foreign key dependencies.

    Uses Kahn's algorithm for topological sort. When cycles are detected,
    edges are removed from the cycle to break it, and the corresponding
    FK constraints are marked as deferred.
    """

    def resolve(self, schema: DatabaseSchema) -> ResolutionResult:
        """Compute a dependency-safe table ordering.

        Args:
            schema: The database schema to analyze.

        Returns:
            A ``ResolutionResult`` with ordered tables and any deferred FKs.

        Raises:
            DependencyResolutionError: If resolution fails for reasons other
                than circular dependencies (which are handled by deferring).
        """
        # Build adjacency: table -> set of tables it depends on
        table_names = {t.fully_qualified_name for t in schema.tables}
        graph: dict[str, set[str]] = {name: set() for name in table_names}
        in_degree: dict[str, int] = {name: 0 for name in table_names}
        fk_index: dict[tuple[str, str], list[ForeignKeyDefinition]] = defaultdict(list)

        for table in schema.tables:
            src = table.fully_qualified_name
            for fk in table.foreign_keys:
                ref = self._resolve_fk_target(fk, schema)
                if ref and ref != src:
                    fk_index[(src, ref)].append(fk)
                    if ref not in graph[src]:
                        graph[src].add(ref)
                        in_degree[ref] = in_degree.get(ref, 0)
                        in_degree[src] = in_degree.get(src, 0)

        # Kahn's algorithm
        ordered: list[str] = []
        queue: deque[str] = deque()

        # Recompute in-degrees from the graph
        in_deg: dict[str, int] = {name: 0 for name in table_names}
        for _node, deps in graph.items():
            for dep in deps:
                in_deg[dep] = in_deg.get(dep, 0) + 1

        for name in table_names:
            if in_deg.get(name, 0) == 0:
                queue.append(name)

        while queue:
            node = queue.popleft()
            ordered.append(node)
            for dep in graph[node]:
                in_deg[dep] -= 1
                if in_deg[dep] == 0:
                    queue.append(dep)

        # Handle circular dependencies by deferring FKs
        deferred_fks: list[ForeignKeyDefinition] = []
        if len(ordered) < len(table_names):
            remaining = table_names - set(ordered)
            # Break cycles: defer all FKs among remaining tables
            for table in schema.tables:
                src = table.fully_qualified_name
                if src not in remaining:
                    continue
                for fk in table.foreign_keys:
                    ref = self._resolve_fk_target(fk, schema)
                    if ref and ref in remaining:
                        deferred_fks.append(fk)

            # Add remaining tables in arbitrary but deterministic order
            for name in sorted(remaining):
                ordered.append(name)

        # Reverse: parents (no deps) should come first
        # Actually Kahn's already gives us "leaves first" since nodes with 0
        # in-degree (no one depends on them) come first. We want the opposite:
        # tables that are depended upon (parents) should come first.
        # Re-read: graph[src].add(ref) means src depends on ref,
        # so ref gets in-degree. Kahn pops 0-in-degree = tables nothing
        # depends on = leaf tables. We want the reverse for creation order.
        ordered.reverse()

        return ResolutionResult(
            ordered_tables=tuple(ordered),
            deferred_fks=tuple(deferred_fks),
        )

    @staticmethod
    def _resolve_fk_target(
        fk: ForeignKeyDefinition,
        schema: DatabaseSchema,
    ) -> str | None:
        """Resolve a FK referenced_table name to a fully qualified table name.

        Args:
            fk: The foreign key definition.
            schema: The database schema for lookup.

        Returns:
            The fully qualified table name, or ``None`` if not found in schema.
        """
        # If the referenced_table already contains a dot, assume it's qualified
        if "." in fk.referenced_table:
            return fk.referenced_table

        # Otherwise search across all schemas
        for table in schema.tables:
            if table.table_name == fk.referenced_table:
                return table.fully_qualified_name
        return None
