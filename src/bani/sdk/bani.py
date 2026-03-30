"""Top-level Bani class for loading and running migrations."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from bani.application.orchestrator import MigrationOrchestrator, MigrationResult
from bani.application.preview import PreviewResult, preview_source
from bani.application.progress import ProgressTracker
from bani.bdl.parser import parse
from bani.bdl.validator import validate_json, validate_xml
from bani.connectors.base import SinkConnector, SourceConnector
from bani.connectors.registry import ConnectorRegistry
from bani.domain.project import ProjectModel

if TYPE_CHECKING:
    pass


class BaniProject:
    """Wrapper around a loaded ProjectModel with validation and execution methods."""

    def __init__(self, project: ProjectModel) -> None:
        """Initialize a BaniProject.

        Args:
            project: The ProjectModel to wrap.
        """
        self._project = project

    def validate(self) -> tuple[bool, list[str]]:
        """Validate the project configuration.

        Returns:
            A tuple of (is_valid, error_messages).
        """
        # For now, basic validation
        errors: list[str] = []

        if not self._project.name:
            errors.append("Project name is required")

        if self._project.source is None:
            errors.append("Source connection is required")
        elif not self._project.source.dialect:
            errors.append("Source dialect is required")

        if self._project.target is None:
            errors.append("Target connection is required")
        elif not self._project.target.dialect:
            errors.append("Target dialect is required")

        return (len(errors) == 0, errors)

    def run(
        self,
        on_progress: Callable[[Any], None] | None = None,
        resume: bool = False,
        cancel_event: Any | None = None,
        checkpoint: Any | None = None,
    ) -> MigrationResult:
        """Execute the migration.

        Args:
            on_progress: Optional callback for progress updates.
            resume: If ``True``, resume from the last checkpoint.
                Completed tables are skipped; in-progress/failed
                tables are dropped and re-transferred.
            cancel_event: Optional ``threading.Event`` that signals
                cancellation. The orchestrator checks it between
                batches and stops gracefully when set.
            checkpoint: Optional ``CheckpointManager`` instance. If
                ``None``, a default one is created.

        Returns:
            A MigrationResult with execution summary.

        Raises:
            ValueError: If the project is invalid.
            Exception: If migration execution fails.
        """
        is_valid, errors = self.validate()
        if not is_valid:
            msg = "Project validation failed: " + "; ".join(errors)
            raise ValueError(msg)

        # Get source and target connectors
        source_cfg = self._project.source
        target_cfg = self._project.target

        assert source_cfg is not None
        assert target_cfg is not None

        # Determine pool size from project options
        pool_size = (
            self._project.options.parallel_workers
            if self._project.options
            else 4
        )

        # Create source connector and connect
        source_connector_class = ConnectorRegistry.get(source_cfg.dialect)
        source = cast(type[SourceConnector], source_connector_class)()
        source.connect(source_cfg, pool_size=pool_size)

        # Create sink connector and connect
        sink_connector_class = ConnectorRegistry.get(target_cfg.dialect)
        sink = cast(type[SinkConnector], sink_connector_class)()
        sink.connect(target_cfg, pool_size=pool_size)

        try:
            # Create progress tracker if callback provided
            tracker = None
            if on_progress:
                tracker = ProgressTracker()
                tracker.add_listener(on_progress)

            kwargs: dict[str, Any] = {"tracker": tracker}
            if checkpoint is not None:
                kwargs["checkpoint"] = checkpoint
            orchestrator = MigrationOrchestrator(
                self._project, source, sink, **kwargs
            )
            if cancel_event is not None:
                orchestrator.set_cancel_event(cancel_event)
            return orchestrator.execute(resume=resume)
        finally:
            source.disconnect()
            sink.disconnect()

    def preview(
        self,
        tables: list[str] | None = None,
        sample_size: int = 10,
    ) -> PreviewResult:
        """Preview data from the source database.

        Delegates to :func:`bani.application.preview.preview_source` for
        the actual introspection and sampling logic.

        Args:
            tables: Optional list of table names to preview. If ``None``,
                all tables are previewed.
            sample_size: Number of rows to sample per table.

        Returns:
            A :class:`PreviewResult` with column metadata and sample rows.
        """
        source_cfg = self._project.source
        assert source_cfg is not None

        pool_size = (
            self._project.options.parallel_workers
            if self._project.options
            else 1
        )
        source_connector_class = ConnectorRegistry.get(source_cfg.dialect)
        source = cast(type[SourceConnector], source_connector_class)()
        source.connect(source_cfg, pool_size=pool_size)

        try:
            return preview_source(source, tables=tables, sample_size=sample_size)
        finally:
            source.disconnect()


class Bani:
    """Top-level entry point for Bani operations."""

    @staticmethod
    def load(path: str | Path) -> BaniProject:
        """Load a BDL project file.

        Args:
            path: Path to the BDL file.

        Returns:
            A BaniProject ready for validation and execution.

        Raises:
            BDLValidationError: If parsing fails.
        """
        path_obj = Path(path) if isinstance(path, str) else path
        project = parse(path_obj)
        return BaniProject(project)

    @staticmethod
    def validate_file(path: str | Path) -> tuple[bool, list[str]]:
        """Validate a BDL file.

        Args:
            path: Path to the BDL file.

        Returns:
            A tuple of (is_valid, error_messages).
        """
        path_obj = Path(path) if isinstance(path, str) else path

        with open(path_obj) as f:
            content = f.read()

        if content.strip().startswith("<"):
            errors = validate_xml(content)
        else:
            errors = validate_json(content)

        return (len(errors) == 0, errors)
