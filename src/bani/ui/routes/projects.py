"""Project CRUD routes — manage BDL project files on disk (Section 20.3).

Projects are stored as ``.bdl`` files in the configured projects directory
(default ``~/.bani/projects/``).
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request

from bani.ui.auth import verify_token
from bani.ui.models import ProjectCreate, ProjectDetail, ProjectSummary, ProjectUpdate

router = APIRouter(tags=["projects"], dependencies=[Depends(verify_token)])


def _projects_dir(request: Request) -> Path:
    """Resolve the projects directory from app state.

    Args:
        request: The incoming FastAPI request.

    Returns:
        Path to the projects directory, created if it does not exist.
    """
    raw: str = getattr(request.app.state, "projects_dir", "~/.bani/projects")
    path = Path(raw).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path


@router.get("/projects", response_model=list[ProjectSummary])
async def list_projects(request: Request) -> list[ProjectSummary]:
    """List all .bdl project files."""
    projects_path = _projects_dir(request)
    results: list[ProjectSummary] = []
    for f in sorted(projects_path.glob("*.bdl"), key=lambda p: p.stat().st_mtime, reverse=True):
        results.append(ProjectSummary(name=f.stem, path=str(f)))
    return results


@router.get("/projects/{name}", response_model=ProjectDetail)
async def get_project(name: str, request: Request) -> ProjectDetail:
    """Read a specific project's BDL content.

    Args:
        name: Project name (without .bdl extension).
        request: The incoming request.

    Raises:
        HTTPException: 404 if the project file does not exist.
    """
    projects_path = _projects_dir(request)
    file_path = projects_path / f"{name}.bdl"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
    content = file_path.read_text(encoding="utf-8")
    return ProjectDetail(name=name, path=str(file_path), content=content)


@router.post("/projects", response_model=ProjectDetail, status_code=201)
async def create_project(body: ProjectCreate, request: Request) -> ProjectDetail:
    """Create a new project (save BDL file).

    Args:
        body: Project name and BDL content.
        request: The incoming request.

    Raises:
        HTTPException: 409 if a project with that name already exists.
    """
    projects_path = _projects_dir(request)
    file_path = projects_path / f"{body.name}.bdl"
    if file_path.exists():
        raise HTTPException(
            status_code=409, detail=f"Project '{body.name}' already exists"
        )
    file_path.write_text(body.content, encoding="utf-8")

    # Notify scheduler registry of new project
    registry = getattr(request.app.state, "scheduler_registry", None)
    if registry:
        registry.reload(body.name)

    return ProjectDetail(name=body.name, path=str(file_path), content=body.content)


@router.put("/projects/{name}", response_model=ProjectDetail)
async def update_project(
    name: str, body: ProjectUpdate, request: Request
) -> ProjectDetail:
    """Update an existing project.

    Args:
        name: Project name.
        body: Updated BDL content.
        request: The incoming request.

    Raises:
        HTTPException: 404 if the project file does not exist.
    """
    projects_path = _projects_dir(request)
    file_path = projects_path / f"{name}.bdl"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
    file_path.write_text(body.content, encoding="utf-8")

    # Notify scheduler registry of updated project
    registry = getattr(request.app.state, "scheduler_registry", None)
    if registry:
        registry.reload(name)

    return ProjectDetail(name=name, path=str(file_path), content=body.content)


@router.delete("/projects/{name}", status_code=204)
async def delete_project(name: str, request: Request) -> None:
    """Delete a project.

    Args:
        name: Project name.
        request: The incoming request.

    Raises:
        HTTPException: 404 if the project file does not exist.
    """
    projects_path = _projects_dir(request)
    file_path = projects_path / f"{name}.bdl"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
    # Stop scheduler before deleting
    registry = getattr(request.app.state, "scheduler_registry", None)
    if registry:
        registry.stop(name)

    file_path.unlink()
