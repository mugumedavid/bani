"""Schema inspection routes (Section 20.3).

Provides an endpoint for introspecting a database schema via the SDK's
SchemaInspector.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException

from bani.ui.auth import verify_token
from bani.ui.models import (
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    SchemaInspectRequest,
    SchemaInspectResponse,
    TableInfo,
)

router = APIRouter(tags=["schema"], dependencies=[Depends(verify_token)])


@router.post("/schema/inspect", response_model=SchemaInspectResponse)
async def inspect_schema(body: SchemaInspectRequest) -> SchemaInspectResponse:
    """Introspect a database schema.

    Runs the schema inspection in a background thread since the
    underlying connector operations are synchronous / blocking.

    Args:
        body: Connection details for the database to inspect.

    Returns:
        The introspected schema as a structured response.

    Raises:
        HTTPException: 400 if the dialect is not found.
        HTTPException: 500 if introspection fails.
    """

    def _inspect() -> SchemaInspectResponse:
        import os

        # Ensure MSSQL connector is imported so ODBCSYSINI gets set
        # before pyodbc tries to find the ODBC driver.
        import bani.connectors.mssql.connector  # noqa: F401
        from bani.sdk.schema_inspector import SchemaInspector

        # Handle credential modes:
        # - Env var mode: pass the env var name directly
        # - Direct mode: set as temp env var, pass the temp name
        if body.username_is_env:
            user_env = body.username_env
        else:
            user_env = "_BANI_UI_USER"
            if body.username_env:
                os.environ[user_env] = body.username_env

        if body.password_is_env:
            pass_env = body.password_env
        else:
            pass_env = "_BANI_UI_PASS"
            if body.password_env:
                os.environ[pass_env] = body.password_env

        try:
            import logging as _log

            _log.getLogger("bani.ui").info(
                "Schema inspect: dialect=%s host=%s port=%s ODBCSYSINI=%s",
                body.resolved_dialect,
                body.host,
                body.port,
                os.environ.get("ODBCSYSINI", "NOT SET"),
            )
            db_schema = SchemaInspector.inspect(
                dialect=body.resolved_dialect,
                host=body.host,
                port=body.port,
                database=body.database,
                username_env=user_env,
                password_env=pass_env,
                **body.extra,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            # Only clean up temp env vars (not real ones)
            if not body.username_is_env:
                os.environ.pop("_BANI_UI_USER", None)
            if not body.password_is_env:
                os.environ.pop("_BANI_UI_PASS", None)

        tables: list[TableInfo] = []
        for t in db_schema.tables:
            columns = [
                ColumnInfo(
                    name=c.name,
                    data_type=c.data_type,
                    nullable=c.nullable,
                    default_value=c.default_value,
                    is_auto_increment=c.is_auto_increment,
                    arrow_type_str=c.arrow_type_str,
                )
                for c in t.columns
            ]
            indexes = [
                IndexInfo(
                    name=idx.name,
                    columns=list(idx.columns),
                    is_unique=idx.is_unique,
                )
                for idx in t.indexes
            ]
            foreign_keys = [
                ForeignKeyInfo(
                    name=fk.name,
                    source_table=fk.source_table,
                    source_columns=list(fk.source_columns),
                    referenced_table=fk.referenced_table,
                    referenced_columns=list(fk.referenced_columns),
                )
                for fk in t.foreign_keys
            ]
            tables.append(
                TableInfo(
                    schema_name=t.schema_name,
                    table_name=t.table_name,
                    columns=columns,
                    primary_key=list(t.primary_key),
                    indexes=indexes,
                    foreign_keys=foreign_keys,
                    row_count_estimate=t.row_count_estimate,
                )
            )

        return SchemaInspectResponse(
            source_dialect=db_schema.source_dialect,
            tables=tables,
        )

    try:
        return await asyncio.to_thread(_inspect)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
