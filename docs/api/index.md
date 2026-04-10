# API Reference

The API reference is auto-generated from source code docstrings using [mkdocstrings](https://mkdocstrings.github.io/). All public classes and methods are documented with Google-style docstrings.

---

## Sections

| Module | Description |
|---|---|
| [SDK](sdk.md) | Public SDK classes: `Bani`, `BaniProject`, `ProjectBuilder`, `SchemaInspector` |
| [Domain Models](domain.md) | Core domain types: `ProjectModel`, `DatabaseSchema`, exception hierarchy |
| [Connectors](connectors.md) | Abstract base classes: `SourceConnector`, `SinkConnector` |

---

## Usage

The SDK is the primary public API for programmatic use:

```python
from bani.sdk.bani import Bani, BaniProject
from bani.sdk.project_builder import ProjectBuilder
from bani.sdk.schema_inspector import SchemaInspector
```

Domain models are used throughout the SDK and connectors:

```python
from bani.domain.project import ProjectModel, ConnectionConfig
from bani.domain.schema import DatabaseSchema, TableDefinition
from bani.domain.errors import BaniError, BDLValidationError
```

The connector base classes define the interface that all connectors implement:

```python
from bani.connectors.base import SourceConnector, SinkConnector
```
