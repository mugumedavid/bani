"""BDL validation against XSD and JSON Schema."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from bani.domain.errors import ConfigurationError


def validate_xml(xml_content: str | bytes, xsd_path: Path | None = None) -> list[str]:
    """Validate XML content against XSD schema.

    Args:
        xml_content: XML content as string or bytes.
        xsd_path: Path to XSD schema file. If None, uses bundled schema.

    Returns:
        List of validation error messages (empty = valid).
    """
    try:
        from lxml import etree
    except ImportError as e:
        raise ConfigurationError(
            "lxml is required for XML validation. Install with: pip install lxml"
        ) from e

    if xsd_path is None:
        xsd_path = Path(__file__).parent / "schemas" / "bdl-1.0.xsd"

    if not xsd_path.exists():
        raise ConfigurationError(f"XSD schema file not found: {xsd_path}")

    try:
        if isinstance(xml_content, str):
            xml_content = xml_content.encode("utf-8")

        xml_doc = etree.fromstring(xml_content)
        xsd_doc = etree.parse(str(xsd_path))
        xsd_schema = etree.XMLSchema(xsd_doc)

        if not xsd_schema.validate(xml_doc):
            error_strings = []
            for error in xsd_schema.error_log:
                error_strings.append(str(error))
            return error_strings
        return []
    except Exception as e:
        return [f"XML validation error: {e}"]


def validate_json(
    json_content: str | dict[str, Any], schema_path: Path | None = None
) -> list[str]:
    """Validate JSON content against JSON Schema.

    Args:
        json_content: JSON content as string or dictionary.
        schema_path: Path to JSON Schema file. If None, uses bundled schema.

    Returns:
        List of validation error messages (empty = valid).
    """
    try:
        import jsonschema
    except ImportError as e:
        raise ConfigurationError(
            "jsonschema is required for JSON validation. "
            "Install with: pip install jsonschema"
        ) from e

    if schema_path is None:
        schema_path = Path(__file__).parent / "schemas" / "bdl-1.0.schema.json"

    if not schema_path.exists():
        raise ConfigurationError(f"JSON Schema file not found: {schema_path}")

    try:
        if isinstance(json_content, str):
            data = json.loads(json_content)
        else:
            data = json_content

        with open(schema_path) as f:
            schema = json.load(f)

        errors: list[str] = []
        validator = jsonschema.Draft7Validator(schema)
        for error in validator.iter_errors(data):
            errors.append(f"{error.message} at {'.'.join(str(p) for p in error.path)}")

        return errors
    except json.JSONDecodeError as e:
        return [f"JSON parsing error: {e}"]
    except Exception as e:
        return [f"JSON validation error: {e}"]
