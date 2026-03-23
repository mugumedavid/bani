"""Type mapping engine for translating column types between database dialects.

The ``TypeMapper`` loads default mappings from JSON files (one per connector)
and supports user overrides via ``MappingRuleSet``. Unmapped types fall back
to the connector's defaults (Section 11).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from bani.domain.errors import TypeMappingError


@dataclass(frozen=True)
class MappingRule:
    """A single type-mapping rule.

    Attributes:
        source_type: Source type pattern (case-insensitive), e.g. ``"VARCHAR"``.
        target_type: Target type to map to, e.g. ``"TEXT"``.
        source_dialect: Dialect this rule applies to, or ``"*"`` for any.
        target_dialect: Dialect this rule produces, or ``"*"`` for any.
    """

    source_type: str
    target_type: str
    source_dialect: str = "*"
    target_dialect: str = "*"

    def matches(
        self,
        source_type: str,
        source_dialect: str,
        target_dialect: str,
    ) -> bool:
        """Check whether this rule applies to the given type and dialect pair.

        Args:
            source_type: The source column type to match against.
            source_dialect: The source database dialect.
            target_dialect: The target database dialect.

        Returns:
            ``True`` if this rule matches the given parameters.
        """
        type_match = self.source_type.upper() == source_type.upper().split("(")[0]
        src_match = self.source_dialect in ("*", source_dialect)
        tgt_match = self.target_dialect in ("*", target_dialect)
        return type_match and src_match and tgt_match


@dataclass(frozen=True)
class MappingRuleSet:
    """An ordered collection of mapping rules.

    Rules are evaluated in order; the first match wins.

    Attributes:
        rules: Tuple of mapping rules.
        name: Optional name for this rule set (e.g. ``"user-overrides"``).
    """

    rules: tuple[MappingRule, ...] = ()
    name: str = ""

    def __iter__(self) -> Iterator[MappingRule]:
        """Iterate over the rules in this set."""
        return iter(self.rules)

    def __len__(self) -> int:
        """Return the number of rules in this set."""
        return len(self.rules)


class TypeMapper:
    """Maps source column types to target column types.

    Resolution order:
    1. User-override rules (if provided).
    2. Default rules loaded from a JSON file.
    3. If no rule matches, raises ``TypeMappingError``.

    Args:
        default_rules: The base set of rules from the connector's
            ``type_defaults.json``.
        override_rules: Optional user-supplied overrides that take priority.
    """

    def __init__(
        self,
        default_rules: MappingRuleSet | None = None,
        override_rules: MappingRuleSet | None = None,
    ) -> None:
        self._default_rules = (
            default_rules if default_rules is not None else MappingRuleSet()
        )
        self._override_rules = (
            override_rules if override_rules is not None else MappingRuleSet()
        )

    @property
    def default_rules(self) -> MappingRuleSet:
        """The base mapping rules."""
        return self._default_rules

    @property
    def override_rules(self) -> MappingRuleSet:
        """User-supplied override rules."""
        return self._override_rules

    def map_type(
        self,
        source_type: str,
        source_dialect: str,
        target_dialect: str,
    ) -> str:
        """Resolve a source column type to a target column type.

        Args:
            source_type: The raw source type string, e.g. ``"VARCHAR(255)"``.
            source_dialect: Source database dialect, e.g. ``"mysql"``.
            target_dialect: Target database dialect, e.g. ``"postgresql"``.

        Returns:
            The mapped target type string.

        Raises:
            TypeMappingError: If no mapping rule matches the source type.
        """
        # Check overrides first
        for rule in self._override_rules:
            if rule.matches(source_type, source_dialect, target_dialect):
                return rule.target_type

        # Then defaults
        for rule in self._default_rules:
            if rule.matches(source_type, source_dialect, target_dialect):
                return rule.target_type

        raise TypeMappingError(
            f"No mapping found for type '{source_type}' "
            f"from {source_dialect} to {target_dialect}",
            source_type=source_type,
            target_dialect=target_dialect,
        )

    @classmethod
    def from_json_file(
        cls,
        path: Path,
        override_rules: MappingRuleSet | None = None,
    ) -> TypeMapper:
        """Load default mapping rules from a JSON file.

        The JSON file should contain an array of objects with keys:
        ``source_type``, ``target_type``, and optionally
        ``source_dialect`` and ``target_dialect``.

        Args:
            path: Path to the JSON mapping file.
            override_rules: Optional user overrides.

        Returns:
            A configured ``TypeMapper``.
        """
        with open(path) as f:
            data = json.load(f)

        rules = tuple(
            MappingRule(
                source_type=entry["source_type"],
                target_type=entry["target_type"],
                source_dialect=entry.get("source_dialect", "*"),
                target_dialect=entry.get("target_dialect", "*"),
            )
            for entry in data
        )

        return cls(
            default_rules=MappingRuleSet(rules=rules, name=path.stem),
            override_rules=override_rules,
        )
