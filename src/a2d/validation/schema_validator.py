"""Schema comparison validation for pre/post migration checks."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SchemaComparisonResult:
    """Result of comparing source and target schemas."""

    matching_columns: list[str] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)
    extra_columns: list[str] = field(default_factory=list)
    type_mismatches: list[dict] = field(default_factory=list)
    is_compatible: bool = True


class SchemaValidator:
    """Validate that target schema is compatible with source schema."""

    def compare_schemas(
        self,
        source_fields: list[dict[str, str]],
        target_fields: list[dict[str, str]],
    ) -> SchemaComparisonResult:
        """Compare source and target field lists.

        Each field dict should have at minimum ``name`` and ``type`` keys.

        Args:
            source_fields: List of dicts with ``name`` and ``type`` from the source.
            target_fields: List of dicts with ``name`` and ``type`` from the target.

        Returns:
            A SchemaComparisonResult with matching, missing, extra columns and
            any type mismatches.
        """
        source_by_name = {f["name"]: f.get("type", "") for f in source_fields}
        target_by_name = {f["name"]: f.get("type", "") for f in target_fields}

        source_names = set(source_by_name.keys())
        target_names = set(target_by_name.keys())

        matching = sorted(source_names & target_names)
        missing = sorted(source_names - target_names)
        extra = sorted(target_names - source_names)

        type_mismatches = []
        for col in matching:
            source_type = source_by_name[col]
            target_type = target_by_name[col]
            if source_type and target_type and not self._types_compatible(source_type, target_type):
                type_mismatches.append(
                    {
                        "column": col,
                        "source_type": source_type,
                        "target_type": target_type,
                    }
                )

        is_compatible = len(missing) == 0 and len(type_mismatches) == 0

        return SchemaComparisonResult(
            matching_columns=matching,
            missing_columns=missing,
            extra_columns=extra,
            type_mismatches=type_mismatches,
            is_compatible=is_compatible,
        )

    @staticmethod
    def _types_compatible(source_type: str, target_type: str) -> bool:
        """Check if two type strings are compatible.

        Uses a simple normalization: strip whitespace, lowercase, and check
        known compatible type families.
        """
        s = source_type.strip().lower()
        t = target_type.strip().lower()

        if s == t:
            return True

        # Define compatible type groups
        int_types = {"int", "int16", "int32", "int64", "integer", "bigint", "smallint", "tinyint", "byte"}
        float_types = {"float", "double", "decimal", "fixeddecimal", "numeric", "real"}
        string_types = {"string", "wstring", "v_string", "v_wstring", "varchar", "text", "str"}
        date_types = {"date", "datetime", "timestamp", "time"}
        bool_types = {"bool", "boolean"}

        for group in [int_types, float_types, string_types, date_types, bool_types]:
            if s in group and t in group:
                return True

        return False
