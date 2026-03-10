"""Post-migration data validation comparing source and target datasets."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DataValidationResult:
    """Result of comparing source and target data."""

    row_count_match: bool = True
    source_rows: int = 0
    target_rows: int = 0
    column_mismatches: list[str] = field(default_factory=list)
    sample_differences: list[dict] = field(default_factory=list)


class DataValidator:
    """Post-migration data validation using DataComPy (optional dependency).

    DataComPy is an optional dependency. If not installed, the validator
    falls back to basic row count and column comparison.
    """

    def validate(
        self,
        source_path: str | Path,
        target_path: str | Path,
        join_columns: list[str],
    ) -> DataValidationResult:
        """Validate that target data matches source data.

        Args:
            source_path: Path to source CSV file.
            target_path: Path to target CSV file.
            join_columns: Columns to join on for row-level comparison.

        Returns:
            DataValidationResult with comparison details.
        """
        source_path = Path(source_path)
        target_path = Path(target_path)

        result = DataValidationResult()

        try:
            import pandas as pd
        except ImportError:
            result.column_mismatches.append("pandas not installed; cannot perform data validation")
            result.row_count_match = False
            return result

        # Read data
        try:
            source_df = pd.read_csv(source_path)
            target_df = pd.read_csv(target_path)
        except Exception as e:
            result.column_mismatches.append(f"Error reading files: {e}")
            result.row_count_match = False
            return result

        result.source_rows = len(source_df)
        result.target_rows = len(target_df)
        result.row_count_match = result.source_rows == result.target_rows

        # Column comparison
        source_cols = set(source_df.columns)
        target_cols = set(target_df.columns)
        missing = source_cols - target_cols
        extra = target_cols - source_cols
        if missing:
            result.column_mismatches.append(f"Missing in target: {sorted(missing)}")
        if extra:
            result.column_mismatches.append(f"Extra in target: {sorted(extra)}")

        # Try DataComPy for detailed comparison
        try:
            import datacompy

            compare = datacompy.Compare(
                source_df,
                target_df,
                join_columns=join_columns,
                df1_name="source",
                df2_name="target",
            )

            if not compare.matches():
                # Get sample of differences
                mismatched = compare.all_mismatch()
                if mismatched is not None and len(mismatched) > 0:
                    for _, row in mismatched.head(5).iterrows():
                        result.sample_differences.append(row.to_dict())
        except ImportError:
            # DataComPy not installed, skip detailed comparison
            pass
        except Exception as e:
            result.column_mismatches.append(f"DataComPy comparison error: {e}")

        return result
