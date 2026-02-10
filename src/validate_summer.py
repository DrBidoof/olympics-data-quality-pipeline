from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import great_expectations as gx
from great_expectations.exceptions import DataContextError

SUITE_NAME = "summer_suite"
DS_NAME = "pandas_tmp"  

REQUIRED_COLS = [
    "Year",
    "City",
    "Sport",
    "Discipline",
    "Athlete",
    "Code",
    "Gender",
    "Event",
    "Medal",
    "Country",
]


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def save_validation_json(results: Dict[str, Any], out_dir: Path, prefix: str) -> Path:
    ensure_dir(out_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"{prefix}_{ts}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    return out_path


def get_or_create_suite(context, suite_name: str):
    try:
        return context.suites.get(suite_name)
    except DataContextError:
        return context.suites.add(gx.ExpectationSuite(name=suite_name))


def get_or_create_pandas_ds(context, ds_name: str):
    try:
        return context.data_sources.get(ds_name)
    except Exception:
        return context.data_sources.add_pandas(name=ds_name)


def normalize_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Code column: strip + uppercase, without turning NaN into 'NAN'.
    """
    if "Code" not in df.columns:
        return df

    # Only transform non-null values
    s = df["Code"]
    non_null = s.notna()
    # Convert to string, then strip/upper
    df.loc[non_null, "Code"] = (
        s.loc[non_null].astype(str).str.strip().str.upper()
    )
    return df


def main(csv_path: str, reports_root: str = "reports/validations") -> int:
    context = gx.get_context(mode="file")

    # Step 4 requirement: read CSV with index_col=0
    df = pd.read_csv(csv_path, index_col=0)

    # Step 4 requirement: normalize Code (strip + uppercase) before expectations
    df = normalize_code(df)

    ds = get_or_create_pandas_ds(context, DS_NAME)
    batch = ds.read_dataframe(df)

    # Ensure suite exists
    get_or_create_suite(context, SUITE_NAME)

    validator = context.get_validator(batch=batch, expectation_suite_name=SUITE_NAME)

    # --- Expectations (Step 4 checklist) ---

    # Strict schema: 10 columns, correct order
    validator.expect_table_columns_to_match_ordered_list(REQUIRED_COLS)

    # Year between 1896–current
    current_year = datetime.now().year
    validator.expect_column_values_to_be_between(
        "Year",
        min_value=1896,
        max_value=current_year,
        mostly=1.0,
    )

    # Athlete not null
    validator.expect_column_values_to_not_be_null("Athlete", mostly=1.0)

    # Gender = Men/Women
    validator.expect_column_values_to_be_in_set("Gender", ["Men", "Women"], mostly=1.0)

    # Code regex ^[A-Z]{3}$
    validator.expect_column_values_to_not_be_null("Code", mostly=1.0)
    validator.expect_column_values_to_match_regex("Code", r"^[A-Z]{3}$", mostly=1.0)

    # Medal in Gold/Silver/Bronze
    validator.expect_column_values_to_be_in_set(
        "Medal", ["Gold", "Silver", "Bronze"], mostly=1.0
    )
    
    # Country not null
    validator.expect_column_values_to_not_be_null("Country", mostly=1.0)

    # Save GX suite safely (add_or_update)
    suite = validator.get_expectation_suite()
    context.suites.add_or_update(suite)

    # Run validation
    results = validator.validate()

    # Save JSON report
    out_path = save_validation_json(results, Path(reports_root), "summer")
    print(f"Saved validation JSON to: {out_path.as_posix()}")
    print("Validation successful:", results.get("success", False))

    # Confirm Validation successful: True (exit code mirrors success)
    return 0 if results.get("success", False) else 1


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Great Expectations validation for Summer dataset")
    ap.add_argument("--input", required=True, help="Path to summer CSV")
    ap.add_argument("--reports", default="reports/validations", help="Reports output directory")
    args = ap.parse_args()

    raise SystemExit(main(args.input, args.reports))
