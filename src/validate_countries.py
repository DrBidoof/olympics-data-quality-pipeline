from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import great_expectations as gx
from great_expectations.exceptions import DataContextError


CSV_PATH = r"C:\Users\dartb\OneDrive\Documents\health infomatics\projects\python\1.olympic pipe line\olympics-data-quality-pipeline\data\sample\countries_sample.csv"

SUITE_NAME = "countries_suite"
DS_NAME = "pandas_tmp"

NUMERIC_COLS = ["Population", "GDP per Capita"]


def coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure numeric columns are truly numeric so GX 'between' checks don't crash.
    Handles blanks, whitespace, and values like "1,234".
    """
    df = df.copy()

    for col in NUMERIC_COLS:
        if col not in df.columns:
            continue

        # Normalize to string, remove commas, strip whitespace, convert blanks to NA
        s = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        )

        # Convert to numeric; invalid parses become NaN
        df[col] = pd.to_numeric(s, errors="coerce")

    return df


def main() -> int:
    context = gx.get_context(mode="file")

    # Step requirement: read with index_col=0
    df = pd.read_csv(CSV_PATH, index_col=0)

    # ✅ IMPORTANT: coerce numeric cols before GX batch is created
    df = coerce_numeric_columns(df)

    # get-or-create datasource
    try:
        ds = context.data_sources.get(DS_NAME)
    except Exception:
        ds = context.data_sources.add_pandas(name=DS_NAME)

    batch = ds.read_dataframe(df)

    # get-or-create suite
    try:
        context.suites.get(SUITE_NAME)
    except DataContextError:
        context.suites.add(gx.ExpectationSuite(name=SUITE_NAME))

    validator = context.get_validator(batch=batch, expectation_suite_name=SUITE_NAME)

    # ---------------- Expectations ----------------
    validator.expect_table_columns_to_match_ordered_list(
        ["Country", "Code", "Population", "GDP per Capita"]
    )

    validator.expect_column_values_to_not_be_null("Country")
    validator.expect_column_values_to_be_unique("Country")

    validator.expect_column_values_to_not_be_null("Code")
    validator.expect_column_values_to_be_unique("Code")
    validator.expect_column_values_to_match_regex("Code", r"^[A-Z]{3}$")

    # Now safe: column is numeric (float) and min/max are numeric types too
    validator.expect_column_values_to_be_between(
        "Population",
        min_value=1,
        max_value=2_000_000_000,
        mostly=0.99,
    )

    validator.expect_column_values_to_be_between(
        "GDP per Capita",
        min_value=1.0,
        max_value=300_000.0,
        mostly=0.99,
    )
    # ------------------------------------------------

    # Save / update suite
    suite = validator.get_expectation_suite()
    context.suites.add_or_update(suite)

    # Run validation
    results = validator.validate()

    # ---------------- Save JSON evidence ----------------
    script_dir = Path(__file__).resolve().parent  # src/
    project_root = script_dir.parent

    out_dir = project_root / "reports" / "validations"
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"countries_{ts}.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(results.to_json_dict(), f, indent=2)

    print(f"Saved validation JSON to: {out_path}")
    print("Validation successful:", results["success"])

    # Optional: build docs (can comment out later for speed)
    context.build_data_docs()

    return 0 if results["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())