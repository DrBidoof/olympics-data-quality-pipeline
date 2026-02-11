from __future__ import annotations

import json
import os
from datetime import datetime

import pandas as pd
import great_expectations as gx
from great_expectations.exceptions import DataContextError


CSV_PATH = r"C:\Users\dartb\OneDrive\Documents\health infomatics\projects\python\1.olympic pipe line\olympics-data-quality-pipeline\data\sample\countries_sample.csv"

SUITE_NAME = "countries_suite"
DS_NAME = "pandas_tmp"


def main() -> int:
    context = gx.get_context(mode="file")

    # Step requirement: read with index_col=0
    df = pd.read_csv(CSV_PATH, index_col=0)

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

    validator.expect_column_values_to_be_between(
        "Population",
        min_value=1,
        max_value=2_000_000_000,
        mostly=0.99,
    )

    validator.expect_column_values_to_be_between(
        "GDP per Capita",
        min_value=1,
        max_value=300_000,
        mostly=0.99,
    )
    # ------------------------------------------------

    # Save / update suite
    suite = validator.get_expectation_suite()
    context.suites.add_or_update(suite)

    # Run validation
    results = validator.validate()

    # ---------------- Save JSON evidence ----------------
    script_dir = os.path.dirname(os.path.abspath(__file__))  # src/
    project_root = os.path.dirname(script_dir)

    out_dir = os.path.join(project_root, "reports", "validations")
    os.makedirs(out_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(out_dir, f"countries_{ts}.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results.to_json_dict(), f, indent=2)

    print(f"Saved validation JSON to: {out_path}")
    print("Validation successful:", results["success"])

    # Optional: build docs (can comment out later for speed)
    context.build_data_docs()

    return 0 if results["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
