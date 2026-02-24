# src/write_run_metadata.py
from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg


DDL = """
CREATE TABLE IF NOT EXISTS validation_runs (
  run_id TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  strict_mode BOOLEAN NOT NULL,
  overall_ok BOOLEAN NOT NULL,

  gx_success_countries BOOLEAN,
  gx_success_summer BOOLEAN,

  integrity_should_fail BOOLEAN,
  bad_rows_total INTEGER,
  bad_rows_null_code INTEGER,
  bad_rows_code_not_in_countries_strict INTEGER,

  countries_rows_total INTEGER,
  summer_rows_total INTEGER,
  countries_clean_rows INTEGER,
  countries_quarantine_rows INTEGER,
  summer_clean_rows INTEGER,
  summer_quarantine_rows INTEGER,

  run_summary JSONB
);
"""


UPSERT = """
INSERT INTO validation_runs (
  run_id,
  strict_mode,
  overall_ok,
  gx_success_countries,
  gx_success_summer,
  integrity_should_fail,
  bad_rows_total,
  bad_rows_null_code,
  bad_rows_code_not_in_countries_strict,
  countries_rows_total,
  summer_rows_total,
  countries_clean_rows,
  countries_quarantine_rows,
  summer_clean_rows,
  summer_quarantine_rows,
  run_summary
)
VALUES (
  %(run_id)s,
  %(strict_mode)s,
  %(overall_ok)s,
  %(gx_success_countries)s,
  %(gx_success_summer)s,
  %(integrity_should_fail)s,
  %(bad_rows_total)s,
  %(bad_rows_null_code)s,
  %(bad_rows_code_not_in_countries_strict)s,
  %(countries_rows_total)s,
  %(summer_rows_total)s,
  %(countries_clean_rows)s,
  %(countries_quarantine_rows)s,
  %(summer_clean_rows)s,
  %(summer_quarantine_rows)s,
  %(run_summary)s::jsonb
)
ON CONFLICT (run_id) DO UPDATE SET
  strict_mode = EXCLUDED.strict_mode,
  overall_ok = EXCLUDED.overall_ok,
  gx_success_countries = EXCLUDED.gx_success_countries,
  gx_success_summer = EXCLUDED.gx_success_summer,
  integrity_should_fail = EXCLUDED.integrity_should_fail,
  bad_rows_total = EXCLUDED.bad_rows_total,
  bad_rows_null_code = EXCLUDED.bad_rows_null_code,
  bad_rows_code_not_in_countries_strict = EXCLUDED.bad_rows_code_not_in_countries_strict,
  countries_rows_total = EXCLUDED.countries_rows_total,
  summer_rows_total = EXCLUDED.summer_rows_total,
  countries_clean_rows = EXCLUDED.countries_clean_rows,
  countries_quarantine_rows = EXCLUDED.countries_quarantine_rows,
  summer_clean_rows = EXCLUDED.summer_clean_rows,
  summer_quarantine_rows = EXCLUDED.summer_quarantine_rows,
  run_summary = EXCLUDED.run_summary;
"""


def get(d: dict, *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--database-url", required=True)
    ap.add_argument("--run-summary", required=True, help="Path to run_summary.json")
    args = ap.parse_args()

    summary_path = Path(args.run_summary)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    payload = {
        "run_id": summary.get("run_id"),
        "strict_mode": bool(summary.get("strict_mode", False)),
        "overall_ok": bool(summary.get("overall_ok", False)),

        # Adjust these keys to match your run_summary.json structure:
        "gx_success_countries": get(summary, "gx", "countries", "success", default=None),
        "gx_success_summer": get(summary, "gx", "summer", "success", default=None),

        "integrity_should_fail": get(summary, "integrity", "should_fail", default=None),
        "bad_rows_total": get(summary, "integrity", "bad_rows_total", default=None),
        "bad_rows_null_code": get(summary, "integrity", "bad_rows_null_code", default=None),
        "bad_rows_code_not_in_countries_strict": get(summary, "integrity", "bad_rows_code_not_in_countries_strict", default=None),

        "countries_rows_total": get(summary, "row_counts", "countries_rows_total", default=None),
        "summer_rows_total": get(summary, "row_counts", "summer_rows_total", default=None),
        "countries_clean_rows": get(summary, "row_counts", "countries_clean_rows", default=None),
        "countries_quarantine_rows": get(summary, "row_counts", "countries_quarantine_rows", default=None),
        "summer_clean_rows": get(summary, "row_counts", "summer_clean_rows", default=None),
        "summer_quarantine_rows": get(summary, "row_counts", "summer_quarantine_rows", default=None),

        "run_summary": json.dumps(summary),
    }

    if not payload["run_id"]:
        raise SystemExit("run_summary.json missing run_id")

    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(UPSERT, payload)

    print(f"Inserted/updated validation_runs for run_id={payload['run_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())