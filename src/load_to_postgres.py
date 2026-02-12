# src/load_to_postgres.py
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg


DDL = """
CREATE TABLE IF NOT EXISTS countries_clean (
  country TEXT,
  code TEXT,
  population BIGINT NULL,
  gdp_per_capita DOUBLE PRECISION NULL
);

CREATE TABLE IF NOT EXISTS countries_quarantine (
  country TEXT,
  code TEXT,
  population BIGINT NULL,
  gdp_per_capita DOUBLE PRECISION NULL,
  quarantine_reason TEXT
);

CREATE TABLE IF NOT EXISTS summer_clean (
  year INT,
  city TEXT,
  sport TEXT,
  discipline TEXT,
  athlete TEXT,
  code TEXT,
  gender TEXT,
  event TEXT,
  medal TEXT,
  country TEXT
);

CREATE TABLE IF NOT EXISTS summer_quarantine (
  year INT NULL,
  city TEXT,
  sport TEXT,
  discipline TEXT,
  athlete TEXT,
  code TEXT,
  gender TEXT,
  event TEXT,
  medal TEXT,
  country TEXT,
  quarantine_reason TEXT
);

CREATE TABLE IF NOT EXISTS validation_runs (
  run_id TEXT,
  timestamp TIMESTAMPTZ,
  suite TEXT,
  success BOOLEAN,
  file_used TEXT
);
"""


def _read_csv(path: Path) -> pd.DataFrame:
    """
    Reads CSV and drops the unnamed index column if present.
    Tolerates files saved with index=True in prior steps.
    """
    df = pd.read_csv(path)
    if df.columns.size > 0 and str(df.columns[0]).strip().lower() in ("unnamed: 0", ""):
        df = df.drop(columns=[df.columns[0]])
    return df


def _strip_and_nullify(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strips whitespace on string columns and converts "" -> NA so they insert as NULL.
    """
    out = df.copy()
    for c in out.columns:
        if out[c].dtype == object or str(out[c].dtype).startswith("string"):
            out[c] = out[c].astype("string").str.strip()
            out[c] = out[c].replace({"": pd.NA})
    return out


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes column names from your CSVs (Title Case) to match Postgres DDL (lowercase).
    """
    rename_map = {
        # Countries
        "Country": "country",
        "Code": "code",
        "Population": "population",
        "GDP per Capita": "gdp_per_capita",

        # Summer
        "Year": "year",
        "City": "city",
        "Sport": "sport",
        "Discipline": "discipline",
        "Athlete": "athlete",
        "Gender": "gender",
        "Event": "event",
        "Medal": "medal",

        # Quarantine
        "quarantine_reason": "quarantine_reason",
        "Quarantine_Reason": "quarantine_reason",
    }
    return df.rename(columns={c: rename_map.get(c, c) for c in df.columns})


def _truncate(cur) -> None:
    # deterministic reruns
    cur.execute("TRUNCATE TABLE countries_clean;")
    cur.execute("TRUNCATE TABLE countries_quarantine;")
    cur.execute("TRUNCATE TABLE summer_clean;")
    cur.execute("TRUNCATE TABLE summer_quarantine;")


def _insert_df(cur, table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return

    cols = list(df.columns)
    col_list = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f'INSERT INTO {table} ({col_list}) VALUES ({placeholders})'

    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df.itertuples(index=False, name=None)
    ]
    cur.executemany(sql, rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Step 7 — load processed CSVs into Postgres (Neon).")
    parser.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Directory containing processed CSVs (default: data/processed)",
    )
    parser.add_argument(
        "--database-url",
        default="",
        help="Postgres connection string. If omitted, uses env DATABASE_URL.",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id for audit logging (default: timestamp-based).",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Do not TRUNCATE tables before loading (default: truncate).",
    )
    args = parser.parse_args()

    processed_dir = Path(args.processed_dir)
    db_url = args.database_url.strip() or os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        raise SystemExit("ERROR: Provide --database-url or set $env:DATABASE_URL")

    run_id = args.run_id.strip() or datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S_utc")
    ts = datetime.now(timezone.utc)

    # Expect these 4 files from Step 6
    paths = {
        "countries_clean": processed_dir / "countries_clean.csv",
        "countries_quarantine": processed_dir / "countries_quarantine.csv",
        "summer_clean": processed_dir / "summer_clean.csv",
        "summer_quarantine": processed_dir / "summer_quarantine.csv",
    }

    # Validate files exist
    for name, p in paths.items():
        if not p.exists():
            raise SystemExit(f"ERROR: Missing required file: {p} ({name})")

    # Read + normalize (IMPORTANT: outside the loop)
    countries_clean = _normalize_cols(_strip_and_nullify(_read_csv(paths["countries_clean"])))
    countries_quarantine = _normalize_cols(_strip_and_nullify(_read_csv(paths["countries_quarantine"])))
    summer_clean = _normalize_cols(_strip_and_nullify(_read_csv(paths["summer_clean"])))
    summer_quarantine = _normalize_cols(_strip_and_nullify(_read_csv(paths["summer_quarantine"])))

    # Optional: ensure numeric types insert cleanly
    if "year" in summer_clean.columns:
        summer_clean["year"] = pd.to_numeric(summer_clean["year"], errors="coerce").astype("Int64")
    if "year" in summer_quarantine.columns:
        summer_quarantine["year"] = pd.to_numeric(summer_quarantine["year"], errors="coerce").astype("Int64")

    if "population" in countries_clean.columns:
        countries_clean["population"] = pd.to_numeric(countries_clean["population"], errors="coerce")
    if "population" in countries_quarantine.columns:
        countries_quarantine["population"] = pd.to_numeric(countries_quarantine["population"], errors="coerce")

    if "gdp_per_capita" in countries_clean.columns:
        countries_clean["gdp_per_capita"] = pd.to_numeric(countries_clean["gdp_per_capita"], errors="coerce")
    if "gdp_per_capita" in countries_quarantine.columns:
        countries_quarantine["gdp_per_capita"] = pd.to_numeric(countries_quarantine["gdp_per_capita"], errors="coerce")

    # Connect + load
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            # Create tables
            cur.execute(DDL)

            # Reset tables for deterministic reruns
            if not args.no_truncate:
                _truncate(cur)

            # Insert data
            _insert_df(cur, "countries_clean", countries_clean)
            _insert_df(cur, "countries_quarantine", countries_quarantine)
            _insert_df(cur, "summer_clean", summer_clean)
            _insert_df(cur, "summer_quarantine", summer_quarantine)

            # Audit log
            cur.execute(
                """
                INSERT INTO validation_runs (run_id, timestamp, suite, success, file_used)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (run_id, ts, "load_to_postgres", True, str(processed_dir)),
            )

        conn.commit()

    print("Step 7 complete — loaded processed CSVs into Postgres.")
    print(f"run_id: {run_id}")
    print(f"countries_clean: {len(countries_clean)}")
    print(f"countries_quarantine: {len(countries_quarantine)}")
    print(f"summer_clean: {len(summer_clean)}")
    print(f"summer_quarantine: {len(summer_quarantine)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())