from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd

# ---- Defaults ----
DEFAULT_COUNTRIES_CSV = r"C:\Users\dartb\OneDrive\Documents\health infomatics\projects\python\1.olympic pipe line\olympics-data-quality-pipeline\data\sample\countries_sample.csv"
DEFAULT_SUMMER_CSV = r"C:\Users\dartb\OneDrive\Documents\health infomatics\projects\python\1.olympic pipe line\olympics-data-quality-pipeline\data\sample\summer_sample.csv"
DEFAULT_CODE_MAP_CSV = r"C:\Users\dartb\OneDrive\Documents\health infomatics\projects\python\1.olympic pipe line\olympics-data-quality-pipeline\data\reference\code_map.csv"

# Policy
HISTORICAL_CODE_ALLOWLIST = {"BOH"}  # expand later if needed
FAIL_ON_NULL_CODES = False          # Step 6 quarantines these

# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def normalize_code_series(s: pd.Series) -> pd.Series:
    out = s.copy()
    mask = out.notna()
    out.loc[mask] = out.loc[mask].astype(str).str.strip().str.upper()
    return out


def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, index_col=0)


def load_code_map(path: str) -> dict[str, str]:
    """
    Expect CSV with columns: from_code,to_code
    """
    df = pd.read_csv(path)
    df["from_code"] = normalize_code_series(df["from_code"])
    df["to_code"] = normalize_code_series(df["to_code"])
    return dict(zip(df["from_code"], df["to_code"]))


# -------------------------------------------------------
# Core Logic
# -------------------------------------------------------

def find_bad_code_rows(
    summer_df: pd.DataFrame,
    countries_df: pd.DataFrame,
    code_map: dict[str, str] | None = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:

    summer = summer_df.copy()
    countries = countries_df.copy()

    # Normalize
    summer["Code"] = normalize_code_series(summer["Code"])
    countries["Code"] = normalize_code_series(countries["Code"])

    # Apply mapping (Summer → Countries code system)
    if code_map:
        summer["Code_raw"] = summer["Code"]
        summer["Code"] = summer["Code"].replace(code_map)
        mapped_count = int((summer["Code_raw"] != summer["Code"]).sum())
    else:
        mapped_count = 0

    summer_total = int(len(summer))
    countries_total = int(len(countries))

    valid_codes = set(countries["Code"].dropna().unique().tolist())

    code_is_null = summer["Code"].isna()
    code_not_in = ~summer["Code"].isin(valid_codes)

    bad_mask = code_is_null | code_not_in
    bad_rows = summer.loc[bad_mask].copy()

    bad_total = int(len(bad_rows))
    bad_null_code = int(code_is_null.sum())
    bad_not_in = int((~code_is_null & code_not_in).sum())

    unique_bad_codes = sorted([c for c in bad_rows["Code"].dropna().unique().tolist()])

    summary: Dict[str, Any] = {
        "run_timestamp": datetime.now().isoformat(),
        "summer_rows_total": summer_total,
        "countries_rows_total": countries_total,
        "valid_country_codes_count": int(len(valid_codes)),
        "mapped_rows_count": mapped_count,
        "bad_rows_total": bad_total,
        "bad_rows_null_code": bad_null_code,
        "bad_rows_code_not_in_countries": bad_not_in,
        "unique_bad_codes_count": int(len(unique_bad_codes)),
        "unique_bad_codes_sample": unique_bad_codes[:25],
    }

    # --------------------------
    # Strict failure policy
    # --------------------------
    # strict "not-in-countries" codes exclude allowlisted historical codes
    not_in_codes_strict = [c for c in unique_bad_codes if c not in HISTORICAL_CODE_ALLOWLIST]
    bad_rows_code_not_in_countries_strict = len(not_in_codes_strict)

    should_fail = False
    if FAIL_ON_NULL_CODES and bad_null_code > 0:
        should_fail = True
    if bad_rows_code_not_in_countries_strict > 0:
        should_fail = True

    # add policy/evidence fields
    summary["bad_rows_code_not_in_countries_strict"] = bad_rows_code_not_in_countries_strict
    summary["unique_bad_codes_strict_sample"] = not_in_codes_strict[:25]
    summary["historical_code_allowlist"] = sorted(list(HISTORICAL_CODE_ALLOWLIST))
    summary["fail_on_null_codes"] = FAIL_ON_NULL_CODES
    summary["should_fail"] = should_fail

    return bad_rows, summary


# -------------------------------------------------------
# Main
# -------------------------------------------------------

def main(
    countries_csv: str = DEFAULT_COUNTRIES_CSV,
    summer_csv: str = DEFAULT_SUMMER_CSV,
    code_map_csv: str = DEFAULT_CODE_MAP_CSV,
    out_dir: str | None = None,
) -> int:

    root = project_root()
    quarantine_dir = Path(out_dir) if out_dir else (root / "reports" / "quarantine")
    ensure_dir(quarantine_dir)

    countries_df = load_csv(countries_csv)
    summer_df = load_csv(summer_csv)

    code_map = load_code_map(code_map_csv)

    bad_rows, summary = find_bad_code_rows(
        summer_df, countries_df, code_map=code_map
    )

    stamp = ts()
    bad_csv_path = quarantine_dir / f"summer_bad_code_{stamp}.csv"
    bad_json_path = quarantine_dir / f"summer_bad_code_summary_{stamp}.json"

    bad_rows.to_csv(bad_csv_path, index=False)

    with bad_json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # Console output
    print("=== Step 5: Cross-Table Integrity Check ===")
    print(f"Countries file: {countries_csv}")
    print(f"Summer file:    {summer_csv}")
    print(f"Code map file:  {code_map_csv}")
    print(f"Output CSV:     {bad_csv_path.as_posix()}")
    print(f"Output JSON:    {bad_json_path.as_posix()}")
    print("--- Counts ---")
    print(f"Summer rows total:                 {summary['summer_rows_total']}")
    print(f"Countries rows total:              {summary['countries_rows_total']}")
    print(f"Valid country codes count:         {summary['valid_country_codes_count']}")
    print(f"Mapped rows count:                 {summary['mapped_rows_count']}")
    print(f"Bad rows total:                    {summary['bad_rows_total']}")
    print(f"Bad rows with NULL Code:           {summary['bad_rows_null_code']}")
    print(f"Bad rows Code not in Countries:    {summary['bad_rows_code_not_in_countries']}")
    print(f"Unique bad codes count:            {summary['unique_bad_codes_count']}")
    print("--- Strict Policy ---")
    print(f"Historical allowlist:              {summary['historical_code_allowlist']}")
    print(f"Fail on NULL codes:                {summary['fail_on_null_codes']}")
    print(f"Bad not-in-countries (strict):     {summary['bad_rows_code_not_in_countries_strict']}")
    print(f"Should fail (strict):              {summary['should_fail']}")

    return 1 if summary["should_fail"] else 0


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Step 5: Cross-table integrity check")
    ap.add_argument("--countries", default=None, help="Countries CSV")
    ap.add_argument("--summer", default=None, help="Summer CSV")
    ap.add_argument("--code-map", default=None, help="Code map CSV")
    ap.add_argument("--out-dir", default=None, help="Output directory")
    args = ap.parse_args()

    raise SystemExit(
        main(
            countries_csv=args.countries or DEFAULT_COUNTRIES_CSV,
            summer_csv=args.summer or DEFAULT_SUMMER_CSV,
            code_map_csv=args.code_map or DEFAULT_CODE_MAP_CSV,
            out_dir=args.out_dir,
        )
    )