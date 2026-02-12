# src/split_quarantine.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime

import pandas as pd


# -----------------------------
# Config
# -----------------------------
SUMMER_REQUIRED_COLS = [
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

# Countries: only Country + Code are required (Population / GDP can be missing)
COUNTRIES_REQUIRED_COLS = ["Country", "Code"]

VALID_GENDERS = {"Men", "Women"}
VALID_MEDALS = {"Gold", "Silver", "Bronze"}

CODE_REGEX = r"^[A-Z]{3}$"


@dataclass(frozen=True)
class Paths:
    summer_csv: Path
    countries_csv: Path
    code_map_csv: Path
    out_dir: Path


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _read_csv(path: Path, index_col: int | None = 0) -> pd.DataFrame:
    # index_col=0 matches your pipeline convention (and tolerates a saved index)
    return pd.read_csv(path, index_col=index_col)


def _normalize_code_series(s: pd.Series) -> pd.Series:
    # Normalize: strip + uppercase, preserve NA
    s2 = s.astype("string")
    s2 = s2.str.strip()
    s2 = s2.str.upper()
    return s2


def _load_code_map(code_map_csv: Path) -> dict[str, str]:
    """
    code_map.csv expected columns:
      - from_code
      - to_code
    Example row: SRB,SCG
    """
    m = pd.read_csv(code_map_csv)
    # Be tolerant if user named columns differently (common case)
    cols = [c.strip().lower() for c in m.columns]
    m.columns = cols

    if "from_code" in cols and "to_code" in cols:
        from_col, to_col = "from_code", "to_code"
    else:
        # fallback: assume first two columns are from/to
        from_col, to_col = cols[0], cols[1]

    m[from_col] = _normalize_code_series(m[from_col])
    m[to_col] = _normalize_code_series(m[to_col])

    mapping = dict(zip(m[from_col].fillna(""), m[to_col].fillna(""), strict=False))
    # Remove blank keys just in case
    mapping.pop("", None)
    return mapping


def _apply_mapping(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    out["Code"] = _normalize_code_series(out.get("Code"))
    out["Code"] = out["Code"].replace(mapping)  # deterministic mapping
    return out


def _first_reason(series_list: list[pd.Series], labels: list[str]) -> pd.Series:
    """
    Deterministic: returns the FIRST matching reason in priority order.
    If none match, returns <NA>.
    """
    reason = pd.Series(pd.NA, index=series_list[0].index, dtype="string")
    for mask, label in zip(series_list, labels, strict=True):
        reason = reason.mask(mask & reason.isna(), label)
    return reason


# -----------------------------
# Countries split
# -----------------------------
def split_countries(df_countries: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df_countries.copy()

    # Normalize Code (do NOT map Countries; Countries is your reference system)
    df["Code"] = _normalize_code_series(df.get("Code"))

    # Missing required (any required col missing or empty string where applicable)
    missing_required = pd.Series(False, index=df.index)
    for col in COUNTRIES_REQUIRED_COLS:
        if col not in df.columns:
            # if schema is wrong, everything is missing_required
            missing_required |= True
        else:
            if col in ("Country", "Code"):
                missing_required |= df[col].isna() | (df[col].astype("string").str.strip() == "")
            else:
                missing_required |= df[col].isna()

    # Invalid code format
    invalid_code_format = (
        df["Code"].isna()
        | ~df["Code"].astype("string").str.match(CODE_REGEX, na=False)
    )

    # Quarantine reason priority for Countries
    df["quarantine_reason"] = _first_reason(
        [missing_required, invalid_code_format],
        ["missing_required", "invalid_code_format"],
    )

    quarantine = df[df["quarantine_reason"].notna()].copy()
    clean = df[df["quarantine_reason"].isna()].copy()

    return clean, quarantine


# -----------------------------
# Summer split
# -----------------------------
def split_summer(
    df_summer: pd.DataFrame,
    countries_codes: set[str],
    code_to_country: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df_summer.copy()

    # Normalize relevant string fields
    df["Code"] = _normalize_code_series(df.get("Code"))
    if "Gender" in df.columns:
        df["Gender"] = df["Gender"].astype("string").str.strip()
    if "Medal" in df.columns:
        df["Medal"] = df["Medal"].astype("string").str.strip()
    if "Country" in df.columns:
        df["Country"] = df["Country"].astype("string").str.strip()

    # ✅ Deterministic repair: fill missing Country from reference when Code resolves
    # Only fills when:
    # - Country is blank/NA
    # - Code exists
    # - Code is in the countries reference (via map lookup)
    if "Country" in df.columns and "Code" in df.columns:
        missing_country_mask = df["Country"].isna() | (df["Country"].astype("string").str.strip() == "")
        # map returns NA if code not found, which is fine (stays missing)
        df.loc[missing_country_mask, "Country"] = df.loc[missing_country_mask, "Code"].map(code_to_country)

    # Missing required: any required col null/empty
    missing_required = pd.Series(False, index=df.index)
    for col in SUMMER_REQUIRED_COLS:
        if col not in df.columns:
            missing_required |= True
        else:
            if col in ("City", "Sport", "Discipline", "Athlete", "Code", "Gender", "Event", "Medal", "Country"):
                missing_required |= df[col].isna() | (df[col].astype("string").str.strip() == "")
            else:
                # Year
                missing_required |= df[col].isna()

    # Invalid code format (only meaningful if code present; still okay)
    invalid_code_format = (
        df["Code"].isna()
        | ~df["Code"].astype("string").str.match(CODE_REGEX, na=False)
    )

    # Invalid medal
    invalid_medal = (~df["Medal"].isin(list(VALID_MEDALS))) | df["Medal"].isna()

    # Invalid gender
    invalid_gender = (~df["Gender"].isin(list(VALID_GENDERS))) | df["Gender"].isna()

    # Invalid year
    year_num = pd.to_numeric(df.get("Year"), errors="coerce")
    current_year = datetime.now().year
    invalid_year = year_num.isna() | (year_num < 1896) | (year_num > current_year)

    # FK failure after harmonization (countries_codes are already normalized)
    code_not_in_countries = df["Code"].isna() | (~df["Code"].isin(countries_codes))

    # BUT: don’t double-tag missing_required as FK; keep FK as a later reason only
    code_not_in_countries = code_not_in_countries & (~missing_required)

    # Quarantine reason priority for Summer (deterministic)
    df["quarantine_reason"] = _first_reason(
        [
            missing_required,
            invalid_code_format & (~missing_required),
            invalid_medal & (~missing_required),
            invalid_year & (~missing_required),
            invalid_gender & (~missing_required),
            code_not_in_countries,
        ],
        [
            "missing_required",
            "invalid_code_format",
            "invalid_medal",
            "invalid_year",
            "invalid_gender",
            "code_not_in_countries",
        ],
    )

    quarantine = df[df["quarantine_reason"].notna()].copy()
    clean = df[df["quarantine_reason"].isna()].copy()

    return clean, quarantine


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Step 6 — split clean vs quarantine deterministically.")
    parser.add_argument("--summer", required=True, help="Path to SummerSD.csv")
    parser.add_argument("--countries", required=True, help="Path to CountriesSD.csv")
    parser.add_argument("--code-map", required=True, help="Path to code_map.csv (from_code,to_code)")
    parser.add_argument("--out-dir", default="data/processed", help="Output directory (default: data/processed)")
    args = parser.parse_args()

    paths = Paths(
        summer_csv=Path(args.summer),
        countries_csv=Path(args.countries),
        code_map_csv=Path(args.code_map),
        out_dir=Path(args.out_dir),
    )
    _ensure_dir(paths.out_dir)

    # Read
    summer_raw = _read_csv(paths.summer_csv, index_col=0)
    countries_raw = _read_csv(paths.countries_csv, index_col=0)

    # Countries split (reference system)
    countries_clean, countries_quarantine = split_countries(countries_raw)

    # Build normalized countries code set from CLEAN only
    countries_clean_norm = countries_clean.copy()
    countries_clean_norm["Code"] = _normalize_code_series(countries_clean_norm["Code"])
    countries_clean_norm["Country"] = countries_clean_norm["Country"].astype("string").str.strip()

    code_to_country = dict(
        zip(
            countries_clean_norm["Code"].fillna(""),
            countries_clean_norm["Country"].fillna(""),
            strict=False,
        )
    )
    code_to_country.pop("", None)
    code_to_country["BOH"] = "Bohemia"         # historical delegation (1900 era)

    countries_codes = set(code_to_country.keys())

    # Apply mapping to SUMMER (same as Step 5)
    mapping = _load_code_map(paths.code_map_csv)
    summer_mapped = _apply_mapping(summer_raw, mapping)

    # Summer split (post-mapping) + fill Country from Code
    summer_clean, summer_quarantine = split_summer(summer_mapped, countries_codes, code_to_country)

    # Write outputs (keep index to match your existing convention)
    countries_clean.to_csv(paths.out_dir / "countries_clean.csv", index=True)
    countries_quarantine.to_csv(paths.out_dir / "countries_quarantine.csv", index=True)
    summer_clean.to_csv(paths.out_dir / "summer_clean.csv", index=True)
    summer_quarantine.to_csv(paths.out_dir / "summer_quarantine.csv", index=True)

    # Print status summary
    print("Step 6 complete — Clean vs Quarantine split created.")
    print(f"Countries: clean={len(countries_clean)}, quarantine={len(countries_quarantine)}")
    print(f"Summer:    clean={len(summer_clean)}, quarantine={len(summer_quarantine)}")

    if len(summer_quarantine) > 0:
        print("\nTop Summer quarantine reasons:")
        print(summer_quarantine["quarantine_reason"].value_counts(dropna=False).head(10).to_string())

    if len(countries_quarantine) > 0:
        print("\nTop Countries quarantine reasons:")
        print(countries_quarantine["quarantine_reason"].value_counts(dropna=False).head(10).to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())