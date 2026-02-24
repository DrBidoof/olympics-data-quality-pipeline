# Olympic Automated Data Quality & Validation Pipeline

**Python • pandas • Great Expectations • PostgreSQL (Neon)**

---

## Overview

This project implements a **deterministic, auditable data quality pipeline** for Olympic datasets (Countries and Summer).

The pipeline validates schema and business rules using Great Expectations, harmonizes country codes, enforces cross-table integrity, deterministically separates clean and quarantine records, and loads validated data into PostgreSQL (Neon).

Each execution produces reproducible validation evidence, execution metrics, and an auditable run history stored in the database. The system is built using production-grade data engineering principles including deterministic processing, strict-mode failure policy, integrity enforcement, and auditability.

---

## Architecture

```
Raw CSV
   ↓
Great Expectations (Schema + Rules)
   ↓
Code Harmonization (Mapping Layer)
   ↓
Cross-table Integrity Check
   ↓
Deterministic Split (Clean / Quarantine)
   ↓
Postgres Load (Neon)
   ↓
SQL Verification + FK Enforcement
   ↓
Run Metadata + Evidence (validation_runs)
```

---

## Key Features

- Deterministic data processing
- Schema validation with Great Expectations
- Regex and range rule enforcement
- Country code harmonization via mapping layer
- Cross-table referential integrity enforcement
- Strict failure mode (fail-fast)
- Deterministic clean vs quarantine split
- PostgreSQL (Neon) integration
- Foreign key + unique constraints enforced at DB level
- Auditable run history (`validation_runs`)
- Reproducible validation evidence per run
- Orchestrated pipeline runner

---

## Tech Stack

- Python 3.x
- pandas
- Great Expectations
- PostgreSQL (Neon)
- psycopg (v3)
- JSON / CSV
- Deterministic ETL design

---

## Folder Structure

```
project-root/
│
├── src/
│   ├── pipeline.py
│   ├── validate_countries.py
│   ├── validate_summer.py
│   ├── check_integrity.py
│   ├── split_quarantine.py
│   ├── load_to_postgres.py
│   ├── write_run_metadata.py
│
├── data/
│   ├── sample/
│   ├── reference/
│   └── processed/
│
├── reports/
│   ├── validations/
│   ├── quarantine/
│   └── runs/
│
├── gx/
└── README.md
```

---

## How to Run

### 1. Install dependencies

```bash
pip install pandas great_expectations psycopg[binary]
```

### 2. Set database connection

```bash
export DATABASE_URL=postgresql://user:password@host/db
```

### 3. Run the pipeline

```bash
python src/pipeline.py --strict
```

---

## Pipeline Steps

| Step | Description |
|------|-------------|
| 01 | Validate Countries (Great Expectations) |
| 02 | Validate Summer (Great Expectations) |
| 03 | Cross-table integrity + harmonization |
| 04 | Deterministic clean vs quarantine split |
| 05 | Load validated data into PostgreSQL |
| 06 | Write run metadata into `validation_runs` |

---

## Database Schema

### Clean Tables

- `countries_clean`
- `summer_clean`

### Quarantine Tables

- `countries_quarantine`
- `summer_quarantine`

### Audit Table

- `validation_runs`

The audit table stores:

- run_id
- strict_mode
- GX validation results
- integrity metrics
- quarantine counts
- full run summary JSON
- execution timestamp

---

## Strict Mode

When `--strict` is enabled, the pipeline stops if:

- Great Expectations validation fails
- Non-allowlisted country codes remain
- Integrity check returns `should_fail = True`

This ensures **invalid data never enters clean tables**.

---

## Historical Code Policy

Some Olympic country codes are historical (e.g., `BOH`).  
These are handled via:

- Deterministic mapping (`code_map.csv`)
- Historical allowlist
- Integrity-aware strict mode

---

## SQL Verification Queries

Check for foreign key violations:

```sql
SELECT s.code
FROM summer_clean s
LEFT JOIN countries_clean c
  ON c.code = s.code
WHERE c.code IS NULL;
```

Expected result: **0 rows**

---

## Deterministic Guarantee

The pipeline guarantees deterministic behavior:

- Same input → same output
- Same quarantine split
- Same row counts
- Same validation outcome
- Fully reproducible run evidence

---

## Example Run Result

```
overall_ok: True
bad_rows_total: 4
bad_rows_null_code: 4
FK violations: 0
```

---

## Engineering Concepts Demonstrated

- Data quality engineering
- Schema validation
- Referential integrity
- Deterministic ETL
- Auditability
- Pipeline orchestration
- Error isolation (quarantine)
- Observability
- Reproducible pipelines

---

## Future Improvements

- Pytest test suite
- Structured JSON logging
- Docker containerization
- GitHub Actions CI/CD
- COPY-based bulk load optimization
- Data drift monitoring
- Modular CLI interface

---

## Author

**Alwyn Lynch**  
Software Engineering • Health Informatics • Data Engineering

---
