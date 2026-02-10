# Data Contract — Olympics Dataset (Countries + Summer)

## Dataset
**Name:** Olympics  
**Tables:** `countries.csv`, `summer.csv`  
**Primary Key (Countries):** `Code`  
**Foreign Key (Summer → Countries):** `summer.Code` → `countries.Code`

## Purpose
This contract defines the expected structure, relationships, and validation rules for the Olympics data pipeline.  
It is the source of truth for Great Expectations suites, quarantine logic, and downstream loading.

---

# 1) Table: countries.csv

## Description
One row per country containing demographic/economic metadata.

## Columns & Types (logical)
| Column | Type | Required | Notes |
|---|---|---:|---|
| Country | string | Yes | Country name |
| Code | string | Yes | 3-letter country code |
| Population | integer/float | No | If present must be > 0 |
| GDP_per_Capita | float | No | If present must be > 0; missing is allowed but flagged |

## Rules
### Country
- Required (not null / not empty)
- Unique

### Code
- Required (not null / not empty)
- Unique
- Must match regex: `^[A-Z]{3}$`

### Population
- Optional
- If present: must be > 0

### GDP_per_Capita
- Optional
- If present: must be > 0
- Missing values are **allowed**, but are **flagged** for reporting

## Missing Data Policy (Countries)
- Missing `GDP_per_Capita`: **ALLOW + FLAG**
  - Do not quarantine solely for missing GDP per Capita
  - Capture count/percent missing in run summary (and/or validation artifacts)

---

# 2) Table: summer.csv

## Description
One row per medal-winning athlete entry in the Summer Olympics dataset.

## Columns & Types (logical)
| Column | Type | Required | Notes |
|---|---|---:|---|
| Year | integer | Yes | 1896 → current year |
| Code | string | Yes | Must match regex + exist in Countries |
| Medal | string | Yes | Gold/Silver/Bronze |
| Athlete | string | Yes | Athlete name |
| Gender | string | Yes | Allowed values set (defined below) |

## Rules
### Year
- Required (not null / not empty)
- Must be an integer
- Range: **1896 → current year**

### Code
- Required (not null / not empty)
- Must match regex: `^[A-Z]{3}$`
- Must exist in `countries.Code` (referential integrity)

### Medal
- Required
- Allowed values: **Gold**, **Silver**, **Bronze**

### Athlete
- Required (not null / not empty)

### Gender
- Required
  -  `Men`, `Women`

> NOTE: Validation must match the dataset’s actual values exactly.

---

# Relationships & Integrity

## Referential Integrity
**Rule:** Every `summer.Code` must exist in `countries.Code`.

**Violation handling:** Quarantine invalid Summer rows
