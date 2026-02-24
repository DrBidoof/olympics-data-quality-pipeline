# src/pipeline.py
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from subprocess import CompletedProcess, run
from typing import Any, Optional


# ----------------------------
# Time / JSON helpers
# ----------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def newest_file_any(dirs: list[Path], pattern: str = "*.json") -> Optional[Path]:
    candidates: list[Path] = []
    for d in dirs:
        if d.exists():
            candidates.extend(list(d.glob(pattern)))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def format_secs(s: float) -> str:
    if s < 60:
        return f"{s:.1f}s"
    return f"{int(s // 60)}m{int(s % 60):02d}s"


# ----------------------------
# Results model
# ----------------------------

@dataclass
class StepResult:
    name: str
    ok: bool
    returncode: int
    started_at: str
    finished_at: str
    duration_seconds: float
    command: list[str]
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    evidence_json: Optional[str] = None
    parsed_metrics: Optional[dict[str, Any]] = None
    error: Optional[str] = None


# ----------------------------
# Evidence parsers (best-effort)
# ----------------------------

def parse_gx_result(evidence_path: Path) -> dict[str, Any]:
    """
    Great Expectations validation result JSON often has:
      - "success": bool
      - "statistics": { evaluated_expectations, successful_expectations, unsuccessful_expectations }
    We extract a compact summary.
    """
    data = read_json(evidence_path)
    stats = data.get("statistics", {}) if isinstance(data, dict) else {}
    return {
        "success": data.get("success") if isinstance(data, dict) else None,
        "evaluated_expectations": stats.get("evaluated_expectations"),
        "successful_expectations": stats.get("successful_expectations"),
        "unsuccessful_expectations": stats.get("unsuccessful_expectations"),
    }


def parse_integrity_summary(evidence_path: Path) -> dict[str, Any]:
    """
    Extract a compact summary from the integrity JSON.
    We also read 'should_fail' if your check_integrity.py emits it (recommended).
    """
    data = read_json(evidence_path)
    if not isinstance(data, dict):
        return {"parse_error": "Integrity evidence is not a JSON object."}

    keys = [
        "run_timestamp",
        "summer_rows_total",
        "countries_rows_total",
        "valid_country_codes_count",
        "mapped_rows_count",
        "bad_rows_total",
        "bad_rows_null_code",
        "bad_rows_code_not_in_countries",
        "bad_rows_code_not_in_countries_strict",
        "unique_bad_codes_count",
        "unique_bad_codes_sample",
        "unique_bad_codes_strict_sample",
        "historical_code_allowlist",
        "fail_on_null_codes",
        "should_fail",
    ]
    out = {k: data.get(k) for k in keys if k in data}

    # Backwards compatibility: if 'should_fail' isn't present,
    # fall back to "bad_rows_total > 0".
    if "should_fail" not in out:
        out["should_fail"] = (data.get("bad_rows_total", 0) or 0) > 0

    return out


# ----------------------------
# Step runner
# ----------------------------

def run_step(
    *,
    name: str,
    cmd: list[str],
    cwd: Path,
    env: dict[str, str],
    logs_dir: Path,
    evidence_dir_hints: Optional[list[Path]] = None,
    evidence_pattern: str = "*.json",
    parse_evidence_fn=None,
) -> StepResult:
    started = now_utc_iso()
    t0 = time.time()

    ensure_dir(logs_dir)
    stdout_path = logs_dir / f"{name}.stdout.log"
    stderr_path = logs_dir / f"{name}.stderr.log"

    try:
        proc: CompletedProcess = run(
            cmd,
            cwd=str(cwd),
            env=env,
            text=True,
            capture_output=True,
        )

        stdout_path.write_text(proc.stdout or "", encoding="utf-8")
        stderr_path.write_text(proc.stderr or "", encoding="utf-8")

        ok = proc.returncode == 0
        evidence_path: Optional[Path] = None
        metrics: Optional[dict[str, Any]] = None

        if evidence_dir_hints:
            evidence_path = newest_file_any(evidence_dir_hints, evidence_pattern)
            if evidence_path and parse_evidence_fn:
                try:
                    metrics = parse_evidence_fn(evidence_path)
                except Exception as e:
                    metrics = {"parse_error": str(e), "evidence_file": str(evidence_path)}

        finished = now_utc_iso()
        dt = time.time() - t0

        return StepResult(
            name=name,
            ok=ok,
            returncode=proc.returncode,
            started_at=started,
            finished_at=finished,
            duration_seconds=dt,
            command=cmd,
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            evidence_json=str(evidence_path) if evidence_path else None,
            parsed_metrics=metrics,
            error=None if ok else f"{name} failed (return code {proc.returncode}). See logs.",
        )

    except Exception as e:
        finished = now_utc_iso()
        dt = time.time() - t0
        return StepResult(
            name=name,
            ok=False,
            returncode=999,
            started_at=started,
            finished_at=finished,
            duration_seconds=dt,
            command=cmd,
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            evidence_json=None,
            parsed_metrics=None,
            error=f"Exception while running {name}: {e}",
        )


# ----------------------------
# Main pipeline
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Olympic Data Quality Pipeline Runner (Step 8)")
    ap.add_argument("--strict", action="store_true", help="Stop pipeline on first failure.")
    ap.add_argument("--python", default=sys.executable, help="Python executable to run step scripts.")
    ap.add_argument("--skip-load", action="store_true", help="Skip Postgres load step.")
    ap.add_argument("--truncate", action="store_true", help="Ask loader to truncate before load (via env var).")
    ap.add_argument(
        "--project-root",
        default=None,
        help="Optional explicit project root path. Defaults to repo root based on this file location.",
    )
    args = ap.parse_args()

    # ✅ EARLY FAILURE PROTECTION (Phase 2 hardening)
    if not args.skip_load:
        if not os.environ.get("DATABASE_URL"):
            print("ERROR: DATABASE_URL environment variable not set.")
            return 2

    # Your structure: repo_root/src/pipeline.py -> repo_root is parents[1]
    this_file = Path(__file__).resolve()
    default_root = this_file.parents[1]
    root = Path(args.project_root).resolve() if args.project_root else default_root

    src_dir = root / "src"
    processed_dir = root / "data" / "processed"
    reports_dir = root / "reports"

    # ✅ Inputs used by step scripts (relative to project root)
    countries_csv = str(root / "data" / "sample" / "countries_sample.csv")
    summer_csv = str(root / "data" / "sample" / "summer_sample.csv")
    code_map_csv = str(root / "data" / "reference" / "code_map.csv")

    # Great Expectations evidence can be in EITHER:
    #   repo_root/gx/uncommitted/validations
    #   repo_root/src/gx/uncommitted/validations
    gx_validations_hints = [
        root / "gx" / "uncommitted" / "validations",
        root / "src" / "gx" / "uncommitted" / "validations",
        root / "reports" / "validations",  # <-- where your scripts currently write
    ]

    # Integrity evidence: check_integrity.py writes into reports/quarantine by default
    integrity_hints = [
        root / "reports" / "quarantine",
        root / "reports",
        root / "src" / "reports",
    ]

    # Run artifacts (single folder per run_id)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    run_dir = reports_dir / "runs" / run_id
    logs_dir = run_dir / "logs"
    ensure_dir(run_dir)

    # Environment passed to subprocess steps
    env = os.environ.copy()
    env["RUN_ID"] = run_id
    env["PROJECT_ROOT"] = str(root)
    env["RUN_DIR"] = str(run_dir)
    env["PROCESSED_DIR"] = str(processed_dir)
    if args.truncate:
        env["TRUNCATE_BEFORE_LOAD"] = "1"

    # Commands (your actual filenames)
    steps: list[tuple[str, list[str], Optional[list[Path]], Any]] = [
        (
            "01_validate_countries",
            [args.python, str(src_dir / "validate_countries.py")],
            gx_validations_hints,
            parse_gx_result,
        ),
        (
            "02_validate_summer",
            [args.python, str(src_dir / "validate_summer.py")],
            gx_validations_hints,
            parse_gx_result,
        ),
        (
            "03_check_integrity",
            [
                args.python,
                str(src_dir / "check_integrity.py"),
                "--countries", countries_csv,
                "--summer", summer_csv,
                "--code-map", code_map_csv,
            ],
            integrity_hints,
            parse_integrity_summary,
        ),
        (
            "04_split_quarantine",
            [
                args.python,
                str(src_dir / "split_quarantine.py"),
                "--countries", countries_csv,
                "--summer", summer_csv,
                "--code-map", code_map_csv,
                # Optional: if you want split outputs somewhere else:
                # "--out-dir", str(processed_dir),
            ],
            None,
            None,
        ),
    ]

    if not args.skip_load:
        # Recommended design: forward DATABASE_URL as CLI arg to loader
        steps.append(
            (
                "05_load_to_postgres",
                [
                    args.python,
                    str(src_dir / "load_to_postgres.py"),
                    "--database-url",
                    os.environ.get("DATABASE_URL", ""),
                ],
                None,
                None,
            )
        )

    pipeline_started = now_utc_iso()
    results: list[StepResult] = []
    overall_ok = True
    stop_reason: Optional[str] = None

    for name, cmd, evidence_hints, parser_fn in steps:
        r = run_step(
            name=name,
            cmd=cmd,
            cwd=root,
            env=env,
            logs_dir=logs_dir,
            evidence_dir_hints=evidence_hints,
            parse_evidence_fn=parser_fn,
        )

        # Strict-mode extra logic (beyond return code):
        # - GX evidence might report success=false even if script exits 0
        if name in ("01_validate_countries", "02_validate_summer"):
            if r.parsed_metrics and r.parsed_metrics.get("success") is False:
                r.ok = False
                r.error = r.error or "GX validation success=false"

        # - Integrity: prefer 'should_fail' emitted by check_integrity.py
        if name == "03_check_integrity":
            if args.strict and r.parsed_metrics:
                if r.parsed_metrics.get("should_fail") is True:
                    r.ok = False
                    r.error = r.error or "Integrity policy says should_fail=true."

        results.append(r)

        if not r.ok:
            overall_ok = False
            if args.strict:
                stop_reason = r.error or f"{name} failed"
                break

    pipeline_finished = now_utc_iso()

    # Best-effort counts from processed CSVs (fast, no pandas)
    def count_csv_rows(path: Path) -> Optional[int]:
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as f:
            n = sum(1 for _ in f) - 1  # subtract header
        return max(n, 0)

    processed_files = {
        "countries_clean": processed_dir / "countries_clean.csv",
        "countries_quarantine": processed_dir / "countries_quarantine.csv",
        "summer_clean": processed_dir / "summer_clean.csv",
        "summer_quarantine": processed_dir / "summer_quarantine.csv",
    }

    summary = {
        "run_id": run_id,
        "started_at": pipeline_started,
        "finished_at": pipeline_finished,
        "project_root": str(root),
        "strict_mode": bool(args.strict),
        "skip_load": bool(args.skip_load),
        "overall_ok": overall_ok,
        "stop_reason": stop_reason,
        "steps": [asdict(x) for x in results],
        "processed_row_counts": {k: count_csv_rows(v) for k, v in processed_files.items()},
        "artifacts": {
            "run_dir": str(run_dir),
            "logs_dir": str(logs_dir),
            "processed_dir": str(processed_dir),
        },
        "inputs": {
            "countries_csv": countries_csv,
            "summer_csv": summer_csv,
            "code_map_csv": code_map_csv,
        },
        "evidence_search_paths": {
            "gx_validation_dirs": [str(p) for p in gx_validations_hints],
            "integrity_dirs": [str(p) for p in integrity_hints],
        },
        "env_hints": {
            "RUN_ID": run_id,
            "RUN_DIR": str(run_dir),
            "PROCESSED_DIR": str(processed_dir),
        },
    }

    summary_path = run_dir / "run_summary.json"
    write_json(summary_path, summary)

    # Console output
    print("\n================ PIPELINE RUN SUMMARY ================")
    print(f"run_id:     {run_id}")
    print(f"strict:     {args.strict}")
    print(f"overall_ok: {overall_ok}")
    if stop_reason:
        print(f"stopped:    {stop_reason}")
    print(f"summary:    {summary_path}")
    print("------------------------------------------------------")
    for s in results:
        status = "OK" if s.ok else "FAIL"
        print(f"{status:4}  {s.name:22}  {format_secs(s.duration_seconds):>6}  rc={s.returncode}")
        if s.evidence_json:
            print(f"      evidence: {s.evidence_json}")
        if s.parsed_metrics:
            print(f"      metrics:  {s.parsed_metrics}")
        if s.error and not s.ok:
            print(f"      error:    {s.error}")
    print("======================================================\n")

    return 0 if overall_ok else 2


if __name__ == "__main__":
    raise SystemExit(main())