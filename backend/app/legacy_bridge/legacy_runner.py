from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app.core.config import get_settings
from app.legacy_bridge.excel_importer import find_race_workbook
from app.legacy_bridge.odds_importer import find_odds_csv
from app.legacy_bridge.result_importer import find_result_workbook


@dataclass(frozen=True)
class LegacyScriptSpec:
    relative_path: Path | None
    description: str
    output_kinds: tuple[str, ...]


class LegacyRunnerError(RuntimeError):
    """Raised when a legacy collection script fails."""


LEGACY_COLLECTION_SCRIPTS: dict[str, LegacyScriptSpec] = {
    "collection.race_info": LegacyScriptSpec(
        relative_path=Path("etc_py/01_jizen_syuusyuu_race_info_classfix_20251212.py"),
        description="出馬表・レース情報取得",
        output_kinds=("race_workbook",),
    ),
    "collection.past_performances": LegacyScriptSpec(
        relative_path=Path("etc_py/01_jizen_syuusyuu_race_info_classfix_20251212.py"),
        description="過去走情報取得",
        output_kinds=("race_workbook",),
    ),
    "collection.odds": LegacyScriptSpec(
        relative_path=Path("etc_py/1_02_scrape_jra_odds_2.py"),
        description="JRAオッズ取得",
        output_kinds=("odds_csv",),
    ),
    "collection.training": LegacyScriptSpec(
        relative_path=None,
        description="調教データ取得",
        output_kinds=(),
    ),
    "collection.results": LegacyScriptSpec(
        relative_path=Path("etc_py/10_kekka_scraper_20260102.py"),
        description="レース結果取得",
        output_kinds=("result_workbook", "result_master"),
    ),
}


def run_legacy_collection_job(
    job_type: str,
    job_id: str,
    race_date: date | None = None,
    race_id: str | None = None,
    params: dict[str, Any] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Run or dry-run a Phase 4 legacy collection script and write run artifacts."""

    if job_type not in LEGACY_COLLECTION_SCRIPTS:
        raise LegacyRunnerError(f"unsupported legacy job_type: {job_type}")

    settings = get_settings()
    spec = LEGACY_COLLECTION_SCRIPTS[job_type]
    mode = settings.legacy_runner_mode.lower()
    if mode not in {"dry_run", "execute"}:
        raise LegacyRunnerError(f"unsupported legacy_runner_mode: {settings.legacy_runner_mode}")

    run_dir = settings.exports_dir / "runs" / job_id
    log_dir = settings.logs_dir / "runs" / job_id
    run_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    script_path = _resolve_script_path(settings.legacy_root, spec.relative_path)
    input_manifest = {
        "run_id": job_id,
        "job_type": job_type,
        "race_date": race_date.isoformat() if race_date else None,
        "race_id": race_id,
        "force": force,
        "params": _jsonable(params or {}),
        "mode": mode,
        "script": str(script_path) if script_path else None,
        "script_exists": bool(script_path and script_path.exists()),
        "data_contract": {
            "raw": str(settings.excel_input_dir),
            "staging": str(settings.staging_dir),
            "exports": str(settings.exports_dir),
            "logs": str(settings.logs_dir),
        },
        "created_at": _utc_now_text(),
    }
    _write_json(run_dir / "input_manifest_v1.json", input_manifest)

    started = time.perf_counter()
    stdout_path = log_dir / "stdout.log"
    stderr_path = log_dir / "stderr.log"
    if mode == "execute":
        return_code = _execute_script(
            script_path=script_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            job_id=job_id,
            race_date=race_date,
            race_id=race_id,
            params=params or {},
        )
    else:
        return_code = _write_dry_run_logs(stdout_path, stderr_path, spec, script_path)

    duration_seconds = round(time.perf_counter() - started, 3)
    outputs = _discover_outputs(spec.output_kinds, race_date)
    output_manifest = {
        "run_id": job_id,
        "job_type": job_type,
        "mode": mode,
        "description": spec.description,
        "return_code": return_code,
        "duration_seconds": duration_seconds,
        "stdout_log": str(stdout_path),
        "stderr_log": str(stderr_path),
        "outputs": outputs,
        "finished_at": _utc_now_text(),
    }
    _write_json(run_dir / "output_manifest_v1.json", output_manifest)

    if return_code != 0:
        raise LegacyRunnerError(
            f"legacy script failed: job_type={job_type}, return_code={return_code}, stderr={stderr_path}"
        )
    return output_manifest


def _resolve_script_path(legacy_root: Path, relative_path: Path | None) -> Path | None:
    if relative_path is None:
        return None
    return (legacy_root / relative_path).resolve()


def _execute_script(
    script_path: Path | None,
    stdout_path: Path,
    stderr_path: Path,
    job_id: str,
    race_date: date | None,
    race_id: str | None,
    params: dict[str, Any],
) -> int:
    if script_path is None:
        raise LegacyRunnerError("legacy script is not configured for this job_type")
    if not script_path.exists():
        raise FileNotFoundError(f"legacy script not found: {script_path}")

    settings = get_settings()
    env = os.environ.copy()
    env.update(
        {
            "KEIBA_RUN_ID": job_id,
            "KEIBA_RACE_DATE": race_date.isoformat() if race_date else "",
            "KEIBA_RACE_ID": race_id or "",
            "KEIBA_DATA_RAW_DIR": str(settings.excel_input_dir),
            "KEIBA_DATA_STAGING_DIR": str(settings.staging_dir),
            "KEIBA_DATA_EXPORTS_DIR": str(settings.exports_dir),
            "KEIBA_DATA_LOGS_DIR": str(settings.logs_dir),
        }
    )
    extra_args = params.get("args", [])
    if not isinstance(extra_args, list):
        raise LegacyRunnerError("params.args must be a list when provided")

    workspace_dir = Path("/workspace") if Path("/workspace").exists() else Path.cwd()
    with stdout_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open(
        "w",
        encoding="utf-8",
    ) as stderr_file:
        completed = subprocess.run(
            [sys.executable, str(script_path), *[str(item) for item in extra_args]],
            cwd=workspace_dir,
            env=env,
            stdout=stdout_file,
            stderr=stderr_file,
            timeout=settings.legacy_timeout_seconds,
            check=False,
        )
    return completed.returncode


def _write_dry_run_logs(
    stdout_path: Path,
    stderr_path: Path,
    spec: LegacyScriptSpec,
    script_path: Path | None,
) -> int:
    stdout_path.write_text(
        "\n".join(
            [
                "legacy runner dry-run",
                f"description={spec.description}",
                f"script={script_path or '-'}",
                f"script_exists={bool(script_path and script_path.exists())}",
                "external scraping was not executed",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    stderr_path.write_text("", encoding="utf-8")
    return 0


def _discover_outputs(output_kinds: tuple[str, ...], race_date: date | None) -> list[dict[str, Any]]:
    settings = get_settings()
    outputs: list[dict[str, Any]] = []
    for output_kind in output_kinds:
        path: Path | None = None
        if output_kind == "race_workbook":
            path = find_race_workbook(race_date=race_date)
        elif output_kind == "odds_csv":
            path = find_odds_csv(race_date=race_date)
        elif output_kind == "result_workbook":
            path = find_result_workbook(race_date=race_date)
        elif output_kind == "result_master":
            candidate = settings.data_root / "master" / "racedata_results.xlsx"
            path = candidate if candidate.exists() else None

        outputs.append(
            {
                "kind": output_kind,
                "path": str(path) if path else None,
                "exists": bool(path and path.exists()),
            }
        )
    return outputs


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _jsonable(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload, default=str, ensure_ascii=False))


def _utc_now_text() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"
