from __future__ import annotations

import importlib.metadata
import json
import os
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings
from app.db.models import ConfigVersion, FeatureWeightVersion
from app.services.history import file_sha256


SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9._-]{8,100}$")
HISTORY_DATE_PATTERN = re.compile(r"馬の競走成績_with_feat_(\d{8}).*\.xlsx$")
WEIGHT_PATTERN = re.compile(r"best_feature_weights_(\d{8})\.py$")

PREDICTION_CODE_FILES = (
    "1_keibayosou_best_import_roi_runner.py",
    "1_keibayosou_config.py",
    "1_keibayosou_course_style.py",
    "1_keibayosou_features.py",
    "1_keibayosou_loaders.py",
    "1_keibayosou_penalties.py",
    "1_keibayosou_pipeline.py",
    "1_keibayosou_utils.py",
)
PREDICTION_MASTER_FILES = (
    "race_levels.xlsx",
    "場所_馬場_タイム.xlsx",
    "racedata_results.xlsx",
)


class PredictionWorkspaceError(RuntimeError):
    """Raised when an immutable prediction execution workspace cannot be prepared."""


@dataclass(frozen=True)
class PredictionWorkspace:
    root: Path
    script_path: Path
    source_input_path: Path
    odds_path: Path
    expected_output_path: Path
    manifest_path: Path
    manifest: dict[str, Any]


def prepare_prediction_workspace(
    settings: Settings,
    *,
    run_id: str,
    race_date: date,
    source_workbook: Path,
) -> PredictionWorkspace:
    """Create one isolated, immutable-compatible workspace for the legacy two-stage CLI."""

    if not SAFE_RUN_ID.fullmatch(run_id):
        raise PredictionWorkspaceError("unsafe prediction run_id")
    source_workbook = source_workbook.resolve(strict=True)
    if not source_workbook.is_file():
        raise PredictionWorkspaceError("prediction source workbook must be a regular file")

    run_dir = settings.exports_dir / "runs" / run_id
    workspace_root = run_dir / "legacy_workspace"
    if workspace_root.exists() and any(workspace_root.iterdir()):
        raise PredictionWorkspaceError("prediction workspace already exists; create a new run_id")

    input_dir = workspace_root / "data" / "input"
    odds_dir = workspace_root / "data" / "ozzu_csv"
    master_dir = workspace_root / "data" / "master"
    output_dir = workspace_root / "data" / "output"
    weights_dir = workspace_root / "yosou_py"
    for directory in (input_dir, odds_dir, master_dir, output_dir, weights_dir):
        directory.mkdir(parents=True, exist_ok=True)

    code_entries: list[dict[str, Any]] = []
    for file_name in PREDICTION_CODE_FILES:
        source = (settings.legacy_root / file_name).resolve(strict=True)
        destination = workspace_root / file_name
        _copy_immutable(source, destination)
        code_entries.append(_file_entry(source, destination, workspace_root))

    yyyymmdd = race_date.strftime("%Y%m%d")
    input_destination = input_dir / f"馬の競走成績_{yyyymmdd}.xlsx"
    _copy_immutable(source_workbook, input_destination)

    source_odds = (settings.odds_input_dir / f"OZZU_{yyyymmdd}.csv").resolve(strict=True)
    odds_destination = odds_dir / source_odds.name
    _copy_immutable(source_odds, odds_destination)

    master_entries: list[dict[str, Any]] = []
    for file_name in PREDICTION_MASTER_FILES:
        source = (settings.data_root / "master" / file_name).resolve(strict=True)
        destination = master_dir / file_name
        _copy_immutable(source, destination)
        master_entries.append(_file_entry(source, destination, workspace_root))

    selected_weight = _latest_weight_file(
        settings.legacy_root / "yosou_py",
        cutoff_exclusive=yyyymmdd,
    )
    weight_entry: dict[str, Any] | None = None
    if selected_weight is not None:
        weight_destination = weights_dir / selected_weight.name
        _copy_immutable(selected_weight, weight_destination)
        weight_entry = _file_entry(selected_weight, weight_destination, workspace_root)

    history_entries: list[dict[str, Any]] = []
    if settings.legacy_output_dir.exists():
        for source in sorted(settings.legacy_output_dir.glob("馬の競走成績_with_feat_*.xlsx")):
            match = HISTORY_DATE_PATTERN.fullmatch(source.name)
            if match is None or "_with_dl" in source.stem:
                continue
            # 対象日以降の予想を履歴へ混ぜず、過去日検証で未来情報を使わない。
            if match.group(1) >= yyyymmdd:
                continue
            destination = output_dir / source.name
            link_mode = _link_or_copy_history(source.resolve(strict=True), destination)
            entry = _file_entry(source.resolve(), destination, workspace_root)
            entry["materialization"] = link_mode
            history_entries.append(entry)

    dependencies = _runtime_versions()
    code_bundle_sha256 = _canonical_hash(
        [{"name": item["name"], "sha256": item["sha256"]} for item in code_entries]
    )
    input_snapshot_sha256 = _canonical_hash(
        {
            "source_input": file_sha256(input_destination),
            "odds": file_sha256(odds_destination),
            "masters": [item["sha256"] for item in master_entries],
            "weight": weight_entry["sha256"] if weight_entry else None,
            "history": [item["sha256"] for item in history_entries],
            "code_bundle": code_bundle_sha256,
        }
    )
    expected_output = output_dir / f"馬の競走成績_with_feat_{yyyymmdd}.xlsx"
    manifest = {
        "schema_version": "prediction_workspace_v1",
        "run_id": run_id,
        "race_date": race_date.isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "workspace_root": str(workspace_root.resolve()),
        "source_input": _file_entry(source_workbook, input_destination, workspace_root),
        "odds": _file_entry(source_odds, odds_destination, workspace_root),
        "masters": master_entries,
        "weight": weight_entry,
        "weight_cutoff_exclusive": yyyymmdd,
        "code_files": code_entries,
        "code_bundle_sha256": code_bundle_sha256,
        "history_cutoff_exclusive": yyyymmdd,
        "history_files": history_entries,
        "history_count": len(history_entries),
        "dependencies": dependencies,
        "input_snapshot_sha256": input_snapshot_sha256,
        "expected_output": str(expected_output.resolve()),
    }
    manifest_path = run_dir / "prediction_workspace_manifest_v1.json"
    _write_json_atomic(manifest_path, manifest)
    return PredictionWorkspace(
        root=workspace_root,
        script_path=workspace_root / "1_keibayosou_best_import_roi_runner.py",
        source_input_path=input_destination,
        odds_path=odds_destination,
        expected_output_path=expected_output,
        manifest_path=manifest_path,
        manifest=manifest,
    )


def register_prediction_versions(
    db: Session,
    *,
    manifest: dict[str, Any],
    environment: str,
) -> tuple[ConfigVersion, FeatureWeightVersion]:
    """Register reproducible runtime and weight versions from a workspace manifest."""

    config_payload = {
        "schema_version": manifest["schema_version"],
        "dependencies": manifest["dependencies"],
        "code_bundle_sha256": manifest["code_bundle_sha256"],
        "code_files": [
            {"name": item["name"], "sha256": item["sha256"]}
            for item in manifest["code_files"]
        ],
        "masters": [
            {"name": item["name"], "sha256": item["sha256"]}
            for item in manifest["masters"]
        ],
    }
    config_hash = _canonical_hash(config_payload)
    config_version = db.scalar(
        select(ConfigVersion).where(ConfigVersion.sha256 == config_hash)
    )
    if config_version is None:
        config_version = ConfigVersion(
            config_key="legacy_prediction_runtime",
            version_name=f"runtime-{config_hash[:12]}",
            environment=_database_environment(environment),
            config_json=config_payload,
            sha256=config_hash,
            is_active=False,
            created_by="prediction_workspace",
            note="Immutable runtime snapshot derived from prediction_workspace_v1",
        )
        db.add(config_version)
        db.flush()

    weight = manifest.get("weight")
    weight_payload = (
        {
            "mode": "external_python_module",
            "source_file": weight["name"],
            "source_sha256": weight["sha256"],
        }
        if weight
        else {
            "mode": "built_in_config",
            "source_file": "1_keibayosou_config.py",
            "source_sha256": next(
                item["sha256"]
                for item in manifest["code_files"]
                if item["name"] == "1_keibayosou_config.py"
            ),
        }
    )
    weight_hash = _canonical_hash(weight_payload)
    weight_version = db.scalar(
        select(FeatureWeightVersion).where(FeatureWeightVersion.sha256 == weight_hash)
    )
    if weight_version is None:
        source_name = weight_payload["source_file"]
        weight_version = FeatureWeightVersion(
            weight_key="legacy_prediction_feature_weights",
            version_name=f"{Path(source_name).stem}-{weight_hash[:12]}",
            weights_json=weight_payload,
            source_file_path=weight.get("source_path") if weight else None,
            sha256=weight_hash,
            is_active=False,
            created_by="prediction_workspace",
            note="Exact source module is preserved as an immutable run artifact",
        )
        db.add(weight_version)
        db.flush()
    return config_version, weight_version


def _copy_immutable(source: Path, destination: Path) -> None:
    source = source.resolve(strict=True)
    if not source.is_file():
        raise PredictionWorkspaceError(f"required prediction file is not regular: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_hash = file_sha256(source)
    if destination.exists():
        if file_sha256(destination) != source_hash:
            raise PredictionWorkspaceError(
                "prediction workspace file already exists with different bytes"
            )
        return
    temporary = destination.with_name(f".{destination.name}.tmp")
    shutil.copy2(source, temporary)
    if file_sha256(temporary) != source_hash:
        temporary.unlink(missing_ok=True)
        raise PredictionWorkspaceError("prediction workspace copy hash mismatch")
    os.replace(temporary, destination)


def _link_or_copy_history(source: Path, destination: Path) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise PredictionWorkspaceError("history destination already exists")
    try:
        if os.name == "nt":
            os.link(source, destination)
            return "hardlink"
        os.symlink(source, destination)
        return "symlink"
    except OSError:
        try:
            os.link(source, destination)
            return "hardlink"
        except OSError:
            shutil.copy2(source, destination)
            return "copy"


def _latest_weight_file(folder: Path, *, cutoff_exclusive: str) -> Path | None:
    if not folder.is_dir():
        return None
    candidates: list[tuple[str, Path]] = []
    for path in folder.iterdir():
        match = WEIGHT_PATTERN.fullmatch(path.name)
        # 対象日に学習した重みも当日結果を含む可能性があるため、前日以前に限る。
        if match and match.group(1) < cutoff_exclusive and path.is_file():
            candidates.append((match.group(1), path.resolve()))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def _file_entry(source: Path, materialized: Path, root: Path) -> dict[str, Any]:
    return {
        "name": materialized.name,
        "relative_path": materialized.relative_to(root).as_posix(),
        "source_path": str(source.resolve()),
        "sha256": file_sha256(materialized),
        "size_bytes": materialized.stat().st_size,
    }


def _runtime_versions() -> dict[str, str]:
    packages = ("numpy", "openpyxl", "pandas", "requests", "torch")
    versions = {"python": sys.version.split()[0]}
    for package in packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "missing"
    return versions


def _database_environment(value: str) -> str:
    return {
        "production": "production",
        "research": "research",
        "development": "development",
        "test": "test",
        "local": "development",
    }.get(value.lower(), "development")


def _canonical_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    os.replace(temporary, path)
