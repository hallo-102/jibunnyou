from __future__ import annotations

import importlib.util
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import load_settings


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str
    required: bool = True

    def to_dict(self) -> dict:
        return asdict(self)


def _module_check(name: str, *, required: bool = True) -> Check:
    ok = importlib.util.find_spec(name) is not None
    return Check(name=f"module:{name}", ok=ok, detail="installed" if ok else "not installed", required=required)


def run_doctor(settings_path: str | Path | None = None) -> dict:
    settings = load_settings(settings_path)
    root = settings.project_root
    app = settings.section("app")
    pred = settings.section("prediction")

    checks: list[Check] = []
    checks.append(Check("project_root", root.exists(), str(root)))
    checks.append(Check("settings", True, str(settings.path)))

    runtime_db = root / str(app.get("runtime_db", "data/runtime/keiba_v2.sqlite3"))
    history_db = root / str(app.get("history_db", "data/runtime/history.sqlite3"))
    model_path = root / str(pred.get("model_path", "data/runtime/model.txt"))

    for path, name in ((runtime_db, "runtime_db"), (history_db, "history_db")):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(path) as conn:
                conn.execute("SELECT 1")
            checks.append(Check(name, True, str(path)))
        except Exception as exc:
            checks.append(Check(name, False, f"{path}: {exc}"))

    checks.extend([
        _module_check("pandas"),
        _module_check("numpy"),
        _module_check("yaml"),
        _module_check("openpyxl"),
        _module_check("lightgbm", required=False),
        _module_check("sklearn", required=False),
        _module_check("mlflow", required=False),
        _module_check("prefect", required=False),
        _module_check("requests", required=False),
        _module_check("bs4", required=False),
        _module_check("selenium", required=False),
        _module_check("playwright", required=False),
    ])

    checks.append(Check(
        "trained_model",
        model_path.exists(),
        str(model_path) if model_path.exists() else "model not trained; fallback prediction will be used",
        required=False,
    ))

    history_rows = 0
    if history_db.exists():
        try:
            with sqlite3.connect(history_db) as conn:
                row = conn.execute("SELECT COUNT(*) FROM horse_runs").fetchone()
                history_rows = int(row[0]) if row else 0
            checks.append(Check(
                "history_rows",
                history_rows > 0,
                f"{history_rows} rows",
                required=False,
            ))
        except Exception as exc:
            checks.append(Check("history_rows", False, str(exc), required=False))

    required_ok = all(c.ok for c in checks if c.required)
    collection_modules = {c.name: c.ok for c in checks if c.name in {
        "module:requests", "module:bs4", "module:selenium", "module:playwright"
    }}
    collection_ready = bool(collection_modules) and all(collection_modules.values())
    ml_modules = {c.name: c.ok for c in checks if c.name in {"module:lightgbm", "module:sklearn"}}
    ml_ready = bool(ml_modules) and all(ml_modules.values()) and model_path.exists()

    return {
        "ok": required_ok,
        "mode": str(app.get("mode", "SHADOW")),
        "collection_ready": collection_ready,
        "ml_ready": ml_ready,
        "history_rows": history_rows,
        "checks": [c.to_dict() for c in checks],
    }
