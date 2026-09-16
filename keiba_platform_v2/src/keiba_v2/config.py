from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Settings:
    raw: dict[str, Any]
    project_root: Path

    def section(self, name: str) -> dict[str, Any]:
        value = self.raw.get(name, {})
        if not isinstance(value, dict):
            raise ValueError(f"settings section must be a mapping: {name}")
        return value


def load_settings(path: str | Path | None = None) -> Settings:
    project_root = Path(__file__).resolve().parents[2]
    settings_path = Path(path) if path else project_root / "config" / "settings.yaml"
    if not settings_path.is_absolute():
        settings_path = project_root / settings_path
    if not settings_path.exists():
        raise FileNotFoundError(f"settings not found: {settings_path}")
    with settings_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError("settings.yaml root must be a mapping")
    return Settings(raw=raw, project_root=project_root)
