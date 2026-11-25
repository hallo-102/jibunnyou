"""Docker版の実行時共通設定。予想ロジックは含めない。"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


JST = ZoneInfo("Asia/Tokyo")


def validate_raceday(value: str) -> str:
    """YYYYMMDDを実在日付として検証し、そのまま返す。"""
    text = str(value).strip()
    try:
        datetime.strptime(text, "%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"対象日は実在するYYYYMMDD形式で指定してください: {text!r}") from exc
    return text


def resolve_raceday(cli_value: str | None = None) -> str:
    """CLI引数、KEIBA_RACEDAY、JST当日の順で対象日を決める。"""
    if cli_value is not None and str(cli_value).strip():
        return validate_raceday(str(cli_value))
    env_value = os.getenv("KEIBA_RACEDAY", "").strip()
    if env_value:
        return validate_raceday(env_value)
    return datetime.now(JST).strftime("%Y%m%d")


def env_path(name: str, default: Path) -> Path:
    """環境変数のパスを優先し、未設定なら安全な既定値を返す。"""
    value = os.getenv(name, "").strip()
    return Path(value).expanduser() if value else default
