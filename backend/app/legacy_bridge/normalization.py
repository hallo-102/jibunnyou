from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


def normalize_horse_name(value: Any) -> str:
    """Normalize horse names for matching across Excel and OZZU files."""

    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    return re.sub(r"\s+", "", text).strip()


def compact_key(value: Any) -> str:
    """Normalize column names by removing spaces and normalizing width."""

    text = unicodedata.normalize("NFKC", str(value))
    return re.sub(r"\s+", "", text).strip().lower()


def pick_value(row: dict[str, Any], aliases: list[str], default: Any = None) -> Any:
    """Pick a row value using exact or compacted aliases."""

    for alias in aliases:
        if alias in row and not is_blank(row[alias]):
            return row[alias]

    compact_aliases = {compact_key(alias) for alias in aliases}
    for key, value in row.items():
        if compact_key(key) in compact_aliases and not is_blank(value):
            return value
    return default


def is_blank(value: Any) -> bool:
    """Return True for None, NaN, or empty strings."""

    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    return isinstance(value, str) and value.strip() == ""


def safe_str(value: Any) -> str | None:
    """Convert a value to a stripped string unless it is blank."""

    if is_blank(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def safe_int(value: Any) -> int | None:
    """Convert numeric-looking values to int."""

    if is_blank(value):
        return None
    text = str(value).strip()
    match = re.search(r"-?\d+", text)
    if match is None:
        return None
    return int(match.group(0))


def safe_float(value: Any) -> float | None:
    """Convert numeric-looking values to float."""

    if is_blank(value):
        return None
    text = unicodedata.normalize("NFKC", str(value)).strip()
    text = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if match is None:
        return None
    return float(match.group(0))


def parse_date(value: Any) -> date | None:
    """Parse date-like values from Excel, CSV, or filenames."""

    if is_blank(value):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def parse_race_date_from_filename(path: Path) -> date | None:
    """Extract YYYYMMDD from a file name."""

    match = re.search(r"(20\d{6})", path.name)
    if match is None:
        return None
    return parse_date(match.group(1))


def normalize_race_id(value: Any) -> str | None:
    """Return a stable 12 digit race id when present."""

    if is_blank(value):
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    digits = re.sub(r"\D", "", text)
    if len(digits) == 12:
        return digits
    return digits or None


def race_no_from_id(race_id: str | None) -> int | None:
    """Extract race number from the last two digits of a JRA race id."""

    if not race_id or len(race_id) < 2:
        return None
    return safe_int(race_id[-2:])


def parse_race_no(value: Any) -> int | None:
    """Parse race number from values like 1, 1R, or 01R."""

    return safe_int(value)


def file_sha256(path: Path) -> str:
    """Calculate a SHA-256 checksum for an input file."""

    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def jsonable(value: Any) -> Any:
    """Convert pandas/numpy values into JSON-safe Python values."""

    if is_blank(value):
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "item"):
        return jsonable(value.item())
    if isinstance(value, dict):
        return {str(key): jsonable(child) for key, child in value.items()}
    if isinstance(value, list):
        return [jsonable(child) for child in value]
    return value


def row_to_jsonable(row: dict[str, Any]) -> dict[str, Any]:
    """Convert a pandas row dict to a JSON-safe dict."""

    return {str(key): jsonable(value) for key, value in row.items()}
