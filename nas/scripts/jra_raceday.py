"""JRA開催日を、ローカルカレンダーまたは公開レース一覧から判定する。"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path

import requests


@dataclass(frozen=True)
class RaceDayResult:
    """開催日判定の結果。"""

    raceday: str
    is_raceday: bool
    race_count: int
    source: str


def _read_calendar(raceday: str, calendar_path: Path) -> RaceDayResult:
    """1行1日付の管理カレンダーを読み、開催有無を返す。"""
    values = {
        line.split("#", 1)[0].strip()
        for line in calendar_path.read_text(encoding="utf-8").splitlines()
        if line.split("#", 1)[0].strip()
    }
    invalid = sorted(value for value in values if not re.fullmatch(r"\d{8}", value))
    if invalid:
        raise ValueError(f"開催日カレンダーに不正な行があります: {invalid[:3]}")
    return RaceDayResult(raceday, raceday in values, 0, f"calendar:{calendar_path}")


def check_jra_raceday(raceday: str, calendar_path: Path | None = None) -> RaceDayResult:
    """JRA公式の日別競馬番組で、取得失敗と非開催日を区別する。"""
    if calendar_path and calendar_path.is_file():
        return _read_calendar(raceday, calendar_path)

    year = raceday[:4]
    month = str(int(raceday[4:6]))
    monthday = raceday[4:]
    template = os.getenv(
        "KEIBA_RACEDAY_CHECK_URL",
        "https://www.jra.go.jp/keiba/calendar{year}/{year}/{month}/{monthday}.html",
    )
    url = template.format(raceday=raceday, year=year, month=month, monthday=monthday)
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; keiba-nas-raceday-check/1.0)"},
            timeout=30,
        )
    except requests.RequestException as exc:
        raise RuntimeError(f"開催日判定ページを取得できません: {url}") from exc

    if response.status_code == 200:
        response.encoding = response.apparent_encoding
        expected_date = f"{year}年{int(raceday[4:6])}月{int(raceday[6:])}日"
        if expected_date not in response.text or "競馬番組" not in response.text:
            raise RuntimeError("JRA日別番組ページが対象日を示していません。判定不能として停止します")
        return RaceDayResult(raceday, True, 0, url)

    if response.status_code in {403, 404}:
        # JRA全体への遮断を非開催と誤認しないよう、年次開催日程への到達を追加確認する。
        index_url = f"https://www.jra.go.jp/keiba/calendar{year}/index.html"
        try:
            index_response = requests.get(
                index_url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; keiba-nas-raceday-check/1.0)"},
                timeout=30,
            )
        except requests.RequestException as exc:
            raise RuntimeError("JRA日別ページの非開催確認に失敗しました") from exc
        index_response.encoding = index_response.apparent_encoding
        if index_response.status_code != 200 or "開催日程" not in index_response.text:
            raise RuntimeError("JRAサイトへの遮断と非開催を区別できないため停止します")
        return RaceDayResult(raceday, False, 0, url)

    raise RuntimeError(f"JRA日別番組ページが予期しないHTTP状態です: status={response.status_code}")


def main() -> int:
    """CLIから開催日判定を実行し、JSONを表示する。"""
    parser = argparse.ArgumentParser(description="JRA開催日を判定します")
    parser.add_argument("raceday", help="対象日（YYYYMMDD）")
    parser.add_argument("--calendar", type=Path, help="任意の管理カレンダー")
    args = parser.parse_args()
    result = check_jra_raceday(args.raceday, args.calendar)
    print(json.dumps(asdict(result), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
