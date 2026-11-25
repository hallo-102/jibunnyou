"""数値予想不能レースを、計算値を捏造せず最終Excelへ補完する。"""

from __future__ import annotations

import argparse
import json
import os
from copy import copy
from dataclasses import asdict, dataclass
from pathlib import Path

from openpyxl import load_workbook
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter


BET_SHEET = "買い目_レース別1行"
NOW_SHEET = "今走レース情報"
PREDICTED_STATUS = "PREDICTED"
NO_PREDICTION_STATUS = "NO_PREDICTION"
FORMAL_REASON_CODES = {"NEW_HORSE", "INSUFFICIENT_HISTORY", "JUMP_RACE"}
OTHER_REASON_CODE = "OTHER_UNSUPPORTED"
STATUS_COLUMNS = [
    "予想可否",
    "予想状態",
    "買い目",
    "買い目判定",
    "対象外理由コード",
    "対象外理由",
    "購入金額",
    "score",
    "rank",
    "予想印",
    "軸馬",
    "相手馬",
    "新聞表示",
]
LEGACY_UNCOMPUTED_COLUMNS = [
    "score1",
    "score2",
    "gap12",
    "dango_2_5",
    "1位馬番",
    "2位馬番",
    "3位馬番",
    "4位馬番",
    "5位馬番",
    "6位馬番",
    "ランク(S/A/B)",
    "軸馬番",
    "単勝オッズ_1位",
    "1位判定",
    "対象外(1位)",
]


REASON_TEXTS = {
    "NEW_HORSE": (
        "新馬戦で必要な過去走がありません。現在の予想モデルでは信頼できる順位を計算できないため、"
        "このレースは予想対象外です。"
    ),
    "INSUFFICIENT_HISTORY": (
        "有効な過去走・履歴が不足しています。現在の予想モデルでは信頼できる順位を計算できないため、"
        "このレースは予想対象外です。"
    ),
    "JUMP_RACE": (
        "現在の予想モデルは平地競走用のため、この障害レースには適用していません。"
    ),
    "OTHER_UNSUPPORTED": (
        "現在の分類規則では予想不能になった理由を確定できません。確認が必要なため本番処理を停止します。"
    ),
}
NEWSPAPER_TEXTS = {
    "NEW_HORSE": (
        "【予想対象外・データ不足】\n"
        "新馬または有効な過去走が不足しており、現在の予想モデルでは信頼できる順位を計算できません。\n"
        "買い目：見送り"
    ),
    "INSUFFICIENT_HISTORY": (
        "【予想対象外・データ不足】\n"
        "新馬または有効な過去走が不足しており、現在の予想モデルでは信頼できる順位を計算できません。\n"
        "買い目：見送り"
    ),
    "JUMP_RACE": (
        "【予想対象外・障害レース】\n"
        "現在の予想モデルは平地競走用のため、この障害レースには適用していません。\n"
        "買い目：見送り"
    ),
    "OTHER_UNSUPPORTED": (
        "【予想対象外・要確認】\n"
        "対象外理由を自動分類できませんでした。READY作成とアップロードを停止します。\n"
        "買い目：見送り"
    ),
}


@dataclass(frozen=True)
class CompletionResult:
    """予想対象外レース補完の集計結果。"""

    path: str
    input_race_count: int
    predicted_race_count: int
    unsupported_race_count: int
    other_unsupported_count: int
    added_race_ids: list[str]
    classifications: dict[str, str]


def _normalize_race_id(value: object) -> str:
    """Excel数値・文字列のrace_idを12桁文字列へ正規化する。"""
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    if not text.isdigit():
        raise ValueError(f"12桁のレースIDではありません: {value!r}")
    normalized = text.zfill(12)
    if len(normalized) != 12:
        raise ValueError(f"12桁のレースIDではありません: {value!r}")
    return normalized


def _header_map(sheet) -> dict[str, int]:
    """1行目の非空見出しを列番号へ対応付ける。"""
    mapping: dict[str, int] = {}
    for cell in sheet[1]:
        header = str(cell.value or "").strip()
        if not header:
            continue
        if header in mapping:
            raise ValueError(f"見出しが重複しています: sheet={sheet.title}, header={header}")
        mapping[header] = cell.column
    return mapping


def _records_by_race(sheet) -> dict[str, list[dict[str, object]]]:
    """今走レース情報をrace_id別のレコードへ変換する。"""
    headers = [str(cell.value or "").strip() for cell in sheet[1]]
    if "レースID" not in headers:
        raise ValueError(f"レースID列がありません: sheet={sheet.title}")
    grouped: dict[str, list[dict[str, object]]] = {}
    for values in sheet.iter_rows(min_row=2, values_only=True):
        record = {header: values[index] for index, header in enumerate(headers) if header}
        if record.get("レースID") in (None, ""):
            continue
        race_id = _normalize_race_id(record["レースID"])
        grouped.setdefault(race_id, []).append(record)
    return grouped


def _history_coverage(workbook, race_id: str, current_rows: list[dict[str, object]]) -> tuple[int, int]:
    """登録馬のうち対象レース自身を除く過去走がある馬名の数を返す。"""
    current_horses = {str(row.get("馬名") or "").strip() for row in current_rows}
    current_horses.discard("")
    if race_id not in workbook.sheetnames:
        return 0, len(current_horses)
    sheet = workbook[race_id]
    headers = _header_map(sheet)
    horse_column = headers.get("馬名")
    history_race_id_column = headers.get("race_id")
    if horse_column is None or history_race_id_column is None:
        return 0, len(current_horses)
    history_horses: set[str] = set()
    for row_number in range(2, sheet.max_row + 1):
        horse_name = str(sheet.cell(row=row_number, column=horse_column).value or "").strip()
        history_race_id = sheet.cell(row=row_number, column=history_race_id_column).value
        if not horse_name or history_race_id in (None, ""):
            continue
        # 過去日を後日再取得した場合は対象レースの結果も先頭行に入るため、
        # そのレース自身を「過去走」として数えない。
        if _normalize_race_id(history_race_id) == race_id:
            continue
        history_horses.add(horse_name)
    history_horses.discard("")
    return len(current_horses & history_horses), len(current_horses)


def _classify_race(workbook, race_id: str, current_rows: list[dict[str, object]]) -> str:
    """実際のレース条件と過去走収録状況から対象外理由を分類する。"""
    representative = current_rows[0]
    race_name = str(representative.get("レース名") or "")
    race_class = str(representative.get("クラス") or "")
    course = str(representative.get("コース") or "")
    covered, registered = _history_coverage(workbook, race_id, current_rows)
    if "障害" in race_name or course.startswith("障"):
        return "JUMP_RACE"
    if ("新馬" in race_name or "新馬" in race_class) and covered == 0:
        return "NEW_HORSE"
    if registered == 0 or covered < registered:
        return "INSUFFICIENT_HISTORY"
    return OTHER_REASON_CODE


def _copy_cell_style(source, destination) -> None:
    """既存行の表示書式だけを新規セルへ複製する。"""
    if source.has_style:
        destination._style = copy(source._style)
    if source.number_format:
        destination.number_format = source.number_format
    destination.alignment = copy(source.alignment)
    destination.protection = copy(source.protection)


def _ensure_status_columns(sheet) -> dict[str, int]:
    """状態列を末尾へ追加し、既存ヘッダー書式を維持する。"""
    headers = _header_map(sheet)
    source_header = sheet.cell(row=1, column=1)
    for header in STATUS_COLUMNS:
        if header in headers:
            continue
        column = sheet.max_column + 1
        cell = sheet.cell(row=1, column=column, value=header)
        _copy_cell_style(source_header, cell)
        headers[header] = column
    widths = {
        "予想可否": 14,
        "予想状態": 18,
        "買い目": 12,
        "買い目判定": 14,
        "対象外理由コード": 24,
        "対象外理由": 58,
        "購入金額": 12,
        "score": 12,
        "rank": 10,
        "予想印": 10,
        "軸馬": 12,
        "相手馬": 18,
        "新聞表示": 58,
    }
    for header, width in widths.items():
        sheet.column_dimensions[get_column_letter(headers[header])].width = width
    return headers


def _existing_bet_rows(sheet, race_id_column: int) -> dict[str, int]:
    """買い目シートのrace_id行番号を取得し、重複を拒否する。"""
    rows: dict[str, int] = {}
    for row_number in range(2, sheet.max_row + 1):
        value = sheet.cell(row=row_number, column=race_id_column).value
        if value in (None, ""):
            continue
        race_id = _normalize_race_id(value)
        if race_id in rows:
            raise ValueError(f"買い目シートのレースIDが重複しています: {race_id}")
        rows[race_id] = row_number
    return rows


def _write_unsupported_row(
    sheet,
    row_number: int,
    headers: dict[str, int],
    source_record: dict[str, object],
    reason_code: str,
) -> None:
    """対象外レース1行を空の計算欄と正式理由付きで書き込む。"""
    style_row = max(2, row_number - 1)
    for column in range(1, sheet.max_column + 1):
        _copy_cell_style(sheet.cell(row=style_row, column=column), sheet.cell(row=row_number, column=column))
    for header in ("レースID", "レース名", "発走時刻", "場所", "コース", "馬場", "頭数"):
        if header in headers:
            sheet.cell(row=row_number, column=headers[header], value=source_record.get(header))
    for header in LEGACY_UNCOMPUTED_COLUMNS:
        if header in headers:
            sheet.cell(row=row_number, column=headers[header], value=None)
    values = {
        "予想可否": "予想対象外",
        "予想状態": NO_PREDICTION_STATUS,
        "買い目": "見送り",
        "買い目判定": "NO_BET",
        "対象外理由コード": reason_code,
        "対象外理由": REASON_TEXTS[reason_code],
        "購入金額": 0,
        "score": None,
        "rank": None,
        "予想印": None,
        "軸馬": None,
        "相手馬": None,
        "新聞表示": NEWSPAPER_TEXTS[reason_code],
    }
    for header, value in values.items():
        sheet.cell(row=row_number, column=headers[header], value=value)
    sheet.cell(row=row_number, column=headers["購入金額"]).number_format = '#,##0"円"'
    for header in ("対象外理由", "新聞表示"):
        cell = sheet.cell(row=row_number, column=headers[header])
        cell.alignment = Alignment(vertical="top", wrap_text=True)
    sheet.row_dimensions[row_number].height = 54


def complete_unsupported_races(output_path: Path, source_input: Path, raceday: str) -> CompletionResult:
    """不足レースを正式対象外として補完し、再オープン後に原子的置換する。"""
    if output_path.name != f"馬の競走成績_with_feat_{raceday}.xlsx":
        raise ValueError(f"正式ファイル名ではありません: {output_path.name}")
    if not output_path.is_file() or not source_input.is_file():
        raise FileNotFoundError("補完に必要な入力Excelまたは完成Excelがありません")

    source_book = load_workbook(source_input, read_only=True, data_only=True)
    output_book = load_workbook(output_path)
    temporary = output_path.with_name(f".{output_path.name}.unsupported.tmp.xlsx")
    try:
        if NOW_SHEET not in source_book.sheetnames or BET_SHEET not in output_book.sheetnames:
            raise ValueError("今走レース情報または買い目_レース別1行シートがありません")
        source_races = _records_by_race(source_book[NOW_SHEET])
        bet_sheet = output_book[BET_SHEET]
        headers = _ensure_status_columns(bet_sheet)
        existing = _existing_bet_rows(bet_sheet, headers["レースID"])
        extra = sorted(set(existing) - set(source_races))
        if extra:
            raise ValueError(f"入力に存在しないレースが買い目シートにあります: {extra}")

        # 既存の数値予想行はA～Xを触らず、分類用メタデータだけ追加する。
        for race_id, row_number in existing.items():
            status = str(bet_sheet.cell(row=row_number, column=headers["予想状態"]).value or "").strip()
            if status == NO_PREDICTION_STATUS:
                continue
            bet_sheet.cell(row=row_number, column=headers["予想可否"], value="数値予想済み")
            bet_sheet.cell(row=row_number, column=headers["予想状態"], value=PREDICTED_STATUS)

        classifications: dict[str, str] = {}
        added: list[str] = []
        for race_id in sorted(set(source_races) - set(existing)):
            rows = source_races[race_id]
            reason_code = _classify_race(source_book, race_id, rows)
            classifications[race_id] = reason_code
            current_rows = _existing_bet_rows(bet_sheet, headers["レースID"])
            row_number = next(
                (number for existing_id, number in sorted(current_rows.items(), key=lambda item: item[1]) if existing_id > race_id),
                bet_sheet.max_row + 1,
            )
            if row_number <= bet_sheet.max_row:
                bet_sheet.insert_rows(row_number)
            _write_unsupported_row(bet_sheet, row_number, headers, rows[0], reason_code)
            added.append(race_id)

        output_book.save(temporary)
    finally:
        source_book.close()
        output_book.close()

    # 生成途中のブックでは置換せず、必須シートと件数を再確認する。
    reopened = load_workbook(temporary, read_only=True, data_only=True)
    try:
        if BET_SHEET not in reopened.sheetnames:
            raise ValueError(f"補完後に必須シートがありません: {BET_SHEET}")
        sheet = reopened[BET_SHEET]
        headers = _header_map(sheet)
        rows = _existing_bet_rows(sheet, headers["レースID"])
        if set(rows) != set(source_races):
            missing = sorted(set(source_races) - set(rows))
            extra = sorted(set(rows) - set(source_races))
            raise ValueError(f"補完後の全レース一致に失敗しました: missing={missing}, extra={extra}")
        predicted_count = 0
        unsupported_count = 0
        other_count = 0
        for row_number in rows.values():
            status = str(sheet.cell(row=row_number, column=headers["予想状態"]).value or "").strip()
            code = str(sheet.cell(row=row_number, column=headers["対象外理由コード"]).value or "").strip()
            if status == PREDICTED_STATUS:
                predicted_count += 1
            elif status == NO_PREDICTION_STATUS:
                unsupported_count += 1
                other_count += int(code == OTHER_REASON_CODE)
    finally:
        reopened.close()

    os.replace(temporary, output_path)
    return CompletionResult(
        str(output_path),
        len(source_races),
        predicted_count,
        unsupported_count,
        other_count,
        added,
        classifications,
    )


def main() -> int:
    """CLIから最終Excelの対象外レース補完を実行する。"""
    parser = argparse.ArgumentParser(description="予想不能レースを正式な対象外行として補完します")
    parser.add_argument("output_path", type=Path)
    parser.add_argument("source_input", type=Path)
    parser.add_argument("raceday")
    args = parser.parse_args()
    result = complete_unsupported_races(args.output_path, args.source_input, args.raceday)
    print(json.dumps(asdict(result), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
