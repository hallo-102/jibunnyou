"""rclone前に当日完成Excelをfail-closedで検査する。"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from openpyxl import load_workbook

from nas.scripts.finalize_unsupported_races import (
    FORMAL_REASON_CODES,
    LEGACY_UNCOMPUTED_COLUMNS,
    NO_PREDICTION_STATUS,
    OTHER_REASON_CODE,
    PREDICTED_STATUS,
    STATUS_COLUMNS,
)


TARGET_SHEET = "買い目_レース別1行"
NOW_SHEET = "今走レース情報"
REQUIRED_COLUMN_INDEXES = tuple(range(2, 8)) + tuple(range(12, 19))


@dataclass(frozen=True)
class ExcelValidationResult:
    """完成Excelの検査結果。"""

    path: str
    raceday: str
    size: int
    sheet: str
    race_count: int
    source_race_count: int
    predicted_race_count: int
    unsupported_race_count: int
    unclassified_race_count: int
    other_unsupported_count: int
    duplicate_race_count: int
    required_headers: list[str]


def _race_ids(sheet) -> set[str]:
    """見出しからレースID列を特定し、データ行のIDを抽出する。"""
    header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
    normalized_headers = [str(value or "").strip().lower() for value in header_row]
    id_index = None
    for candidate in ("rid_str", "レースid", "race_id", "rid"):
        if candidate in normalized_headers:
            id_index = normalized_headers.index(candidate) + 1
            break
    if id_index is None:
        raise ValueError(f"レースID列が見つかりません: sheet={sheet.title}")
    values: set[str] = set()
    for row in sheet.iter_rows(min_row=2, min_col=id_index, max_col=id_index, values_only=True):
        value = row[0]
        if value is None or not str(value).strip():
            continue
        text = str(value).strip()
        if text.endswith(".0"):
            text = text[:-2]
        values.add(text)
    return values


def _rows_with_ids(sheet) -> tuple[list[str], list[tuple[str, tuple[object, ...]]]]:
    """見出しとrace_id付き全行を返し、重複検査で行数を失わないようにする。"""
    values = list(sheet.iter_rows(values_only=True))
    headers = [str(value or "").strip() for value in values[0]]
    normalized = [header.lower() for header in headers]
    id_index = None
    for candidate in ("rid_str", "レースid", "race_id", "rid"):
        if candidate in normalized:
            id_index = normalized.index(candidate)
            break
    if id_index is None:
        raise ValueError(f"レースID列が見つかりません: sheet={sheet.title}")
    rows: list[tuple[str, tuple[object, ...]]] = []
    for row in values[1:]:
        value = row[id_index]
        if value is None or not str(value).strip():
            continue
        text = str(value).strip().removesuffix(".0")
        rows.append((text, row))
    return headers, rows


def _is_blank(value: object) -> bool:
    """未計算セルとして許可できる空値だけを判定する。"""
    return value is None or not str(value).strip()


def _source_race_ids(path: Path) -> set[str]:
    """予想前の元入力から全race_idを取得する。"""
    source = load_workbook(path, read_only=True, data_only=True)
    try:
        if NOW_SHEET not in source.sheetnames:
            raise ValueError(f"元入力に必須シートがありません: {NOW_SHEET}")
        return _race_ids(source[NOW_SHEET])
    finally:
        source.close()


def validate_excel(
    path: Path,
    raceday: str,
    stable_seconds: float = 0.0,
    source_input: Path | None = None,
) -> ExcelValidationResult:
    """正式名、書込完了、必須シート・列、全レース収録を検査する。"""
    expected_name = f"馬の競走成績_with_feat_{raceday}.xlsx"
    if path.name != expected_name:
        raise ValueError(f"正式ファイル名が対象日と一致しません: expected={expected_name}, actual={path.name}")
    if path.name.startswith("~$") or ".uploading" in path.name:
        raise ValueError(f"一時ファイルはアップロードできません: {path.name}")
    if not path.is_file():
        raise FileNotFoundError(f"完成Excelがありません: {path}")
    first_size = path.stat().st_size
    if first_size <= 0:
        raise ValueError(f"完成Excelが空です: {path}")
    if stable_seconds > 0:
        time.sleep(stable_seconds)
        second_size = path.stat().st_size
        if first_size != second_size:
            raise RuntimeError(f"完成Excelのサイズが変化中です: {first_size} -> {second_size}")

    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        if TARGET_SHEET not in workbook.sheetnames:
            raise ValueError(f"必須シートがありません: {TARGET_SHEET}")
        sheet = workbook[TARGET_SHEET]
        if sheet.max_column < max(REQUIRED_COLUMN_INDEXES):
            raise ValueError(f"必須列B～G・L～Rがありません: max_column={sheet.max_column}")
        header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
        headers = [str(header_row[index - 1] or "").strip() for index in REQUIRED_COLUMN_INDEXES]
        if any(not header for header in headers):
            raise ValueError(f"必須列B～G・L～Rの見出しに空欄があります: {headers}")
        all_headers, bet_rows = _rows_with_ids(sheet)
        header_indexes = {header: index for index, header in enumerate(all_headers)}
        missing_status_headers = [header for header in STATUS_COLUMNS if header not in header_indexes]
        if missing_status_headers:
            raise ValueError(f"対象外管理の必須列がありません: {missing_status_headers}")
        bet_race_id_list = [race_id for race_id, _ in bet_rows]
        bet_race_ids = set(bet_race_id_list)
        duplicate_count = len(bet_race_id_list) - len(bet_race_ids)
        if duplicate_count:
            raise ValueError(f"レースID重複があります: duplicate_count={duplicate_count}")
        if not bet_race_ids:
            raise ValueError(f"{TARGET_SHEET} にレースデータ行がありません")

        predicted_count = 0
        unsupported_count = 0
        unclassified_count = 0
        other_count = 0
        for race_id, row in bet_rows:
            status = str(row[header_indexes["予想状態"]] or "").strip()
            availability = str(row[header_indexes["予想可否"]] or "").strip()
            if status == PREDICTED_STATUS and availability == "数値予想済み":
                predicted_count += 1
                continue
            if status != NO_PREDICTION_STATUS or availability != "予想対象外":
                unclassified_count += 1
                continue
            unsupported_count += 1
            reason_code = str(row[header_indexes["対象外理由コード"]] or "").strip()
            if reason_code == OTHER_REASON_CODE:
                other_count += 1
            elif reason_code not in FORMAL_REASON_CODES:
                unclassified_count += 1
            if str(row[header_indexes["買い目"]] or "").strip() != "見送り":
                raise ValueError(f"対象外レースの買い目が見送りではありません: race_id={race_id}")
            if str(row[header_indexes["買い目判定"]] or "").strip() != "NO_BET":
                raise ValueError(f"対象外レースの買い目判定がNO_BETではありません: race_id={race_id}")
            amount = row[header_indexes["購入金額"]]
            if not isinstance(amount, (int, float)) or float(amount) != 0.0:
                raise ValueError(f"対象外レースの購入金額が0円ではありません: race_id={race_id}")
            required_blank = [*LEGACY_UNCOMPUTED_COLUMNS, "score", "rank", "予想印", "軸馬", "相手馬"]
            non_blank = [header for header in required_blank if header in header_indexes and not _is_blank(row[header_indexes[header]])]
            if non_blank:
                raise ValueError(f"対象外レースの未計算項目に値があります: race_id={race_id}, columns={non_blank}")
            if _is_blank(row[header_indexes["対象外理由"]]) or _is_blank(row[header_indexes["新聞表示"]]):
                raise ValueError(f"対象外レースの理由または新聞表示が空です: race_id={race_id}")

        if unclassified_count:
            raise ValueError(f"未分類レースがあります: unclassified_count={unclassified_count}")
        if other_count:
            raise ValueError(f"OTHER_UNSUPPORTEDがあります: count={other_count}")
        if len(bet_rows) != predicted_count + unsupported_count:
            raise ValueError(
                "入力レース分類の合計が一致しません: "
                f"total={len(bet_rows)} predicted={predicted_count} unsupported={unsupported_count}"
            )
        source_ids = set(bet_race_ids)
        if NOW_SHEET in workbook.sheetnames:
            now_race_ids = _race_ids(workbook[NOW_SHEET])
            if source_input is None and now_race_ids != bet_race_ids:
                missing = sorted(now_race_ids - bet_race_ids)
                extra = sorted(bet_race_ids - now_race_ids)
                raise ValueError(f"全レース一致検査に失敗しました: missing={missing}, extra={extra}")
            if not now_race_ids.issubset(bet_race_ids):
                extra = sorted(now_race_ids - bet_race_ids)
                raise ValueError(f"完成Excelの今走情報に未分類レースがあります: extra={extra}")
            if source_input is None:
                source_ids = now_race_ids
    finally:
        workbook.close()

    if source_input is not None:
        if not source_input.is_file():
            raise FileNotFoundError(f"元入力Excelがありません: {source_input}")
        source_ids = _source_race_ids(source_input)
        if source_ids != bet_race_ids:
            missing = sorted(source_ids - bet_race_ids)
            extra = sorted(bet_race_ids - source_ids)
            raise ValueError(
                "元入力との全レース一致検査に失敗しました: "
                f"source={len(source_ids)} final={len(bet_race_ids)} missing={missing} extra={extra}"
            )

    # 保存完了後に再度開けることを確認する。
    reopened = load_workbook(path, read_only=True, data_only=True)
    reopened.close()
    return ExcelValidationResult(
        str(path),
        raceday,
        first_size,
        TARGET_SHEET,
        len(bet_race_ids),
        len(source_ids),
        predicted_count,
        unsupported_count,
        unclassified_count,
        other_count,
        duplicate_count,
        headers,
    )


def main() -> int:
    """CLIで完成Excelを検査し、結果をJSON表示する。"""
    parser = argparse.ArgumentParser(description="完成Excelを検査します")
    parser.add_argument("path", type=Path)
    parser.add_argument("raceday")
    parser.add_argument("--stable-seconds", type=float, default=2.0)
    parser.add_argument("--source-input", type=Path, help="予想前の元入力Excel")
    args = parser.parse_args()
    result = validate_excel(args.path, args.raceday, args.stable_seconds, args.source_input)
    print(json.dumps(asdict(result), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
