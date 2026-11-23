"""xlsx loader with optional openpyxl dependency.

If openpyxl is not installed, callers get OpenpyxlMissingError with a clear
message so the GUI can show it and continue with manual input.
Unknown/missing columns are reported as '不明' - never guessed.
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from evaluation_utils import JST
from evaluation_utils import integer, parse_timestamp

try:
    from storage import compute_sha256
except ImportError:  # when imported as part of a package / different cwd
    import os as _os
    import sys as _sys

    _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
    from storage import compute_sha256  # noqa: E402

UNKNOWN = "不明"

COLUMN_CANDIDATES = {
    "date": ["開催日", "日付", "date"],
    "place": ["競馬場", "場所", "場", "place"],
    "race_no": ["R", "レース番号", "race_no"],
    "race_id": ["race_id", "レースID", "rid_str"],
    "horse_no": ["馬番"],
    "horse_name": ["馬名"],
    "race_name": ["レース名", "クラス", "レース名・クラス"],
    "field_size": ["頭数", "出走頭数"],
    "start_time": ["発走時刻", "start_time"],
    "course": ["コース", "course"],
    "excel_rank": ["ランク(S/A/B)", "ランク", "race_rank"],
    "excel_judgement": ["判定"],
    "excel_reason": ["理由"],
    "prediction_status": ["予想状態"],
    "prediction_availability": ["予想可否"],
    "exclusion_reason": ["対象外理由"],
    "bet_judgement": ["買い目判定"],
    "roi_judgement": ["購入判定"],
    "roi_reason": ["購入理由"],
    "roi_amount": ["合計購入金額"],
}

HORSE_COLUMNS = {
    "production_rank": ["予想順位", "本番AI順位", "production_rank"],
    "production_score": ["score", "本番AIスコア", "production_score"],
    "popularity_rank": ["人気順位", "人気", "popularity_rank"],
    "popularity_observed_at": ["人気取得時刻", "人気取得日時", "popularity_observed_at"],
    "ai_observed_at": ["AI順位取得時刻", "AI確定時刻", "ai_observed_at"],
    "age": ["年齢", "馬齢", "age"],
}

HISTORY_COLUMNS = {
    "horse_name": ["馬名"], "date": ["日付", "開催日", "date"],
    "distance": ["距離", "コース"], "finish": ["着順"], "field_size": ["頭数"],
    "race_name": ["レース名", "クラス"], "clock": ["タイム"],
    "time_index": ["タイム指数"], "past_race_id": ["race_id", "レースID"],
    "place": ["競馬場", "場所"], "laps": ["ラップタイム", "laps"],
    "passing": ["通過", "コーナー通過順"], "trouble": ["備考", "不利メモ"],
    "winner_clock": ["勝ち馬時計", "勝ち馬走破時計"],
}


class OpenpyxlMissingError(Exception):
    pass


def _find_column(headers, candidates):
    # 空白だけを正規化し、短い「場」が「馬場」に一致する部分一致は使わない。
    normalized = [re.sub(r"\s+", "", str(h)).casefold() if h is not None else "" for h in headers]
    for cand in candidates:
        key = re.sub(r"\s+", "", cand).casefold()
        if key in normalized:
            return normalized.index(key)
    return None


def _filename_date(path):
    # このプロジェクトの原本ファイル名に明記された開催日だけを利用する。
    match = re.search(r"^馬の競走成績(?:_with_feat)?_(\d{8})(?:_|\.)", os.path.basename(path))
    if match:
        try:
            return datetime.strptime(match[1], "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return UNKNOWN


def _read_sheet(ws, target_date=UNKNOWN, current=False, read_horses=True):
    rows = iter(ws.iter_rows(values_only=True))
    headers = next(rows, ())
    if not current and {"開催", "RaceData01"}.issubset(set(headers)):
        raise ValueError("過去走シートは今走一覧に使えません。「今走レース情報」を含む原本を選んでください。")
    col_idx = {key: _find_column(headers, candidates) for key, candidates in COLUMN_CANDIDATES.items()}
    horse_idx = {key: _find_column(headers, candidates) for key, candidates in HORSE_COLUMNS.items()}
    top5_columns = [_find_column(headers, [f"{rank}位馬番"]) for rank in range(1, 6)]

    def cell(row, key):
        idx = col_idx.get(key)
        if idx is None or idx >= len(row):
            return UNKNOWN
        v = row[idx]
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return UNKNOWN
        return v

    races_map = {}
    order = []
    for row_number, row in enumerate(rows, 2):
        if row is None or all(v is None for v in row):
            continue
        date = cell(row, "date")
        place = cell(row, "place")
        race_no = cell(row, "race_no")
        race_id = cell(row, "race_id")
        if race_id != UNKNOWN:
            # Excelの数値IDと文字列IDを同一キーとして扱う。
            race_id = str(int(race_id)) if isinstance(race_id, (int, float)) else str(race_id).strip()
        if current:
            if date == UNKNOWN:
                date = target_date
            if race_no == UNKNOWN and re.fullmatch(r"\d{12}", str(race_id)):
                # JRAレースIDの末尾2桁はレース番号。過去走のR列からは補わない。
                race_no = int(race_id[-2:])
        race_name = cell(row, "race_name")
        # Group key: race_id if known, else date/place/race_no
        key = str(race_id) if race_id != UNKNOWN else f"{date}|{place}|{race_no}"
        if key not in races_map:
            races_map[key] = {
                "date": str(date) if not isinstance(date, str) else date,
                "place": place,
                "race_no": race_no,
                "race_id": race_id,
                "race_key": key,
                "race_name": race_name,
                "field_size": cell(row, "field_size"),
                "horses": [],
            }
            # 原本の判定を表示用に保持し、独立アプリの判定と混同しない。
            for field in COLUMN_CANDIDATES:
                if field not in ("horse_no", "horse_name") and field not in races_map[key]:
                    races_map[key][field] = cell(row, field)
            top5 = []
            for idx in top5_columns:
                if idx is not None and idx < len(row) and row[idx] is not None:
                    try:
                        top5.append(int(row[idx]))
                    except (TypeError, ValueError):
                        continue
            races_map[key]["predicted_top5"] = top5
            order.append(key)
        horse_no = cell(row, "horse_no")
        horse_name = cell(row, "horse_name")
        if read_horses and (horse_no != UNKNOWN or horse_name != UNKNOWN):
            horse = {"horse_no": horse_no, "horse_name": horse_name}
            for field, idx in horse_idx.items():
                horse[field] = row[idx] if idx is not None and idx < len(row) else None
            horse["history"] = []
            horse["input_source"] = {"sheet": ws.title, "row": row_number,
                                      "popularity_column": str(headers[horse_idx['popularity_rank']]) if horse_idx['popularity_rank'] is not None else None,
                                      "ai_rank_column": str(headers[horse_idx['production_rank']]) if horse_idx['production_rank'] is not None else None}
            races_map[key]["horses"].append(horse)
    return [races_map[k] for k in order]


def _read_history(ws):
    """過去走は馬名で対応付ける。過去走の馬番を今走馬番へ転用しない。"""
    rows = iter(ws.iter_rows(values_only=True))
    headers = next(rows, ())
    columns = {key: _find_column(headers, candidates) for key, candidates in HISTORY_COLUMNS.items()}
    by_name = {}
    for row_no, row in enumerate(rows, 2):
        record = {key: row[idx] if idx is not None and idx < len(row) else None for key, idx in columns.items()}
        name = record.pop("horse_name")
        if name is None or not str(name).strip():
            continue
        record.update(source_sheet=ws.title, source_row=row_no)
        # 既知の原本には通過以降の見出しと値がずれた行がある。推測で列をずらして修復しない。
        passing = record.get('passing')
        if passing is not None and not re.fullmatch(r'\d+(?:-\d+)+', str(passing)):
            record['schema_issues'] = ['通過以降の列・値の対応を確認できないため展開・不利情報は未取得扱い']
            record['passing'], record['trouble'] = None, None
        by_name.setdefault(str(name).strip(), []).append(record)
    return by_name


def _attach_history(wb, races, source_sheets):
    for race in races:
        rid = str(race.get("race_id"))
        by_name = {}
        if rid in wb.sheetnames:
            source_sheets.append(rid)
            by_name = _read_history(wb[rid])
        names = set()
        for horse in race["horses"]:
            name = str(horse.get("horse_name", "")).strip()
            names.add(name)
            horse["history"] = by_name.get(name, [])
            # 第1判定のAI順位は原本の馬別確定順位のみ。TOP5から欠損順位を補わない。
        # 障害など、今走表から落ちた馬名は取得するが、今走馬番・AI順位は不明のまま。
        for name, history in by_name.items():
            if not names:
                race["horses"].append({"horse_no": None, "horse_name": name,
                                       "production_rank": None, "production_score": None, "history": history})


def _virtual_plans(ws):
    """原本に明記された買い目・金額だけを仮想検証用に固定する。"""
    iterator = iter(ws.iter_rows(values_only=True))
    headers = next(iterator, ())
    plans = {}
    id_column = _find_column(headers, ['レースID', 'race_id'])
    if id_column is None:
        return plans
    for row_no, row in enumerate(iterator, 2):
        rid = str(row[id_column])
        tickets, valid = [], True
        for kind, size in (('3連複', 3), ('馬連', 2), ('ワイド', 2)):
            count_idx = _find_column(headers, [f'{kind}_点数'])
            amount_idx = _find_column(headers, [f'{kind}_金額'])
            count = integer(row[count_idx]) if count_idx is not None else None
            amount = integer(row[amount_idx]) if amount_idx is not None else None
            if count == 0 and amount in (None, 0):
                continue
            if count is None or count < 1 or amount is None or amount < 0 or amount % count or (amount // count) % 100:
                valid = False
                break
            for point in range(1, count + 1):
                indices = [_find_column(headers, [f'{kind}{point}点目_馬番{i}']) for i in range(1, size + 1)]
                numbers = [integer(row[i]) if i is not None else None for i in indices]
                if any(n is None or n < 1 for n in numbers) or len(set(numbers)) != size:
                    valid = False
                    break
                tickets.append({'ticket_type': kind, 'combination': '-'.join(map(str, numbers)),
                                'amount': amount // count, 'source_sheet': ws.title, 'source_row': row_no})
        plans[rid] = tickets if valid and tickets else None
    return plans


def load_races_from_xlsx(path):
    """今走専用シートを優先し、過去走を混ぜずに開催・出走馬を返す。"""
    try:
        import openpyxl  # type: ignore
    except ImportError as exc:
        raise OpenpyxlMissingError(
            "openpyxl がインストールされていないため xlsx を読み取れません。"
            "Excelの自動判定には pip install openpyxl で依存をインストールしてください。"
        ) from exc

    sha = compute_sha256(path)
    source = open(path, "rb")
    wb = None
    source_sheets = []
    try:
        wb = openpyxl.load_workbook(source, data_only=True, read_only=True)
        target_date = _filename_date(path)
        if "今走レース情報" in wb.sheetnames or "買い目_レース別1行" in wb.sheetnames:
            current_races = []
            if "今走レース情報" in wb.sheetnames:
                source_sheets.append("今走レース情報")
                current_races = _read_sheet(wb["今走レース情報"], target_date, current=True)
            current_by_key = {r["race_key"]: r for r in current_races}
            races = []
            if "買い目_レース別1行" in wb.sheetnames:
                source_sheets.append("買い目_レース別1行")
                metadata = _read_sheet(wb["買い目_レース別1行"], target_date, current=True, read_horses=False)
                for info in metadata:
                    # 新馬・障害など馬別情報がないレースも、明記されたレース情報は残す。
                    race = dict(current_by_key.get(info["race_key"], info))
                    race.update({k: v for k, v in info.items() if k != "horses" and v != UNKNOWN})
                    races.append(race)
            seen = {r["race_key"] for r in races}
            races.extend(r for r in current_races if r["race_key"] not in seen)
            if "回収率重視_買い目候補" in wb.sheetnames:
                source_sheets.append("回収率重視_買い目候補")
                candidates = _read_sheet(wb["回収率重視_買い目候補"], target_date, current=True, read_horses=False)
                candidates_by_id = {r["race_id"]: r for r in candidates if r["race_id"] != UNKNOWN}
                plans = _virtual_plans(wb['回収率重視_買い目候補'])
                for race in races:
                    candidate = candidates_by_id.get(race["race_id"], {})
                    # 候補シートにないレースを、根拠なく「見送り」とはみなさない。
                    for field in ("roi_judgement", "roi_reason", "roi_amount"):
                        if candidate.get(field, UNKNOWN) != UNKNOWN:
                            race[field] = candidate[field]
                    race['virtual_tickets'] = plans.get(str(race['race_id']))
        else:
            # 明示列を持つ従来の単一表形式との互換性を保つ。
            source_sheets.append(wb.active.title)
            races = _read_sheet(wb.active)
        _attach_history(wb, races, source_sheets)
    finally:
        # Windowsで原本をロックしたままにしない。
        if wb is not None:
            wb.close()
        source.close()

    warnings = []
    observed_at = datetime.now(JST).isoformat(timespec='seconds')
    for race in races:
        for horse in race['horses']:
            if not horse.get('popularity_observed_at'):
                horse['popularity_observed_at'] = observed_at
                horse['popularity_time_source'] = 'Excelファイル読取時刻（元の取得時刻ではない）'
            else:
                horse['popularity_time_source'] = 'Excel記載の取得時刻'
            if not horse.get('ai_observed_at'):
                horse['ai_observed_at'] = observed_at
            for key in ('popularity_observed_at', 'ai_observed_at'):
                stamp = parse_timestamp(horse.get(key))
                horse[key] = stamp.isoformat() if stamp else str(horse.get(key))
        expected = race.get("field_size", UNKNOWN)
        try:
            incomplete = len(race["horses"]) < int(expected)
        except (TypeError, ValueError):
            incomplete = not race["horses"]
        race["horse_data_incomplete"] = incomplete
        if any(h.get("horse_no") in (None, UNKNOWN) for h in race["horses"]):
            race["horse_data_incomplete"] = True
            warnings.append(f"{race['place']} {race['race_no']}R: 今走馬番が未取得の馬があります。")
        if incomplete:
            warnings.append(f"{race['place']} {race['race_no']}R: 今走の出走馬情報が不足しています。")
    return {
        "file_name": os.path.basename(path),
        "sha256": sha,
        "races": races,
        "source_sheets": source_sheets,
        "warnings": warnings,
        "observed_at": observed_at,
    }
