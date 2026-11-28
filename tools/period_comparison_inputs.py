"""期間監査のローカル Excel と証跡目録を比較用の開催日データに変換する。"""

from collections import Counter
from datetime import date
from hashlib import sha256
from io import BytesIO
import json
import re

import pandas as pd

from race_purchase_filter.evaluation_utils import integer
from tokutyouryou_keisann.common import (
    RESULTS_EXCEL_HEADER, RESULTS_EXCEL_SKIPROWS, _normalize_combo, _normalize_result_columns, find_col,
)
from tools.ab_comparison_evidence import result_evidence, snapshot_grade, timestamp
from tools.ab_comparison_inputs import BET_TYPES, RosterUnavailable, features, load_snapshot, result_values
from tools.compare_ab_models import DEFAULT_PERIODS, periods_from, read_json
from tools.prepare_ab_comparison import SNAPSHOT_FILES, result_rows

PERIODS = periods_from({**DEFAULT_PERIODS, "forward": ["2026-10-24", "2026-11-23"]})
RESULT_GRADES = ("A", "B", "C", "unverified", "D")
CONFIRMED_RESULT_GRADES = RESULT_GRADES[:3]


def digest(data):
    content = json.dumps(data, sort_keys=True, ensure_ascii=False, allow_nan=False)
    return sha256(content.encode("utf-8")).hexdigest()


def inventory(path):
    data = read_json(path)
    if data.get("schema_version") != 1:
        raise ValueError("開催日目録は schema_version=1 が必要です")
    days = {}
    for record in data["days"]:
        day = date.fromisoformat(record["date"])
        if day.isoformat() != record["date"] or day in days:
            raise ValueError(f"開催日が不正または重複しています: {record['date']}")
        stage = next((key for key, (start, stop) in PERIODS.items() if start <= day <= stop), None)
        if stage is None:
            raise ValueError(f"対象期間外の開催日です: {day}")
        item = {key: data[key] for key in ("results_workbook", "results_workbook_sha256") if key in data}
        if "results_workbook" in record:
            item.clear()
        item.update(record)
        item.setdefault("races", {})
        for rid, race in item["races"].items():
            if not re.fullmatch(r"\d{10,13}", rid):
                raise ValueError(f"不正なレースIDです: {rid}")
            if set(race) - {"input", "result"}:
                raise ValueError(f"{rid}: レース証跡は input と result を指定してください")
            if set(race.get("input", {})) & {"race_id", "workbook", "workbook_sha256"}:
                raise ValueError(f"{rid}: レース証跡では開催日原本を上書きできません")
            if set(race.get("result", {})) & {"race_id", "finish_top3", "payouts", "race_date"}:
                raise ValueError(f"{rid}: 確定着順と払戻は結果Excelから取得します")
        for source in (item, *(race.get("input", {}) for race in item["races"].values())):
            for key in (*SNAPSHOT_FILES, "results_workbook"):
                if key in source and not re.fullmatch(r"[0-9a-f]{64}", source.get(key + "_sha256", "")):
                    raise ValueError(f"{day}: {key}_sha256 が必要です")
        days[day] = (stage, item)
    if not days:
        raise ValueError("開催日目録が空です")
    return dict(sorted(days.items()))


def payout_number(value):
    return integer(str(value).strip().removesuffix("円"))


def payout_combination(value):
    if re.fullmatch(r"\d+\.0+", str(value)):
        return str(integer(value))
    return _normalize_combo(value)


def excel_results(content, path):
    """結果と払戻を別々に読み、片方のシートがない場合も残りを保持する。"""
    entry_cols = ("rid_str", "着順_num", "馬番_int")
    payout_cols = ("rid_str", "払戻種別", "組番_norm", "払戻金_int")
    entries, payouts = [], []
    with pd.ExcelFile(BytesIO(content), engine="openpyxl") as excel:
        for name in excel.sheet_names:
            frame = excel.parse(name, header=RESULTS_EXCEL_HEADER, skiprows=RESULTS_EXCEL_SKIPROWS)
            if frame.empty:
                continue
            worksheet = excel.book[name]
            raw_columns = next(worksheet.iter_rows(
                min_row=1, max_row=1, max_col=len(frame.columns), values_only=True,
            ), tuple(frame.columns))
            frame.columns = _normalize_result_columns(raw_columns, xlsx_path=str(path), sheet_name=name)
            rid = find_col(frame.columns, ["レースID", "race_id"])
            place = find_col(frame.columns, ["着順", "着 順"])
            horse = find_col(frame.columns, ["馬番", "馬 番"])
            kind = find_col(frame.columns, ["払戻種別", "券種", "式別", "種別"])
            combo = find_col(frame.columns, ["組番", "組み合わせ", "馬番"])
            amount = find_col(frame.columns, ["払戻金", "払戻", "配当", "払戻金(円)"])
            if not rid:
                continue
            ids = features._normalize_rid_series(frame[rid])
            if place and horse:
                entries.append(pd.DataFrame(dict(zip(entry_cols, (
                    ids, pd.to_numeric(frame[place], errors="coerce"), frame[horse].map(integer),
                ), strict=True))))
            if kind and combo and amount:
                payouts.append(pd.DataFrame(dict(zip(payout_cols, (
                    ids, frame[kind].astype(str), frame[combo].map(payout_combination), frame[amount].map(payout_number),
                ), strict=True))))
    return (
        pd.concat(entries, ignore_index=True) if entries else pd.DataFrame(columns=entry_cols),
        pd.concat(payouts, ignore_index=True) if payouts else pd.DataFrame(columns=payout_cols),
    )


class AuditInputs:
    def __init__(self, root):
        self.root = root
        self.result_tables = {}

    def load_book(self, item):
        if "workbook" not in item:
            return None
        path = (self.root / item["workbook"]).resolve()
        content = path.read_bytes()
        if sha256(content).hexdigest() != item["workbook_sha256"]:
            raise ValueError(f"SHA-256不一致: {path}")
        return pd.read_excel(BytesIO(content), sheet_name=None, engine="openpyxl")

    def workbook_results(self, item, rids):
        if "results_workbook" not in item:
            return {}
        path = (self.root / item["results_workbook"]).resolve()
        key = (path, item["results_workbook_sha256"])
        if key not in self.result_tables:
            content = path.read_bytes()
            if sha256(content).hexdigest() != key[1]:
                raise ValueError(f"SHA-256不一致: {path}")
            self.result_tables[key] = excel_results(content, path)
        return {row["race_id"]: row for row in result_rows(
            *self.result_tables[key], ({"race_id": rid} for rid in rids),
        )}

    def day(self, day, item):
        reasons = Counter()
        try:
            book = self.load_book(item)
        except FileNotFoundError:
            reasons["missing_workbook_file"] += 1
            book = None
        now = {} if book is None else book.get(features.NOW_SHEET, {})
        roster = {}
        if isinstance(now, pd.DataFrame) and "レースID" not in now:
            rid_alias = find_col(now.columns, ["rid_str", "race_id"])
            if rid_alias:
                now = book[features.NOW_SHEET] = now.rename(columns={rid_alias: "レースID"})
        if "レースID" in now:
            date_col = find_col(now.columns, ["日付", "開催日", "日付(月日)", "年月日", "raceday"])
            if date_col:
                dates = now[date_col].map(features._parse_validation_date)
                if dates.dropna().ne(pd.Timestamp(day)).any():
                    raise ValueError(f"{day}: 開催日原本に別日の出走表があります")
            ids = features._normalize_rid_series(now["レースID"])
            for rid in ids.dropna().unique():
                if re.fullmatch(r"\d{10,13}", rid):
                    roster[rid] = now.loc[ids.eq(rid)]
                else:
                    reasons["invalid_race_id"] += 1
        elif book is not None:
            reasons["missing_now_sheet_or_race_id"] += 1

        rids = sorted(roster.keys() | item["races"].keys())
        try:
            results = self.workbook_results(item, rids) if rids else {}
        except FileNotFoundError:
            reasons["missing_results_workbook_file"] += 1
            results = {}
        records = {}
        for rid in rids:
            proof = item["races"].get(rid, {})
            row = {"race_id": rid, **{key: item[key] for key in ("workbook", "workbook_sha256") if key in item},
                   **proof.get("input", {})}
            start = timestamp(row["start_at"]) if row.get("start_at") else None
            if start is not None and start.date() != day:
                raise ValueError(f"{rid}: 発走日と目録の開催日が一致しません")
            observed = timestamp(row["observed_at"]) if row.get("observed_at") else None
            chronology = bool(start and observed and observed < start)
            grade = snapshot_grade(row)
            input_issues = []
            if rid not in roster:
                grade = "D"
                input_issues.append("missing_race_roster")
            elif start is not None and observed is not None and not chronology:
                grade = "D"
                input_issues.append("input_at_or_after_start")
            elif grade in ("A", "B") and (
                not chronology or not isinstance(row.get("input_evidence_source"), str)
                or not row["input_evidence_source"].strip()
            ):
                grade = "C"
                input_issues.append("unverified_pre_race_input")
            reasons.update(input_issues)
            if not chronology:
                # C は履歴由来の特徴量調査に使い、時点不明のオッズ・外部マスター・DLは除く。
                for key in (*SNAPSHOT_FILES[1:], "popularity", "popularity_observed_at", "dl"):
                    row.pop(key, None)

            raw = results.get(rid, {"race_id": rid})
            runners = roster.get(rid)
            numbers = runners["馬番"].map(integer).dropna() if runners is not None and "馬番" in runners else None
            result, details = audited_result(day, start, raw, proof.get("result", {}), numbers)
            reasons.update(details)
            records[rid] = {
                "input": row, "input_grade": grade, "result": result,
                "input_issues": input_issues, "result_issues": details,
                "start_at": start.isoformat() if start else None,
                "observed_at": observed.isoformat() if observed is not None else None,
                "pop_observed_at": row.get("popularity_observed_at", row.get("observed_at")) if chronology else None,
            }
        return book, records, reasons


def audited_result(day, start, raw, proof, numbers):
    missing = []
    try:
        top, _ = result_values({**raw, "payouts": {}}, numbers)
    except ValueError:
        top = None
    if top is None:
        missing.append("missing_or_unmatched_finish")
    payouts = {}
    for kind in BET_TYPES:
        prices = raw.get("payouts", {}).get(kind)
        if top and prices:
            try:
                _, valid = result_values({**raw, "payouts": {kind: prices}}, numbers)
                payouts.update(valid)
            except ValueError:
                missing.append("invalid_payout_" + kind)
        else:
            missing.append("missing_payout_" + kind)

    grade = proof.get("evidence_grade", "A" if proof.get("finalized_at") else "C")
    if grade not in RESULT_GRADES:
        raise ValueError(f"{raw['race_id']}: 結果の証跡区分が不正です")
    row = {**proof, "race_id": raw["race_id"], "race_date": day.isoformat(),
           "finish_top3": top, "payouts": payouts}
    if not top or grade == "D":
        evidence = result_evidence({**row, "evidence_grade": "D"}, start)
    elif grade == "unverified" or (grade == "C" and not proof.get("jra_official_result")):
        evidence = {**result_evidence({**row, "evidence_grade": "D"}, start), "grade": "unverified",
                    "race_date": day.isoformat(), "source": proof.get("evidence_source")}
        missing.append("unverified_result_evidence")
    else:
        evidence = result_evidence({**row, "evidence_grade": grade}, start)
    return {"top": top, "payouts": payouts, "evidence": evidence}, missing


def day_features(inputs, day, item, builders):
    book, records, reasons = inputs.day(day, item)
    frames = {}
    for rid, record in records.items():
        if record["input_grade"] != "D":
            try:
                variants = load_snapshot(inputs.root, record["input"], day, reasons, builders, book=book)
            except (FileNotFoundError, RosterUnavailable) as exc:
                variants = None
                record["input_issues"].append(str(exc))
            if variants and set(variants) == builders.keys() | {"corrected_with_cache"}:
                frames[rid] = variants
            else:
                reasons["unusable_or_excluded_roster"] += 1
                record["input_issues"].append("unusable_or_excluded_roster")
                record["input_grade"] = "D"
        record["input_evidence_source"] = record.pop("input").get("input_evidence_source")
    return records, frames, reasons
