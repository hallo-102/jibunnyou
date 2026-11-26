"""比較用の結果証跡と発走前入力区分。"""

from datetime import date, datetime

from race_purchase_filter.evaluation_utils import JST


def timestamp(value):
    stamp = datetime.fromisoformat(value)
    if stamp.utcoffset() is None:
        raise ValueError("タイムゾーン付きの時刻が必要です")
    return stamp.astimezone(JST)


def snapshot_grade(row):
    grade = row.get("input_evidence_grade", "A")
    if grade not in ("A", "B", "C", "D"):
        raise ValueError(f"{row['race_id']}: 発走前入力の証跡区分が不正です")
    return grade


def result_evidence(row, start=None):
    """検証済みの一次資料を参照する証跡レコードを正規化する。"""
    rid = row["race_id"]
    finalized_at = row.get("finalized_at")
    grade = row.get("evidence_grade", "A" if finalized_at is not None else "D")
    if grade not in ("A", "B", "C", "D"):
        raise ValueError(f"{rid}: 結果の証跡グレードが不正です")
    if grade == "D":
        return {"grade": grade, "finalized_at": None, "race_date": None,
                "available_by_date": None, "source": None}

    day_text = row.get("race_date")
    day = date.fromisoformat(day_text) if day_text is not None else None
    if day is not None and start is not None and day != start.date():
        raise ValueError(f"{rid}: 結果の開催日が発走日と一致しません")
    upper = None
    source = row.get("evidence_source")
    stamp = None
    if grade == "A":
        if finalized_at is None:
            raise ValueError(f"{rid}: グレードAには確定時刻が必要です")
        stamp = timestamp(finalized_at)
        if (start is not None and stamp <= start) or (day is not None and stamp.date() < day):
            raise ValueError(f"{rid}: 発走後の確定時刻が必要です")
    else:
        if finalized_at is not None:
            raise ValueError(f"{rid}: 正確な確定時刻はグレードAに指定してください")
        if day is None or not isinstance(source, str) or not source.strip():
            raise ValueError(f"{rid}: 開催日と証跡の参照先が必要です")
        if grade == "B":
            upper = date.fromisoformat(row["available_by_date"])
            if upper < day:
                raise ValueError(f"{rid}: 利用可能日の上限が開催日より前です")
        else:
            official = row.get("jra_official_result", {})
            claimed_payouts = row.get("payouts", {})
            official_payouts = official.get("payouts", {})
            matches = (
                official.get("race_id") == rid
                and official.get("race_date") == day.isoformat()
                and official.get("finish_top3") == row.get("finish_top3")
                and all(kind in official_payouts and official_payouts[kind] == prices
                        for kind, prices in claimed_payouts.items())
            )
            if not matches:
                raise ValueError(f"{rid}: JRA公式確定結果との照合に失敗しました")
    return {
        "grade": grade,
        "finalized_at": stamp.isoformat() if stamp is not None else None,
        "race_date": day.isoformat() if day is not None else None,
        "available_by_date": upper.isoformat() if upper is not None else None,
        "source": source,
    }


def available_by_cutoff(evidence, cutoff):
    """締切日はJST。日単位の証跡は締切日と重なる場合に除外する。"""
    if evidence["grade"] == "A":
        return timestamp(evidence["finalized_at"]).date() <= cutoff
    upper = evidence["available_by_date"]
    return upper is not None and date.fromisoformat(upper) < cutoff
