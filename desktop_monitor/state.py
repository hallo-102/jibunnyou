"""NASの状態を収集し、完了・待機・異常を根拠のある表示へ変換する。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

JST = timezone(timedelta(hours=9))
DEFAULT_ROOT = r"\\192.168.1.92\docker\keiba_yosou_2026_nas"
DRIVE_URL = "https://drive.google.com/drive/folders/1OW7GR-ajUg4fllWGNnL6CMMvhrheCi3j"


def read_json(path: Path, *, required: bool = False) -> dict:
    # 書込途中のJSONや権限エラーは、未実行と区別して呼出側へ通知する。
    try:
        with path.open("rb") as handle:
            raw = handle.read(4 * 1024 * 1024 + 1)
        if len(raw) > 4 * 1024 * 1024:
            raise ValueError(f"状態ファイルが大きすぎます: {path.name}")
        value = json.loads(raw.decode("utf-8-sig"))
        if not isinstance(value, dict):
            raise ValueError(f"状態がJSONオブジェクトではありません: {path.name}")
        return value
    except FileNotFoundError:
        if required:
            raise
        return {}


def tail(path: Path) -> str:
    # 大きなログ全体を転送せず、末尾の64KBだけを取得する。
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - 65536))
            raw = handle.read(65536)
        text = raw.decode("utf-8", errors="replace")
        return text.split("\n", 1)[-1] if size > 65536 else text
    except FileNotFoundError:
        return "ログはまだ作成されていません。"


def collect(root: str, day: str) -> dict:
    base = Path(root)
    if str(base).startswith("\\\\"):
        # NAS側の独立観測はファイル名が不変でないため、SMBの旧内容キャッシュを再利用しない。
        files = sorted((base / "data/output/continuous_improvement/live").glob("snapshot_*.json"))
        if files:
            observed = read_json(files[-1], required=True)
            published = timestamp(observed.get("snapshot_published_at"))
            if observed.get("day") == day:
                if published is None or (datetime.now(JST) - published).total_seconds() > 90:
                    raise OSError("NASの画面用観測が90秒以上更新されていません。前回値を表示します。")
                observed["collected_at"] = datetime.now(JST).isoformat()
                return observed
    status_dir = base / "data/output/status"
    loop = read_json(base / "data/output/continuous_improvement/status.json", required=True)
    return {"loop": loop, "daily": read_json(status_dir / f"STATUS_{day}.json"),
            "ready": read_json(status_dir / f"READY_{day}.json"),
            "daily_log": tail(base / "logs" / f"daily_{day}.log"),
            "scheduler_log": tail(base / "logs/scheduler.log"),
            "collected_at": datetime.now(JST).isoformat(), "day": day}


def timestamp(raw: str | None) -> datetime | None:
    try:
        value = datetime.fromisoformat(raw or "")
        return value.replace(tzinfo=JST) if value.tzinfo is None else value.astimezone(JST)
    except (ValueError, TypeError):
        return None


def clock(raw: str | None) -> str:
    value = timestamp(raw)
    return value.strftime("%m/%d %H:%M:%S") if value else "未記録"


def present(snapshot: dict, now: datetime) -> dict:
    # 過去日を選んだ場合も、NASのheartbeatは現在時刻を基準に評価する。
    loop, daily, ready = snapshot["loop"], snapshot["daily"], snapshot["ready"]
    day = snapshot["day"]
    today = now.strftime("%Y%m%d") == day
    record = loop.get("days", {}).get(day, {})
    checked = timestamp(loop.get("checked_at"))
    age = (now - checked).total_seconds() if checked else None
    fresh = age is not None and -60 <= age <= 300
    no_race = daily.get("status") == "NO_RACE"
    morning_done = bool(ready and ready.get("status") == "READY") or bool(record.get("frozen"))
    stages = []

    def add(title: str, when: str, status: str, detail: str):
        stages.append({"title": title, "when": when, "status": status, "detail": detail})

    add("監視ループ", "06:00〜", "done" if fresh else "warn",
        f"NAS状態更新 {clock(loop.get('checked_at'))}" + ("" if fresh else " ／ 更新間隔を超過・長時間処理中の可能性"))
    morning = "done" if morning_done else "active" if daily.get("status") == "RUNNING" else "error" if daily.get("status") == "FAILED" else "skip" if no_race else "wait"
    morning_detail = "予想・品質確認完了（READY）" if morning_done else "非開催日" if no_race else daily.get("error") or "開催判定 → 情報収集 → 品質確認 → 本番予想"
    # 朝のログに記録されたステージ開始・終了だけを補助表示する。
    if morning == "active":
        stage_lines = [line for line in snapshot.get("daily_log", "").splitlines() if "stage_" in line]
        morning_detail = stage_lines[-1][-180:] if stage_lines else "朝処理が実行中です。詳細はログ欄を確認してください。"
    add("情報収集・本番予想", "06:45〜", morning, str(morning_detail))
    add("発走前の固定保存", "予想完成後", "done" if record.get("frozen") else "error" if record.get("freeze_error") else "skip" if no_race else "wait",
        f"固定時刻 {clock(record.get('frozen_at'))}" if record.get("frozen") else record.get("freeze_error") or "予想・特徴量・重み・買い目を保存")
    shadow_status = "done" if record.get("shadow") else "warn" if record.get("shadow_missed") else "error" if record.get("shadow_error") else "active" if record.get("shadow_attempted") else "skip" if no_race else "wait"
    add("Shadow予想・別枠比較", "最初の発走前", shadow_status,
        "Shadow予想を保存済み" if record.get("shadow") else "発走前に間に合わず、事前実績から除外" if record.get("shadow_missed") else record.get("shadow_error") or "同じ固定入力で改善候補を比較")
    add("結果取得・全レース回顧", "18:00以降", "done" if record.get("review") else "error" if record.get("error") else "skip" if no_race else "wait",
        f"回顧完了 {clock(record.get('reviewed_at'))}" if record.get("review") else record.get("error") or "確定結果・払戻を照合し、回収率と特徴量を評価")
    add("マスタ更新", "回顧後", "done" if record.get("masters_updated") else "error" if record.get("master_update_error") else "skip" if no_race else "wait",
        f"更新完了 {clock(record.get('masters_updated'))}" if record.get("masters_updated") else record.get("master_update_error") or "検査に通ったマスタを次回へ引き継ぐ")
    # 一度成功した工程の古いerrorキーは、現在の障害として数えない。
    upload_status = "done" if record.get("uploaded") == "post" else "error" if record.get("upload_error") and (not record.get("uploaded") or record.get("review")) else "active" if record.get("uploaded") == "pre" else "skip" if no_race else "wait"
    upload_detail = "結果まで保存済み" if record.get("uploaded") == "post" else record.get("upload_error") if upload_status == "error" else "発走前の記録を保存済み／夜の結果保存待ち" if record.get("uploaded") == "pre" else "発走前と回顧後に保存・内容照合"
    add("Google Drive保存", "朝・夜", upload_status, upload_detail)
    issues = [f"{s['title']}：{s['detail']}" for s in stages if s["status"] in {"error", "warn"}]
    if not fresh:
        headline, tone = ("朝処理の実行中記録あり・ループ状態の更新待ち" if daily.get("status") == "RUNNING" else "NASの状態更新を確認してください"), "warn"
    elif any(s["status"] == "error" for s in stages):
        headline, tone = "要確認の工程があります", "error"
    elif no_race:
        headline, tone = "非開催日・朝の予想はスキップ", "done"
    elif record.get("review") and record.get("masters_updated") and record.get("uploaded") == "post":
        headline, tone = "本日の回顧・保存まで完了", "done"
    elif record.get("shadow_missed"):
        headline, tone = "Shadowは事前実績から除外", "warn"
    elif record.get("shadow"):
        headline, tone = "事前記録・Shadow保存済み", "done"
    elif record.get("frozen"):
        headline, tone = "事前記録を固定済み", "active"
    elif daily.get("status") == "RUNNING":
        headline, tone = "朝の予想処理が動いています", "active"
    elif morning_done:
        headline, tone = "本番予想完了・固定保存待ち", "active"
    elif today and now.hour * 60 + now.minute < 405:
        headline, tone = "06:45の朝処理を待っています", "wait"
    elif today and now.hour * 60 + now.minute >= 410:
        headline, tone = "朝処理の開始記録がありません", "warn"
        issues.append("06:45を過ぎています。スケジューラのログを確認してください。")
    else:
        headline, tone = "処理記録を待っています", "wait"
    if not today and not daily and not record:
        headline, tone = "この日の処理記録はありません", "wait"
    if record.get("review") and record.get("masters_updated") and record.get("uploaded") == "post":
        next_step = "次の開催日へ。改善候補は実績条件を満たした非開催日に探索します。"
    elif no_race:
        next_step = "条件を満たせば07:00〜12:00に隔離探索。次の開催日を待ちます。"
    elif today and now.hour >= 22:
        next_step = "新規工程の開始を停止。未完了分は翌稼働日に再開します。"
    elif record.get("shadow") and not record.get("review"):
        next_step = "18:00以降、確定結果・払戻を取得して答え合わせします。"
    elif record.get("frozen") and not record.get("shadow"):
        next_step = "最初の発走前にShadow予想を完了します。間に合わない場合は実績から除外。"
    else:
        next_step = "06:45朝処理 → READY → 発走前の固定保存 → Shadow比較"
    return {"stages": stages, "headline": headline, "tone": tone, "issues": issues,
            "next": next_step, "heartbeat": clock(loop.get("checked_at")),
            "optimizer": loop.get("optimizer", {}), "record": record}
