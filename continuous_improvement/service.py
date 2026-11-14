"""朝処理へ干渉せずREADYを固定し、夕方以降に未完了日を順に再開する。"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

from .archive import JST, freeze, sha256, verify_archive, write_json
from .review import load_results, review, metrics, settle_existing
from .history import aggregate
from .optimization import maybe_run

LOG = logging.getLogger(__name__)


def run(command: list[str], root: Path, *, env: dict | None = None, timeout: int = 3600) -> str:
    # 子処理には非対話引数を渡し、NAS停止前に終了できる上限を設ける。
    process = subprocess.Popen(command, cwd=root, env=env, stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, encoding="utf-8", errors="replace", start_new_session=os.name != "nt")
    try:
        stdout, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        # NASではChromiumを含む子プロセス群も終了させ、再試行へ残さない。
        if os.name != "nt":
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            if os.name != "nt":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
            process.communicate()
        raise
    result = subprocess.CompletedProcess(command, process.returncode, stdout)
    # 実効重みの全辞書はruntime.jsonだけへ保存し、通常ログは短く保つ。
    output = result.stdout.split("RUNTIME_METADATA=", 1)[0] if "RUNTIME_METADATA=" in result.stdout else result.stdout[-6000:]
    LOG.info("command=%s exit=%s output=%s", command[1], result.returncode, output)
    if result.returncode:
        raise RuntimeError(f"工程失敗: {Path(command[1]).name}, exit={result.returncode}")
    return result.stdout


def runtime(root: Path) -> dict:
    raw = run([sys.executable, str(Path(__file__).with_name("runtime_metadata.py")),
               str(root / "app"), str(root / "yosou_py")], root)
    return json.loads(raw.rsplit("RUNTIME_METADATA=", 1)[1].strip())


def recover_morning(root: Path, now: datetime, record: dict) -> None:
    day = now.strftime("%Y%m%d")
    attempt = root / "data/output/status" / f"SCHEDULED_ATTEMPT_{day}.json"
    if not attempt.exists() or json.loads(attempt.read_text(encoding="utf-8")).get("status") != "FAILED":
        return
    journal = root / "data/output/continuous_improvement" / f"morning_recovery_{day}.json"
    if journal.exists():
        saved = json.loads(journal.read_text(encoding="utf-8"))
        if int(saved.get("morning_retries", 0)) > int(record.get("morning_retries", 0)):
            record.update(saved)
    if now.hour >= 9 or int(record.get("morning_retries", 0)) >= 3:
        return
    retry_at = record.get("morning_retry_at")
    if retry_at and now < datetime.fromisoformat(retry_at):
        return
    # 既存スケジューラーが失敗終了した場合だけ、保存済み入力から最大3回再開する。
    resume = "odds"
    if (root / "data/ozzu_csv" / f"OZZU_{day}.csv").exists():
        resume = "pre-race-info"
        if (root / "data/input" / f"馬の競走成績_{day}.xlsx").exists():
            resume = "prediction"
    record["morning_retries"] = int(record.get("morning_retries", 0)) + 1
    record["morning_retry_at"] = (now + timedelta(minutes=15)).isoformat()
    write_json(journal, record)
    try:
        run([sys.executable, "-m", "scripts.run_daily_pipeline", day,
             "--resume-from", resume, "--upload-mode", "real"], root, timeout=3600)
        record["morning_recovery"] = "COMPLETED"
    except Exception as exc:
        record["morning_recovery"] = "FAILED"
        record["morning_error"] = str(exc)
    write_json(journal, record)


def postprocess(root: Path, pre: Path, *, collect_results: bool = True) -> Path:
    manifest = verify_archive(pre)
    day = manifest["day"]
    work = pre.parent / "work"
    work.mkdir(exist_ok=True)
    results = work / "racedata_results.xlsx"
    if not results.exists():
        shutil.copy2(root / "data/master/racedata_results.xlsx", results)
    if collect_results:
        env = os.environ.copy()
        env.update(KEIBA_BASE_DIR=str(root), KEIBA_MASTER_DIR=str(work), KEIBA_RACEDAY=day)
        import pandas as pd
        entries = pd.read_excel(pre / "input.xlsx", sheet_name="今走レース情報")
        ids_path = work / "expected_race_ids.json"
        ids_path.write_text(json.dumps(sorted(entries["レースID"].astype(str).unique().tolist())), encoding="utf-8")
        scraper = Path(__file__).resolve().parents[1] / "etc_py/10_kekka_scraper_20260102.py"
        run([sys.executable, str(scraper), day, "--race-ids-file", str(ids_path)], root, env=env, timeout=3600)
    # reviewが全出走馬・全レース・払戻を検証してからマスタ更新へ進める。
    output = review(pre, results)
    shadows = [p for p in pre.parent.glob("shadow_pre*") if (p / "manifest.json").exists()]
    shadow = max(shadows, key=lambda p: json.loads((p / "manifest.json").read_text(encoding="utf-8"))["saved_at"]) if shadows else pre.parent / "shadow_pre"
    if shadow.is_dir():
        shadow_output = review(pre, results, shadow=shadow)
        production_report = json.loads((output / "review.json").read_text(encoding="utf-8"))
        shadow_report = json.loads((shadow_output / "review.json").read_text(encoding="utf-8"))
        import pandas as pd
        production_rows = pd.read_csv(output / "race_horse_results.csv", dtype={"rid_str": str})
        shadow_rows = pd.read_csv(shadow_output / "race_horse_results.csv", dtype={"rid_str": str})
        production_universe = {rid: set(group["馬番"]) for rid,group in production_rows.groupby("rid_str")}
        shadow_universe = {rid: set(group["馬番"]) for rid,group in shadow_rows.groupby("rid_str")}
        common_races = sorted(rid for rid in production_universe if rid in shadow_universe and production_universe[rid] == shadow_universe[rid])
        _, payouts = load_results(results, day)
        write_json(pre.parent / "comparison.json", {"day": day, "mode": manifest["mode"],
                   "production": production_report["overall"], "shadow": shadow_report["overall"],
                   "production_runtime_id": production_report["runtime_id"],
                   "shadow_runtime_id": shadow_report["runtime_id"],
                   "same_prediction_universe": production_universe == shadow_universe,
                   "common_races": common_races,
                   "common_production": metrics(production_rows.loc[production_rows["rid_str"].isin(common_races)], payouts),
                   "common_shadow": metrics(shadow_rows.loc[shadow_rows["rid_str"].isin(common_races)], payouts),
                   "common_production_tickets": settle_existing(pre / "prediction.xlsx", payouts, load_results(results, day)[0], set(common_races)),
                   "common_shadow_tickets": settle_existing(shadow / "prediction.xlsx", payouts, load_results(results, day)[0], set(common_races)),
                   "auto_promote": False, "legacy_forward_generation": None})
    aggregate(root / "data/output/continuous_improvement")
    return output


def update_masters(root: Path, pre: Path) -> None:
    # 過去検証から正式マスタを書き換えず、確定済み前向き日だけ更新する。
    import pandas as pd
    manifest = verify_archive(pre)
    if manifest["mode"] != "prospective":
        raise ValueError("過去検証から本番マスタは更新できません")
    day = manifest["day"]
    now = datetime.now(JST)
    if 6 <= now.hour < 10:
        raise ValueError("朝予想とマスタ更新の競合を避け、10:00以降に再開します")
    attempt = root / "data/output/status" / f"SCHEDULED_ATTEMPT_{now:%Y%m%d}.json"
    if attempt.exists() and json.loads(attempt.read_text(encoding="utf-8")).get("status") == "RUNNING":
        raise ValueError("既存朝処理が実行中のためマスタ更新を延期します")
    master = root / "data/master"
    with pd.ExcelFile(master / "racedata_results.xlsx") as excel:
        dates = [s for s in excel.sheet_names if len(s) == 8 and s.isdigit()]
    if dates and max(dates) >= day:
        level_dates = pd.read_excel(master / "race_levels.xlsx", sheet_name="races", usecols=["date"])["date"]
        latest_level = pd.to_datetime(level_dates.astype(str), errors="coerce").max()
        if pd.notna(latest_level) and latest_level.strftime("%Y%m%d") >= day:
            return
    work = pre.parent / "work"
    staged = work / "race_levels.xlsx"
    run([sys.executable, str(root / "maintenance/00_Export_To_Excel_4.py"),
         "--excel", str(work / "racedata_results.xlsx"), "--out", str(staged)], root, timeout=3600)
    from .bootstrap import publish_pair
    publish_pair(root, work / "racedata_results.xlsx", staged, day, allow_same_day=True)


def upload(root: Path, day_dir: Path) -> None:
    # 既存rclone認証と配信先を再利用し、固定アーカイブ専用サブフォルダへ保存する。
    common = ["--config", str(root / "config/rclone/rclone.conf"),
              "--drive-root-folder-id", "1kqYzK2jZjLQThnMu2YtpiTVQ5VZv3qDH"]
    target = f"gdrive:continuous_improvement/{day_dir.parent.name}/{day_dir.name}"
    exclusions = ["--exclude", "work/**", "--exclude", ".*/**"]
    run(["rclone", "copy", str(day_dir), target, *exclusions, *common], root, timeout=3600)
    run(["rclone", "check", str(day_dir), target, *exclusions, "--one-way", "--download", *common], root, timeout=3600)


def tick(root: Path, now: datetime | None = None) -> dict:
    now = now or datetime.now(JST)
    today = now.strftime("%Y%m%d")
    base = root / "data/output/continuous_improvement"
    status_path = base / "status.json"
    state = json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {"days": {}}
    state["checked_at"] = now.isoformat()
    if now.hour >= 22 or now.hour < 6:
        # 22:30停止前に新しい長時間工程を起動せず、翌朝から継続する。
        write_json(status_path, state)
        return state
    ready = root / "data/output/status" / f"READY_{today}.json"
    day_state = state["days"].setdefault(today, {})
    if not ready.exists():
        recover_morning(root, now, day_state)
        write_json(status_path, state)
    if ready.exists() and not day_state.get("frozen"):
        try:
            pre = freeze(root, today, runtime(root), now=now)
            day_state.update(frozen=str(pre), frozen_at=now.isoformat())
            write_json(status_path, state)
        except Exception as exc:
            day_state["freeze_error"] = str(exc)
            LOG.exception("freeze_failed day=%s", today)
    # 発走前アーカイブは夕方まで待たずDriveへ保存する。
    if day_state.get("frozen") and not day_state.get("uploaded"):
        try:
            upload(root, Path(day_state["frozen"]).parent)
            day_state["uploaded"] = "pre"
        except Exception as exc:
            day_state["upload_error"] = str(exc)
        write_json(status_path, state)
    shadow_retry = day_state.get("shadow_retry_at")
    if day_state.get("frozen") and not day_state.get("shadow") and not day_state.get("shadow_missed") and (not shadow_retry or now >= datetime.fromisoformat(shadow_retry)):
        day_state["shadow_attempted"] = now.isoformat()
        write_json(status_path, state)
        try:
            env = os.environ.copy()
            active_path = base / "active_shadow.json"
            if active_path.exists():
                active = json.loads(active_path.read_text(encoding="utf-8"))
                source = Path(active["source"]).resolve()
                if not source.is_relative_to((base / "optimization").resolve()):
                    raise ValueError("隔離領域外のShadow候補は使えません")
                env["KEIBA_SHADOW_SOURCE_DIR"] = str(source)
            result = run([sys.executable, "-m", "continuous_improvement.shadow", day_state["frozen"]], root, env=env, timeout=3600)
            day_state["shadow"] = result.strip().splitlines()[-1]
            upload(root, Path(day_state["frozen"]).parent)
        except Exception as exc:
            day_state["shadow_error"] = str(exc)
            if "発走前" in str(exc):
                day_state["shadow_missed"] = True
            day_state["shadow_retry_at"] = (datetime.now(JST) + timedelta(minutes=30)).isoformat()
        write_json(status_path, state)
    for pre in sorted((base / "prospective").glob("*/pre")):
        day = pre.parent.name
        record = state["days"].setdefault(day, {})
        # 当日は全レース終了後の18:00以降。翌稼働日の未完了も処理する。
        if day == today and now.hour < 18:
            continue
        if datetime.now(JST).hour >= 21:
            break
        if not record.get("review"):
            retry_at = record.get("retry_at")
            if retry_at and now < datetime.fromisoformat(retry_at):
                continue
            try:
                out = postprocess(root, pre)
                record.update(review=str(out), reviewed_at=datetime.now(JST).isoformat())
                record.pop("error", None)
                record.pop("retry_at", None)
            except Exception as exc:
                record.update(error=str(exc), retry_at=(datetime.now(JST) + timedelta(minutes=30)).isoformat())
                LOG.exception("postprocess_failed day=%s", day)
            write_json(status_path, state)
        if record.get("review") and not record.get("masters_updated"):
            try:
                update_masters(root, pre)
                record["masters_updated"] = datetime.now(JST).isoformat()
                record.pop("master_update_error", None)
            except Exception as exc:
                record["master_update_error"] = str(exc)
                LOG.exception("master_update_failed day=%s", day)
            write_json(status_path, state)
        if not record.get("uploaded"):
            try:
                upload(root, pre.parent)
                record["uploaded"] = "post" if record.get("review") else "pre"
            except Exception as exc:
                record["upload_error"] = str(exc)
                LOG.exception("upload_failed day=%s", day)
            write_json(status_path, state)
        elif record.get("review") and record.get("uploaded") == "pre":
            try:
                upload(root, pre.parent)
                record["uploaded"] = "post"
                record.pop("upload_error", None)
            except Exception as exc:
                record["upload_error"] = str(exc)
            write_json(status_path, state)
    write_json(status_path, state)
    try:
        state["optimizer"] = maybe_run(root, now)
    except Exception as exc:
        state["optimizer"] = {"status": "FAILED", "error": str(exc), "production_promoted": False}
    write_json(status_path, state)
    last_published = state.get("status_published_at")
    if not last_published or now - datetime.fromisoformat(last_published) >= timedelta(minutes=30):
        try:
            # 前向き集計履歴も日別成果物とは別の既存ルートへ同期する。
            history = base / "history.json"
            if history.exists():
                run(["rclone", "copyto", str(history), "gdrive:continuous_improvement/history.json",
                     "--config", str(root / "config/rclone/rclone.conf"), "--drive-root-folder-id",
                     "1kqYzK2jZjLQThnMu2YtpiTVQ5VZv3qDH"], root, timeout=300)
            run(["rclone", "copyto", str(status_path), "gdrive:continuous_improvement/status.json",
                 "--config", str(root / "config/rclone/rclone.conf"), "--drive-root-folder-id",
                 "1kqYzK2jZjLQThnMu2YtpiTVQ5VZv3qDH"], root, timeout=300)
            state["status_published_at"] = now.isoformat()
        except Exception as exc:
            state["status_upload_error"] = str(exc)
        write_json(status_path, state)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description="既存NAS予想の固定保存・全レース回顧")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--backfill", help="過去日通し試験。前向き実績から分離して保存")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.backfill:
        pre = freeze(args.root, args.backfill, runtime(args.root), retrospective=True)
        print(postprocess(args.root, pre, collect_results=False))
        return
    if args.once:
        print(json.dumps(tick(args.root), ensure_ascii=False))
        return
    while True:
        try:
            tick(args.root)
        except Exception:
            LOG.exception("lifecycle_tick_failed")
        time.sleep(60)


if __name__ == "__main__":
    main()
