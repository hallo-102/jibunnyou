"""11/14固定原本から回顧・同期を再開する。朝処理や予想計算は起動しない。"""
import json
import logging
from pathlib import Path
import sys
from datetime import datetime

sys.path.insert(0, "/opt/jra-loop")
from continuous_improvement.archive import JST, sha256, verify_archive, write_json
from continuous_improvement.shadow import verify_shadow
from continuous_improvement.service import postprocess, update_masters, upload, run

logging.basicConfig(level=logging.INFO)
root = Path("/workspace")
base = root / "data/output/continuous_improvement"
day = base / "prospective/20261114"
pre = day / "pre"
audit = day / "recovery_audit.json"
try:
    manifest = verify_archive(pre)
    verify_shadow(day / "shadow_pre_7a47012b4a16")
    official = json.loads((day / "official_verification/verification.json").read_text())
    assert official["verified_races"] == official["expected_races"] == 36
    # 本番コード・重み・設定・予想を前後比較し、変更範囲が回顧だけであることを残す。
    protected = list((root / "app").glob("*.py")) + list((root / "yosou_py").glob("*.py"))
    protected += list((root / "config").glob("*model.json"))
    protected += [pre / "prediction.xlsx", day / "shadow_pre_7a47012b4a16/prediction.xlsx",
                  root / "data/output/馬の競走成績_with_feat_20261114.xlsx"]
    before = {str(p): sha256(p) for p in protected if p.exists()}
    write_json(audit, {"phase": "STARTED", "protected_before": before})
    output = postprocess(root, pre, collect_results=False)
    update_masters(root, pre)
    after = {p: sha256(Path(p)) for p in before}
    assert before == after, "本番保護ファイルのSHA変化"
    verify_archive(pre)
    verify_shadow(day / "shadow_pre_7a47012b4a16")
    history_before = json.loads((base / "history.json").read_text())
    from continuous_improvement.history import aggregate
    assert aggregate(base) == history_before, "再集計の冪等性不一致"
    write_json(audit, {"phase": "VERIFIED", "protected_before": before, "protected_after": after,
                      "official_races": 36, "history_idempotent": True, "output": str(output)})
    upload(root, day)
    state_path = base / "status.json"
    state = json.loads(state_path.read_text())
    record = state["days"]["20261114"]
    now = datetime.now(JST).isoformat()
    record.update(review=str(output), reviewed_at=now, masters_updated=now, uploaded="post",
                  official_verified_races=36, recovered_at=now)
    for field in ("error", "retry_at", "upload_error", "master_update_error"):
        record.pop(field, None)
    state["checked_at"] = now
    state["status_published_at"] = now
    write_json(state_path, state)
    common = ["--config", str(root / "config/rclone/rclone.conf"), "--drive-root-folder-id", "1kqYzK2jZjLQThnMu2YtpiTVQ5VZv3qDH"]
    for name in ("history.json", "status.json"):
        run(["rclone", "copyto", str(base / name), "gdrive:continuous_improvement/" + name, *common], root, timeout=300)
    run(["rclone", "check", str(base), "gdrive:continuous_improvement", "--include", "/history.json",
         "--include", "/status.json", "--exclude", "**", "--one-way", "--download", *common], root, timeout=300)
    print("RECOVERY_COMPLETED", flush=True)
except Exception as exc:
    # 成功と偽装せず、固定原本と取得キャッシュを保持して同じスクリプトから再開する。
    state_path = base / "status.json"
    state = json.loads(state_path.read_text())
    state["days"]["20261114"]["recovery_error"] = str(exc)
    write_json(state_path, state)
    raise
