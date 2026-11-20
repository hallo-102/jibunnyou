"""十分な前向き実績が集まった非開催日に、既存最適化を隔離実行する。"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from .archive import JST, sha256, write_json
from .history import aggregate


def activate_candidate(base: Path, sandbox: Path) -> dict | None:
    decisions = sorted((sandbox / "data/output/weight_adoption").glob("decision_*.json"))
    if not decisions:
        return None
    decision = json.loads(decisions[-1].read_text(encoding="utf-8"))
    if decision.get("validation_decision") != "adopted" or decision.get("candidate_roundtrip_match") is not True:
        return None
    candidate = Path(decision["candidate_weight_file"]).resolve()
    if not candidate.is_relative_to(sandbox.resolve()) or sha256(candidate) != decision["candidate_weight_sha256"]:
        raise ValueError("Shadow候補の保存先またはSHAが不正です")
    # 本番互換の重み読込だけを別プロセスで再利用し、実効SHAと形状を再検証する。
    code = "import json,sys;from tokutyouryou_keisann.baseline import load_effective_weights_file,normalized_weights_sha256;w=load_effective_weights_file(sys.argv[1]);print(json.dumps({'sha':normalized_weights_sha256(w),'groups':len(w),'elements':sum(len(v) for v in w.values())}))"
    result = subprocess.run([sys.executable, "-c", code, str(candidate)], cwd=sandbox, check=True,
                            capture_output=True, text=True, encoding="utf-8")
    effective = json.loads(result.stdout.strip().splitlines()[-1])
    if effective["sha"] != decision["reloaded_candidate_normalized_sha256"]:
        raise ValueError("Shadow候補の実効重みSHAが不一致です")
    # 再試行で学習用sandboxが更新されても、採用済みShadowの実体は変わらないよう複製する。
    active_source = sandbox.parent / ("active_" + sha256(candidate)[:16])
    temporary = Path(tempfile.mkdtemp(prefix=".active-", dir=sandbox.parent))
    shutil.copytree(sandbox, temporary, dirs_exist_ok=True, ignore=shutil.ignore_patterns("data", "__pycache__"))
    name = f"best_feature_weights_{datetime.now(JST):%Y%m%d}.py"
    isolated_weight = temporary / "yosou_py" / name
    shutil.copy2(candidate, isolated_weight)
    config_path = temporary / "config/production_model.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config.update(weight_file=f"yosou_py/{name}", model_name=candidate.stem,
                  file_sha256=sha256(isolated_weight), effective_sha256=effective["sha"],
                  expected_group_count=effective["groups"], expected_element_count=effective["elements"],
                  activated_at=datetime.now(JST).isoformat(), activation_reason="隔離Shadow専用候補。本番NAS・本番GitHub設定へ反映しない")
    write_json(config_path, config)
    if active_source.exists():
        if not temporary.resolve().is_relative_to(sandbox.parent.resolve()):
            raise ValueError("一時フォルダが隔離領域を逸脱しています")
        shutil.rmtree(temporary)
    else:
        temporary.rename(active_source)
    active = {"source": str(active_source), "candidate_sha256": sha256(candidate), "effective_sha256": effective["sha"],
              "decision": str(decisions[-1]), "activated_at": datetime.now(JST).isoformat(), "production_promoted": False}
    write_json(base / "active_shadow.json", active)
    return active


def maybe_run(root: Path, now: datetime) -> dict:
    base = root / "data/output/continuous_improvement"
    history = aggregate(base)
    shadow_models = [m for m in history["models"] if m["role"] == "shadow"]
    latest = max(shadow_models, key=lambda m: m["days"][-1]) if shadow_models else None
    ready = latest is not None and latest["prediction_sample_ready"] and latest["profit_sample_ready"]
    status = {"status": "WAITING_SAMPLE", "required": {"days": 8, "races": 300, "trifecta_races": 50},
              "auto_promote": False, "objective": "既存TOP5探索で候補を生成し、次回以降の固定買い目回収率で検証"}
    if not ready:
        write_json(base / "optimizer_status.json", status)
        return status
    day = now.strftime("%Y%m%d")
    daily_path = root / "data/output/status" / f"STATUS_{day}.json"
    if not daily_path.exists() or json.loads(daily_path.read_text(encoding="utf-8")).get("status") != "NO_RACE" or not 7 <= now.hour < 12:
        return {**status, "status": "WAITING_NON_RACEDAY"}
    source = Path(__file__).resolve().parents[1]
    identity = hashlib.sha256((sha256(root / "data/master/racedata_results.xlsx") + sha256(source / "tokutyouryou_keisann/runner.py")).encode()).hexdigest()[:16]
    job = base / "optimization" / identity
    if (job / "completed.json").exists():
        return json.loads((job / "completed.json").read_text(encoding="utf-8"))
    sandbox = job / "source"
    sandbox.mkdir(parents=True, exist_ok=True)
    attempted = job / f"attempt_{day}.json"
    if attempted.exists():
        return {**status, "status": "WAITING_NEXT_DAY", "job": str(job)}
    for path in source.glob("*.py"):
        shutil.copy2(path, sandbox / path.name)
    for folder in ("config", "yosou_py", "tokutyouryou_keisann"):
        shutil.copytree(source / folder, sandbox / folder, dirs_exist_ok=True)
    (sandbox / "data/master").mkdir(parents=True, exist_ok=True)
    for path in (root / "data/master").glob("*.xlsx"):
        shutil.copy2(path, sandbox / "data/master" / path.name)
    training = source / "training_inputs"
    if not training.is_dir():
        raise ValueError("隔離最適化用の既存過去入力が未配置です")
    env = os.environ.copy()
    env.update(KEIBA_ROOT=str(sandbox), KEIBA_EXCEL_DIR=str(training), KEIBA_PY_DIR=str(sandbox / "yosou_py"))
    status.update(status="RUNNING", job=str(job), started_at=now.isoformat())
    write_json(attempted, status)
    write_json(base / "optimizer_status.json", status)
    command = [sys.executable, "-m", "tokutyouryou_keisann", "--trial-iterations", "100"]
    # 最大3時間。非開催日の午前に開始し、22:30のNAS停止に跨がない。
    with (job / "optimizer.log").open("w", encoding="utf-8") as log:
        result = subprocess.run(command, cwd=sandbox, env=env, stdin=subprocess.DEVNULL,
                                stdout=log, stderr=subprocess.STDOUT, timeout=10800)
    status.update(status="COMPLETED" if result.returncode == 0 else "FAILED", exit_code=result.returncode,
                  completed_at=datetime.now(JST).isoformat(), production_promoted=False)
    if result.returncode == 0:
        status["active_shadow"] = activate_candidate(base, sandbox)
        write_json(job / "completed.json", status)
    write_json(base / "optimizer_status.json", status)
    return status
