"""管理者が導入した固定host helper。PR #17の実装を直接利用する。"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys

from .files import atomic_json
from .schema import Rejected


def sha(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def docker_json(args, env):
    return json.loads(subprocess.check_output(["docker", *args], env=env, stderr=subprocess.DEVNULL, timeout=30))


def audit(production, day, env):
    """ソース・モデル・非秘密設定・指定原本と稼働containerを読むだけ。"""
    files = {}
    for directory in ("app", "yosou_py", "scripts", "maintenance"):
        root = production / directory
        for path in sorted(root.rglob("*.py")):
            if path.is_symlink() or not path.resolve().is_relative_to(production):
                raise Rejected("UNSAFE_PROTECTED_SOURCE")
            files[path.relative_to(production).as_posix()] = sha(path)
    names = ["compose.yaml", "Dockerfile", "requirements.txt", "config/production_model.json", "config/shadow_model.json", "config/forward_validation.json",
             f"data/input/馬の競走成績_{day}.xlsx", "data/master/race_levels.xlsx", "data/master/場所_馬場_タイム.xlsx", "data/master/racedata_results.xlsx",
             f"data/ozzu_csv/OZZU_{day}.csv",
             f"data/output/馬の競走成績_with_feat_{day}.xlsx", f"data/output/status/READY_{day}.json", f"data/output/status/STATUS_{day}.json"]
    for name in names:
        path = production / name
        if path.is_symlink() or not path.resolve().is_relative_to(production):
            raise Rejected("UNSAFE_PROTECTED_SOURCE")
        files[name] = sha(path) if path.is_file() else None
    ids = subprocess.check_output(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project.working_dir={production}"], env=env, stderr=subprocess.DEVNULL, timeout=30, text=True).split()
    if not ids:
        raise Rejected("PRODUCTION_CONTAINERS_NOT_IDENTIFIED")
    # Env・秘密設定ファイル・health logを取得しない。
    template = '{"id":{{json .Id}},"image":{{json .Image}},"mounts":{{json .Mounts}},"restart":{{json .HostConfig.RestartPolicy}},"user":{{json .Config.User}},"running":{{json .State.Running}}}'
    containers = {identifier: docker_json(["inspect", "--format", template, identifier], env) for identifier in sorted(ids)}
    return {"files": files, "containers": containers}


def validate_compose(config, checked, source, limits):
    """レビュー済みsourceでも、秘密/本番rw/socketを渡すcomposeなら停止する。"""
    if set(config.get("services", {})) != {"candidate"}:
        raise Rejected("UNSAFE_COMPOSE")
    service = config["services"]["candidate"]
    if service.get("network_mode") != "none" or not service.get("read_only") or service.get("privileged") or str(service.get("user", "0")).split(":")[0] in {"0", "root", ""}:
        raise Rejected("UNSAFE_COMPOSE")
    if service.get("environment", {}) != {"TZ": "Asia/Tokyo"} or service.get("entrypoint") or service.get("command") != ["--workspace", "/candidate", "--snapshot", "/snapshot", "--output", "/candidate/runs"]:
        raise Rejected("UNSAFE_COMPOSE_ENV_OR_COMMAND")
    if "ALL" not in service.get("cap_drop", []) or not any(x.startswith("no-new-privileges") for x in service.get("security_opt", [])):
        raise Rejected("UNSAFE_COMPOSE")
    if any(service.get(key) for key in ("ports", "devices", "secrets", "configs", "group_add", "cap_add", "volumes_from", "networks")) or service.get("pid") == "host" or service.get("ipc") == "host":
        raise Rejected("UNSAFE_COMPOSE")
    expected = {"/snapshot": (checked["CANDIDATE_SNAPSHOT"], True), "/candidate/runs": (checked["CANDIDATE_OUTPUT"], False),
                "/candidate/.nas-candidate-workspace.json": (checked["CANDIDATE_WORKSPACE_MARKER"], True)}
    volumes = service.get("volumes", [])
    if len(volumes) != len(expected) or {volume.get("target") for volume in volumes} != set(expected):
        raise Rejected("UNSAFE_COMPOSE_MOUNTS")
    for volume in volumes:
        target = volume.get("target")
        if target not in expected or volume.get("type") != "bind":
            raise Rejected("UNSAFE_COMPOSE_MOUNTS")
        path, readonly = expected[target]
        if Path(volume.get("source", "")).resolve() != Path(path).resolve() or bool(volume.get("read_only")) != readonly:
            raise Rejected("UNSAFE_COMPOSE_MOUNTS")
    build = service.get("build", {})
    if Path(build.get("context", "")).resolve() != source or build.get("dockerfile") != "nas/Dockerfile" or any(build.get(k) for k in ("secrets", "ssh", "additional_contexts", "dockerfile_inline", "entitlements")):
        raise Rejected("UNSAFE_BUILD_CONTEXT")
    if build.get("args", {}) != {"SOURCE_COMMIT": checked["CANDIDATE_COMMIT"]}:
        raise Rejected("UNSAFE_BUILD_ARGUMENTS")
    if float(service.get("cpus", 0)) != limits["cpus"] or int(service.get("mem_limit", 0)) != limits["memory_mb"] * 1024 * 1024 or service.get("pids_limit") != limits["pids"]:
        raise Rejected("RESOURCE_LIMITS_NOT_APPLIED")


def arm64_verified(runtime, image):
    """ホストのarchitectureではなく、候補の実行記録と実imageの両方を検査する。"""
    candidate_platform = runtime.get("platform", "").lower()
    if image and (image.get("architecture") != "arm64" or image.get("os") != "linux"):
        raise Rejected("ARM64_IMAGE_REQUIRED")
    if candidate_platform and (not candidate_platform.startswith("linux") or re.search(r"x86_64|amd64|armv7", candidate_platform)):
        raise Rejected("ARM64_RUNTIME_REQUIRED")
    return bool(image and candidate_platform.startswith("linux") and re.search(r"aarch64|arm64", candidate_platform))


def comparison_summary(comparison, reference_runtime, runtime, commit):
    """PR17の比較結果を受入判定する。欠損や空集合を差分ゼロとして採用しない。"""
    if reference_runtime.get("source_commit") != commit or runtime.get("source_commit") != commit:
        raise Rejected("REFERENCE_COMMIT_MISMATCH")
    if any(comparison["attribution"].get(key) != "SAME" for key in ("data", "model", "implementation")):
        raise Rejected("COMPARISON_IDENTITY_MISMATCH")
    if not runtime.get("features") or reference_runtime.get("features") != runtime["features"] or any(comparison["unavailable_columns"].get(side, ["unknown"]) for side in ("left", "right")):
        raise Rejected("COMPARISON_COLUMNS_INCOMPLETE")
    races = comparison["races"]
    if comparison["common_horses"] <= 0 or not races:
        raise Rejected("EMPTY_COMPARISON")
    classification = ("予想状態", "予想可否", "対象外理由コード", "対象外理由")
    for side, metadata in (("left", reference_runtime), ("right", runtime)):
        seen, excluded, predicted, horses = set(), set(), 0, 0
        for race in races:
            rows = race[side]["bet_row"]
            if len(rows) != 1 or race["race_id"] in seen or rows[0].get("レースID") != race["race_id"] or not set(classification) <= rows[0].keys():
                raise Rejected("RACE_CLASSIFICATION_INCOMPLETE")
            row = rows[0]
            seen.add(race["race_id"])
            if (row["予想状態"], row["予想可否"]) == ("PREDICTED", "数値予想済み") and race[side]["horses"] > 0 and race[side]["top5"]:
                predicted += 1
            elif (row["予想状態"], row["予想可否"]) == ("NO_PREDICTION", "予想対象外") and row["対象外理由コード"] and row["対象外理由"] and race[side]["horses"] == 0:
                excluded.add(race["race_id"])
            else:
                raise Rejected("RACE_CLASSIFICATION_INCOMPLETE")
            horses += race[side]["horses"]
        validation = metadata["excel_validation"]
        if len(seen) != validation["source_race_count"] or predicted != validation["predicted_race_count"] or len(excluded) != validation["unsupported_race_count"] or excluded != set(metadata["population"]["excluded_races"]) or horses != metadata["population"]["predicted_horses"] or horses != comparison["common_horses"]:
            raise Rejected("RACE_POPULATION_MISMATCH")
    for race in races:
        if any((race["left"]["bet_row"][0][key] or None) != (race["right"]["bet_row"][0][key] or None) for key in classification):
            raise Rejected("RACE_CLASSIFICATION_MISMATCH")
    summary = {"windows_comparison": "MATCH", "changed_horses": len(comparison["horse_differences"]),
               "top5_changed_races": sum(row["top5_changed"] for row in races),
               "tickets_changed_races": sum(row["tickets_changed"] for row in races),
               "fixed_tickets_changed_races": sum(row["fixed_tickets_changed"] for row in races)}
    if comparison["left_only"] or comparison["right_only"] or any(summary[key] for key in ("changed_horses", "top5_changed_races", "tickets_changed_races", "fixed_tickets_changed_races")):
        raise Rejected("WINDOWS_LINUX_MISMATCH")
    return summary


def run(spec):
    if os.name != "posix" or os.geteuid() == 0:
        raise Rejected("NONROOT_LINUX_HOST_REQUIRED")
    source = Path(spec["source"]).resolve(strict=True)
    sys.path.insert(0, str(source))
    # 他PRのコードを複製せず、承認SHAの既存APIを使用する。
    from nas.workspace import initialize, Workspace
    from nas.launch import checked_environment
    from nas.candidate import verify_completed
    from continuous_improvement.archive import verify_archive
    job, policy = spec["job"], spec["policy"]
    production = Path(policy["production_root"]).resolve(strict=True)
    workspace = initialize(Path(spec["workspace"]), production)
    boundary = Workspace(workspace)
    env = dict(os.environ)
    env.update(CANDIDATE_UID=str(os.getuid()), CANDIDATE_GID=str(os.getgid()))
    before = audit(production, job["raceday"], env)
    atomic_json(workspace / "protected_before.json", before)
    try:
        entry = policy["inputs"][job["input_id"]]
        original = Path(entry["root"]).resolve(strict=True)
        if sha(original / "manifest.json") != entry["manifest_sha256"]:
            raise Rejected("INPUT_MANIFEST_MISMATCH")
        manifest = verify_archive(original)
        allowed = {"input.xlsx", "odds.csv", "masters/race_levels.xlsx", "masters/場所_馬場_タイム.xlsx", "calendar.txt", "baseline.xlsx"}
        if manifest["day"] != job["raceday"] or not set(manifest["files"]) <= allowed:
            raise Rejected("UNAPPROVED_SNAPSHOT_CONTENTS")
        snapshot = boundary.output(workspace / "snapshots" / job["input_id"], "snapshots", readonly=[original], new=True)
        snapshot.mkdir(parents=True)
        for name, expected in manifest["files"].items():
            target = snapshot / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(original / name, target)
            if sha(target) != expected:
                raise Rejected("INPUT_COPY_MISMATCH")
        shutil.copy2(original / "manifest.json", snapshot / "manifest.json")
        output = workspace / "runs"
        checked = checked_environment(workspace, snapshot, output, production, job["commit_sha"])
        output.mkdir()
        env.update(checked)
        limits = policy["limits"]
        image_name = spec["project"] + "-candidate"
        override = {"services": {"candidate": {"image": image_name, "cpus": limits["cpus"], "mem_limit": f"{limits['memory_mb']}m", "pids_limit": limits["pids"],
                     "labels": {"io.keiba.nas_agent.instance": spec["instance_id"], "io.keiba.nas_agent.job": job["job_id"]}}}}
        atomic_json(workspace / "limits.compose.json", override)
        command = ["docker", "compose", "-p", spec["project"], "-f", str(source / "nas/compose.yaml"), "-f", str(workspace / "limits.compose.json")]
        effective = docker_json(command[1:] + ["config", "--format", "json"], env)
        validate_compose(effective, checked, source, limits)
        if not policy.get("allow_build", False):
            raise Rejected("BUILD_REQUIRES_ADMIN_APPROVAL")
        subprocess.run(command + ["build", "candidate"], env=env, cwd=source, check=True)
        subprocess.run(command + ["run", "--rm", "--no-build", "candidate"], env=env, cwd=source, check=True)
        runs = [path for path in output.iterdir() if path.is_dir()]
        if len(runs) != 1:
            raise Rejected("UNEXPECTED_RUN_COUNT")
        candidate = runs[0]
        verify_completed(candidate)
        runtime = json.loads((candidate / "runtime.json").read_text(encoding="utf-8"))
        if runtime["source_commit"] != job["commit_sha"] or runtime["input_manifest_sha256"] != entry["manifest_sha256"]:
            raise Rejected("RUNTIME_IDENTITY_MISMATCH")
        image = None
        try:
            # 設定上のplatformだけで実ARM64を断定しない。imageの秘密/Envは取得しない。
            image = docker_json(["image", "inspect", "--format", '{"architecture":{{json .Architecture}},"os":{{json .Os}}}', image_name], env)
        except (OSError, subprocess.SubprocessError, ValueError):
            pass  # 実行成功と受入PASSを区別し、証拠不足はPARTIALに残す。
        arm_verified = arm64_verified(runtime, image)
        summary = {"architecture": platform.machine(), "python_version": runtime["python"], "ready_verified": True, "input_match": True,
                   "races": runtime["excel_validation"]["predicted_race_count"], "horses": runtime["population"]["predicted_horses"],
                   "excluded_races": len(runtime["population"]["excluded_races"]), "windows_comparison": "NOT_AVAILABLE", "acceptance": "PARTIAL", "arm64_verified": arm_verified}
        artifacts = {"candidate_manifest.json": sha(candidate / "manifest.json"), "prediction.xlsx": sha(candidate / "prediction.xlsx")}
        if entry.get("reference_root"):
            reference = Path(entry["reference_root"]).resolve(strict=True)
            if sha(reference / "manifest.json") != entry["reference_manifest_sha256"]:
                raise Rejected("REFERENCE_IDENTITY_MISMATCH")
            verify_completed(reference)
            reference_runtime = json.loads((reference / "runtime.json").read_text(encoding="utf-8"))
            from nas.compare import compare
            comparison = compare(reference / "prediction.xlsx", candidate / "prediction.xlsx", workspace / "comparisons/windows_linux.json", runtime, workspace=workspace)
            summary.update(comparison_summary(comparison, reference_runtime, runtime, job["commit_sha"]))
            if arm_verified:
                summary["acceptance"] = "PASS"
            artifacts["windows_linux.json"] = sha(workspace / "comparisons/windows_linux.json")
        verify_archive(original)
        if sha(original / "manifest.json") != entry["manifest_sha256"]:
            raise Rejected("INPUT_CHANGED")
        after = audit(production, job["raceday"], env)
        atomic_json(workspace / "protected_after.json", after)
        if before != after:
            raise Rejected("PROTECTED_PRODUCTION_CHANGED")
        summary["protected_unchanged"] = True
        artifacts["protected_before.json"] = sha(workspace / "protected_before.json")
        atomic_json(spec["result_path"], {"summary": summary, "artifacts_sha256": artifacts})
    finally:
        # 失敗時も可能な範囲で監査を残す。本番を復元/更新しない。
        if not (workspace / "protected_after.json").exists():
            atomic_json(workspace / "protected_after.json", audit(production, job["raceday"], env))


if __name__ == "__main__":
    run(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")))
