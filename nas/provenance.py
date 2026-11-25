"""利用者の説明文と、保存済みSHAから検証できた出所を区別する。"""
import hashlib
import json
from pathlib import Path

from continuous_improvement.archive import sha256, verify_archive


def digest_mapping(mapping):
    return hashlib.sha256(json.dumps(mapping, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def identity(prediction: Path, *, legacy=False) -> dict:
    """欠けた記録を推測で埋めず、不一致の固定保存は明示停止する。"""
    folder = prediction.parent
    result = {"prediction_sha256": sha256(prediction), "input_manifest_sha256": None,
              "model": None, "implementation_sha256": None, "status": "UNVERIFIED", "missing": []}
    if legacy:
        source = folder / "source_manifest.json"
        if not source.is_file():
            result["missing"].append("source_manifest.json")
            return result
        manifest = json.loads(source.read_text(encoding="utf-8"))
        for name, expected in manifest["files"].items():
            path = (folder / name).resolve()
            if not path.is_relative_to(folder.resolve()) or sha256(path) != expected:
                raise ValueError("legacy出所ソースSHA不一致")
        trace = folder / "trace_manifest.json"
        if not trace.is_file():
            result["missing"].append("trace_manifest.json")
            return result
        trace = json.loads(trace.read_text(encoding="utf-8"))
        if trace["prediction_sha256"] != result["prediction_sha256"] or trace["raw_scores_sha256"] != sha256(folder / "raw_scores.json"):
            raise ValueError("legacy出所と予想/診断SHA不一致")
        result["input_manifest_sha256"] = manifest.get("input_manifest_sha256")
        result["implementation_sha256"] = digest_mapping({k: v for k, v in manifest["files"].items() if k.endswith(".py") and k.startswith("app/")})
        runtime_path = folder / "runtime.json"
        if runtime_path.is_file() and "runtime_sha256" in trace and "source_manifest_sha256" in trace:
            if trace["runtime_sha256"] != sha256(runtime_path) or trace["source_manifest_sha256"] != sha256(source):
                raise ValueError("legacy runtime/source manifest固定SHAが不一致")
            runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
            if runtime.get("prediction_sha256") != result["prediction_sha256"] or runtime.get("input_manifest_sha256") != result["input_manifest_sha256"]:
                raise ValueError("legacy runtimeと固定保存が不一致")
            result["model"] = runtime.get("model_identity")
            if result["model"]:
                weight = result["model"]["weight_file"]
                if manifest["files"].get(weight) != result["model"]["file_sha256"]:
                    raise ValueError("legacyモデル実体SHAが不一致")
                from model_registry import normalized_weights_sha256
                weights = {**runtime["weights"]["common"], **{tuple(k.split("|")): v for k, v in runtime["weights"]["surface"].items()}}
                if normalized_weights_sha256(weights) != result["model"]["effective_sha256"]:
                    raise ValueError("legacy実効重みSHAが不一致")
        else:
            result["missing"].append("runtime.json/model_identity")
    else:
        if not (folder / "manifest.json").is_file():
            result["missing"].append("manifest.json")
            return result
        manifest = verify_archive(folder)
        if manifest["files"].get(prediction.name) != result["prediction_sha256"] or "runtime.json" not in manifest["files"]:
            raise ValueError("candidate予想/runtimeが固定manifestにありません")
        runtime = json.loads((folder / "runtime.json").read_text(encoding="utf-8"))
        result["input_manifest_sha256"] = runtime.get("input_manifest_sha256")
        policy = runtime.get("candidate_policy")
        if policy:
            result["model"] = {k: policy.get(k) for k in ("model_name", "file_sha256", "effective_sha256")}
            weight_name = f"yosou_py/{policy['model_name']}.py"
            if runtime.get("code_hashes", {}).get(weight_name) != policy["file_sha256"]:
                raise ValueError("candidateモデル実体SHAが出所と不一致")
            from model_registry import normalized_weights_sha256
            weights = {**runtime["weights"]["common"], **{tuple(k.split("|")): v for k, v in runtime["weights"]["surface"].items()}}
            if normalized_weights_sha256(weights) != policy["effective_sha256"]:
                raise ValueError("candidate実効重みSHAが出所と不一致")
        if runtime.get("code_hashes"):
            result["implementation_sha256"] = digest_mapping({"core": {k: v for k, v in runtime["code_hashes"].items() if k.endswith(".py") and "/" not in k},
                                                               "adapters": runtime.get("adapter_hashes", {})})
    for field in ("input_manifest_sha256", "model", "implementation_sha256"):
        if not result[field]:
            result["missing"].append(field)
    result["status"] = "VERIFIED_SAVED_RECORDS" if not result["missing"] else "PARTIAL"
    return result


def attribution(left: Path, right: Path) -> dict:
    """同一入力という主張は両側のinput_manifest SHAからのみ生成する。"""
    a = identity(left, legacy=(left.parent / "source_manifest.json").is_file())
    b = identity(right)
    relation = lambda x, y: "UNVERIFIED" if x is None or y is None else "SAME" if x == y else "DIFFERENT"
    model = lambda value: None if not value or any(not value.get(k) for k in ("file_sha256", "effective_sha256")) else {k: value[k] for k in ("file_sha256", "effective_sha256")}
    return {"left": a, "right": b, "data": relation(a["input_manifest_sha256"], b["input_manifest_sha256"]),
            "model": relation(model(a["model"]), model(b["model"])),
            "implementation": relation(a["implementation_sha256"], b["implementation_sha256"]),
            "rule": "保存済み識別子の照合。稼働中実体の証明や複合差の単一原因認定はしない"}
