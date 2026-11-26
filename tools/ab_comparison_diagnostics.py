"""PR #31 の固定コミットを再生する検証用の特徴量経路。"""

from hashlib import sha256
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from types import ModuleType

FEATURE_FILE = "1_keibayosou_features.py"
PRE_FIX_MAIN = "178855fd117301131c8e3ebf70ce8c5e9e327728"
PR31 = {
    "D-01": "a8c32f2",
    "D-02": "5fc76d4",
    "D-03": "1b7c21f",
    "D-04": "e0ffd7f",
    "D-05": "fb26511",
    "D-09": "c063e98",
    "D-12": "582ef21",
}


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])


def diagnostic_builders(root):
    """履歴からモジュールを独立して読み、現行のモジュールを変更しない。"""
    builders, metadata = {}, {}
    main = git(root, "show", f"{PRE_FIX_MAIN}:{FEATURE_FILE}")
    d01 = git(root, "show", f"{PR31['D-01']}:{FEATURE_FILE}")

    def register(label, code, revision, validation, use_cache):
        module = ModuleType(f"ab_diagnostics_{label}")
        module.__file__ = str(root / FEATURE_FILE)
        exec(compile(code, f"{revision}:{FEATURE_FILE}", "exec"), module.__dict__)
        builders[label] = (module.build_features_from_excel, validation, use_cache)
        metadata[label] = {
            "code_sha": git(root, "rev-parse", revision).decode().strip(),
            "feature_source_sha256": sha256(code).hexdigest(),
            "validation": validation,
            "use_horses_sheet": use_cache,
        }

    register("main", main, PRE_FIX_MAIN, None, True)
    register("main_no_cache", main, PRE_FIX_MAIN, None, False)
    previous = "main_no_cache"
    for item, revision in PR31.items():
        if item == "D-12":
            continue
        label = "cumulative_" + item
        source = git(root, "show", f"{revision}:{FEATURE_FILE}")
        register(label, source, revision, True, False)
        metadata[label]["compare_to"] = previous
        previous = label
        if item == "D-01":
            continue
        with TemporaryDirectory(prefix="jibunnyou-ab-patch-") as tmp:
            source_path = Path(tmp) / FEATURE_FILE
            source_path.write_bytes(d01)
            # D-03 の隣接行は D-02 による変更。対象行の旧テキストを照合して適用する。
            context = "-U0" if item == "D-03" else "-U1"
            diff = git(root, "diff", context, f"{revision}^", revision, "--", FEATURE_FILE)
            subprocess.run(
                ["patch", "--batch", "--fuzz=0", "--no-backup-if-mismatch", str(source_path)],
                input=diff, capture_output=True, check=True,
            )
            label = "single_" + item
            register(label, source_path.read_bytes(), revision, True, False)
            metadata[label]["foundation_sha"] = metadata["cumulative_D-01"]["code_sha"]
            metadata[label]["compare_to"] = "cumulative_D-01"

    try:
        old = git(root, "show", f"{PR31['D-12']}^:config/shadow_model.json")
        new = git(root, "show", f"{PR31['D-12']}:config/shadow_model.json")
    except subprocess.CalledProcessError:
        raise ValueError("D-12 の固定条件を参照できません") from None
    import json

    before, after = json.loads(old), json.loads(new)
    weight = git(root, "show", f"{PR31['D-12']}:{after['weight_file']}")
    actual = sha256(weight).hexdigest()
    metadata["D-12"] = {
        "code_sha": git(root, "rev-parse", PR31["D-12"]).decode().strip(),
        "actual_weight_sha256": actual,
        "before_expected": before["file_sha256"],
        "after_expected": after["file_sha256"],
        "before_matches": before["file_sha256"] == actual,
        "after_matches": after["file_sha256"] == actual,
    }
    return builders, metadata
