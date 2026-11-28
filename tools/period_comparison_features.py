"""PR #31 の検証式を、項目と horses 補完を独立に切り替えて再生する。"""

from hashlib import sha256
import re
from types import ModuleType

from tools.ab_comparison_diagnostics import FEATURE_FILE, PR31, diagnostic_builders, git
from tools.ab_comparison_inputs import features

METHODS = {
    "M00": (),
    "M01": ("D-02",),
    "M02": ("D-03",),
    "M03": ("D-02", "D-03"),
    "M04": ("D-01", "D-02", "D-03", "D-04", "D-05", "D-09"),
    "M05": ("D-01",),
    "M06": ("D-04",),
    "M07": ("D-05",),
    "M08": ("D-09",),
}

# PR #31 最終版の validation を分解する。各アンカーの全出現を照合し、
# 上流のソースが変わった場合に意図しない式で走ることを防ぐ。
SWITCHES = (
    ("D-01", "if validation:\n        now_date", "if APPLY_D01:\n        now_date", 1),
    ("D-01", "if validation and c_date:", "if APPLY_D01 and c_date:", 1),
    ("D-01", "if validation:\n                if c_date", "if APPLY_D01:\n                if c_date", 1),
    ("D-01", 'if not validation:\n                    one["__date__"]', 'if not APPLY_D01:\n                    one["__date__"]', 1),
    ("D-01", "if raceday or validation:", "if raceday or APPLY_D01:", 1),
    ("D-01", "if date_col and validation:", "if date_col and APPLY_D01:", 1),
    ("D-02", "_parse_pace_from_laps(lap, validation=validation)", "_parse_pace_from_laps(lap, validation=APPLY_D02)", 1),
    ("D-03", "_parse_class_name_from_text(name, validation=validation)", "_parse_class_name_from_text(name, validation=APPLY_D03)", 1),
    ("D-03", 'r_now.get("レース名", "")), validation=validation,', 'r_now.get("レース名", "")), validation=APPLY_D03,', 1),
    ("D-04", "if validation:\n            status_col", "if APPLY_D04:\n            status_col", 1),
    ("D-04", "if validation:\n                finish_s", "if APPLY_D04:\n                finish_s", 1),
    ("D-09", "finish_s.dropna() if validation else finish_s", "finish_s.dropna() if APPLY_D09 else finish_s", 1),
)


def isolated_source(source):
    for item, old, new, expected in SWITCHES:
        actual = source.count(old)
        source = source.replace(old, new)
        if actual != expected:
            raise ValueError(f"{item}: PR #31 の差分位置が一致しません ({actual}/{expected})")
    source, actual = re.subn(
        r"(_safe_recent_mean\(\w+), validation=validation\)", r"\1, validation=APPLY_D05)", source,
    )
    if actual != 6:
        raise ValueError(f"D-05: PR #31 の差分位置が一致しません ({actual}/6)")
    return source


def period_builders(root):
    _, historical_metadata = diagnostic_builders(root)
    revision = historical_metadata["cumulative_D-09"]["code_sha"]
    original = git(root, "show", f"{revision}:{FEATURE_FILE}")
    source = isolated_source(original.decode("utf-8"))
    compiled = compile(source, f"{revision}:{FEATURE_FILE}:isolated", "exec")
    builders = {
        "legacy": (features.build_features_from_excel, False, True),
        "corrected": (features.build_features_from_excel, True, False),
        "legacy_no_cache": (features.build_features_from_excel, False, False),
    }
    for method, items in METHODS.items():
        if method in {"M00", "M04"}:
            continue
        module = ModuleType(f"period_comparison_{method}")
        module.__file__ = str(root / FEATURE_FILE)
        module.__dict__.update(
            {f"APPLY_{item.replace('-', '')}": item in items for item in METHODS["M04"]}
        )
        exec(compiled, module.__dict__)
        # validation=False keeps the legacy horses fallback independent of D-01–D-09.
        builders[method] = (module.build_features_from_excel, False, True)
    metadata = {
        "D-12": historical_metadata["D-12"],
        "feature_revision": revision,
        "isolated_source_sha256": sha256(source.encode("utf-8")).hexdigest(),
        "fix_revisions": {item: historical_metadata["cumulative_" + item]["code_sha"]
                          for item in PR31 if item != "D-12"},
    }
    return builders, metadata
