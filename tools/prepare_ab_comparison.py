"""比較設定の入力を複製し、証跡と SHA-256 を持つフィクスチャを作る。"""

import argparse
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory

from model_registry import sha256_file
from tools.ab_comparison_evidence import snapshot_grade, timestamp
from tools.ab_comparison_inputs import read_results
from tools.compare_ab_models import read_json, save_json


SNAPSHOT_FILES = ("workbook", "race_levels", "base_time", "odds_csv")


def workbook_results(path, evidence):
    from tokutyouryou_keisann.common import load_results_all_sheets

    entries, payouts = load_results_all_sheets(str(path))
    for proof in evidence:
        rid = proof["race_id"]
        row = dict(proof)
        top = entries.loc[entries["rid_str"].eq(rid) & entries["着順_num"].between(1, 3)]
        if sorted(top["着順_num"]) == [1, 2, 3] and top["馬番_int"].notna().all():
            row["finish_top3"] = top.sort_values("着順_num")["馬番_int"].astype(int).tolist()
            race_payouts = payouts.loc[payouts["rid_str"].eq(rid)]
            row["payouts"] = {}
            for kind in ("単勝", "複勝", "3連複"):
                matches = race_payouts.loc[race_payouts["払戻種別"].str.strip().eq(kind)]
                if (not matches.empty and matches["組番_norm"].is_unique
                        and matches["払戻金_int"].gt(0).all()):
                    row["payouts"][kind] = dict(zip(matches["組番_norm"], map(int, matches["払戻金_int"])))
        yield row


def prepare(config_path, output, workbook_evidence_path=None):
    if output.exists():
        raise FileExistsError(output)
    root = config_path.parent
    config = read_json(config_path)
    snapshots = read_json(root / config["snapshots"])
    results_path = root / config["results"]
    if results_path.suffix.lower() == ".xlsx":
        if workbook_evidence_path is None:
            raise ValueError("確定結果の Excel には --result-evidence が必要です")
        results = list(workbook_results(results_path, read_json(workbook_evidence_path)))
    else:
        if workbook_evidence_path is not None:
            raise ValueError("--result-evidence は確定結果の Excel に指定してください")
        results = read_json(results_path)

    with TemporaryDirectory(dir=output.parent) as temp:
        fixture = Path(temp) / "fixture"
        fixture.mkdir()
        archive = fixture / "archive"
        archive.mkdir()
        starts, snapshot_grades, copied = {}, {}, {}
        for row in snapshots:
            rid = row["race_id"]
            if not re.fullmatch(r"\d{10,13}", rid) or rid in starts:
                raise ValueError(f"レースIDが不正または重複しています: {rid}")
            start = timestamp(row["start_at"])
            starts[rid] = start
            grade = snapshot_grades[rid] = row["input_evidence_grade"] = snapshot_grade(row)
            if grade == "A":
                observed = timestamp(row["observed_at"])
                if observed >= start:
                    raise ValueError(f"{rid}: 発走前スナップショットが必要です")
                for key in (*SNAPSHOT_FILES[1:], "dl"):
                    if row.get(key) and timestamp(row[f"{key}_observed_at"]) > observed:
                        raise ValueError(f"{rid}: {key} が予想スナップショットより後です")
                if timestamp(row.get("popularity_observed_at", row["observed_at"])) > observed:
                    raise ValueError(f"{rid}: 人気の取得が予想スナップショットより後です")
                if row.get("dl") and timestamp(row["dl_model_training_end_at"]) >= timestamp(row["dl_observed_at"]):
                    raise ValueError(f"{rid}: DL 学習終了・取得時刻が不正です")
            for key in SNAPSHOT_FILES:
                if key not in row:
                    continue
                source = (root / row[key]).resolve()
                if source not in copied:
                    target = archive / f"{len(copied)}{source.suffix}"
                    shutil.copyfile(source, target)
                    copied[source] = (target.relative_to(fixture).as_posix(), sha256_file(target))
                target, digest = copied[source]
                if row.get(f"{key}_sha256", digest) != digest:
                    raise ValueError(f"SHA-256不一致: {source}")
                row[key], row[f"{key}_sha256"] = target, digest

        evidence = {}
        read_results(results, {}, evidence, starts=starts)
        # 比較実行設定を同梱する。入力以外の設定パスは元の参照先を維持する。
        for key in ("candidate_manifest", "operational_results"):
            if key in config:
                config[key] = str((root / config[key]).resolve())
        for baseline in config.get("baselines", {}).values():
            if "model_root" in baseline:
                baseline["model_root"] = str((root / baseline["model_root"]).resolve())
        config.update(snapshots="snapshots.json", results="results.json")
        for name, data in (("run.json", config), ("snapshots.json", snapshots), ("results.json", results)):
            save_json(fixture / name, data)
        manifest = {
            "schema_version": 1,
            "snapshot_evidence_grades": snapshot_grades,
            "result_evidence": evidence,
            "files_sha256": {path.relative_to(fixture).as_posix(): sha256_file(path)
                             for path in sorted(fixture.rglob("*")) if path.is_file()},
        }
        save_json(fixture / "manifest.json", manifest)
        fixture.rename(output)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--result-evidence", type=Path)
    args = parser.parse_args()
    prepare(args.config.resolve(), args.output.resolve(), args.result_evidence)


if __name__ == "__main__":
    main()
