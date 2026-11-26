"""Prepare a hash-pinned one-day fixture for the PR #34 comparison."""
import argparse
from datetime import date, timedelta
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from nas_agent.comparison import (
    CONTROL_FILES, SNAPSHOT_FILES, archive_path, sha256, stage_fixture, validate_rows,
)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def result_rows(path, finalized, excluded):
    """Adapt the repository's settled-results workbook using archive timestamps."""
    from tokutyouryou_keisann.common import load_results_all_sheets

    entries, payouts = load_results_all_sheets(str(path))
    rows = []
    for race_id, finalized_at in finalized.items():
        row = {"race_id": race_id, "finalized_at": finalized_at}
        top = entries.loc[(entries["rid_str"] == race_id) & entries["着順_num"].between(1, 3)]
        if race_id not in excluded and sorted(top["着順_num"]) == [1, 2, 3] and top["馬番_int"].notna().all():
            numbers = [int(number) for number in top.sort_values("着順_num")["馬番_int"]]
            if min(numbers) > 0 and len(set(numbers)) == 3:
                row["finish_top3"] = numbers
                race_payouts = payouts.loc[payouts["rid_str"] == race_id]
                prices = {}
                for kind in ("単勝", "複勝", "3連複"):
                    matches = race_payouts.loc[race_payouts["払戻種別"].str.strip() == kind]
                    if (not matches.empty and matches["組番_norm"].is_unique
                            and (matches["払戻金_int"] > 0).all()):
                        prices[kind] = dict(zip(matches["組番_norm"], map(int, matches["払戻金_int"])))
                row["payouts"] = prices
        rows.append(row)
    return rows


def smoke_settings(day):
    return {
        "schema_version": 1,
        "snapshots": "snapshots.json",
        "results": "results.json",
        "periods": {stage: [(day + timedelta(days=offset)).isoformat()] * 2
                    for offset, stage in enumerate(("train", "validation", "forward"))},
        "optimizer_iterations": dict.fromkeys(("N_ITER_DEFAULT", "N_ITER_PLACE", "N_ITER_PLACE_SURFACE"), 2),
    }


def prepare(index, settled, output, day):
    if output.exists():
        raise FileExistsError(output)
    with TemporaryDirectory(dir=output.parent) as temporary:
        fixture = Path(temporary) / "fixture"
        evidence = fixture / "archive" / "source"
        evidence.mkdir(parents=True)
        # Hash and parse the same copies that will be registered in the manifest.
        index_copy = evidence / "snapshot_index.json"
        settled_copy = evidence / f"settled{settled.suffix.lower()}"
        shutil.copyfile(index, index_copy)
        shutil.copyfile(settled, settled_copy)
        metadata = read_json(index_copy)
        snapshots = metadata["snapshots"]
        copied = {}
        for row in snapshots:
            for key in SNAPSHOT_FILES:
                if key not in row:
                    continue
                name = row[key]
                # Paths in the archive index use the same relative path rules as fixture paths.
                source = archive_path(index.parent, name)
                target = f"archive/inputs/{name.removeprefix('archive/')}"
                if target not in copied:
                    destination = fixture / target
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, destination)
                    copied[target] = sha256(destination)
                expected = row.get(f"{key}_sha256")
                if expected is not None and expected != copied[target]:
                    raise ValueError(f"保存時の SHA256 と一致しません: {name}")
                row[key] = target
                row[f"{key}_sha256"] = copied[target]
        if settled_copy.suffix == ".xlsx":
            results = result_rows(settled_copy, metadata["finalized_at"], metadata.get("exclude_results", {}))
        elif settled_copy.suffix == ".json":
            results = read_json(settled_copy)
        else:
            raise ValueError("確定結果には .xlsx または .json を指定してください")
        validate_rows(snapshots, results, day)
        settings = smoke_settings(day)
        for filename, data in (("run.json", settings), ("snapshots.json", snapshots), ("results.json", results)):
            (fixture / filename).write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
                                            encoding="utf-8")
        files = {**copied, **{name: sha256(fixture / name) for name in sorted(CONTROL_FILES)}}
        files.update({path.relative_to(fixture).as_posix(): sha256(path) for path in sorted(evidence.iterdir())})
        manifest = fixture / "manifest.json"
        manifest.write_text(json.dumps({"schema_version": 1, "raceday": day.isoformat(), "files_sha256": files},
                                       ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        digest = sha256(manifest)
        stage_fixture({"root": fixture, "manifest_sha256": digest}, day.strftime("%Y%m%d"),
                      Path(temporary) / "validated", lambda: None)
        fixture.rename(output)
    return digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", required=True, type=Path)
    parser.add_argument("--settled", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--raceday", type=date.fromisoformat, default=date(2026, 8, 9))
    args = parser.parse_args()
    print(prepare(args.index, args.settled, args.output, args.raceday))


if __name__ == "__main__":
    main()
