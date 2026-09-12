# -*- coding: utf-8 -*-
"""
馬レベル・レースレベル v2 の生成〜検証〜最終判定を1回で実行する統合ランナー。

処理順:
1. 00_Export_To_Excel_4_v2.py
   - v1既存シート + v2比較用シートを生成
2. compare_race_level_v1_v2.py
   - v1 / v2 のレース前rating予測力を比較
3. build_confirmed_race_level_v2.py
   - 後続成績から confirmed race level を生成
   - entries_confirmed_v2 は各出走時点までの情報だけを使うリーク防止版
4. evaluate_confirmed_race_level_v2.py
   - 古い70%でalpha選択、新しい30%で固定評価
5. 最終判定Excel/CSVを生成

重要:
- mainブランチや本番モデルの設定は変更しない。
- ROIだけで採用しない。
- 採用判定は予測力を優先し、ROIは確認指標として扱う。
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
DEFAULT_MASTER = REPO_ROOT / "data" / "master"

SCRIPT_V2 = HERE / "00_Export_To_Excel_4_v2.py"
SCRIPT_COMPARE = HERE / "compare_race_level_v1_v2.py"
SCRIPT_CONFIRMED = HERE / "build_confirmed_race_level_v2.py"
SCRIPT_EVAL_CONFIRMED = HERE / "evaluate_confirmed_race_level_v2.py"

KEY_METRICS = [
    "top1_win_rate",
    "top1_place_rate",
    "top3_contains_winner_rate",
    "mean_spearman",
]


def run_step(name: str, cmd: List[str]) -> Dict[str, object]:
    print("\n" + "=" * 80)
    print(f"[PIPELINE] {name}")
    print("[CMD] " + " ".join(cmd))
    print("=" * 80)

    started = dt.datetime.now()
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        text=True,
        capture_output=True,
    )
    ended = dt.datetime.now()

    if proc.stdout:
        print(proc.stdout.rstrip())
    if proc.stderr:
        print(proc.stderr.rstrip(), file=sys.stderr)

    result = {
        "step": name,
        "returncode": int(proc.returncode),
        "started_at": started.isoformat(timespec="seconds"),
        "ended_at": ended.isoformat(timespec="seconds"),
        "elapsed_sec": round((ended - started).total_seconds(), 3),
        "command": " ".join(cmd),
        "stdout_tail": "\n".join(proc.stdout.splitlines()[-30:]) if proc.stdout else "",
        "stderr_tail": "\n".join(proc.stderr.splitlines()[-30:]) if proc.stderr else "",
    }
    if proc.returncode != 0:
        raise RuntimeError(
            f"{name} が失敗しました。returncode={proc.returncode}\n"
            f"stderr:\n{result['stderr_tail']}"
        )
    return result


def require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} が見つかりません: {path}")


def read_excel_sheet(path: Path, sheet: str) -> pd.DataFrame:
    require_file(path, "Excel")
    xls = pd.ExcelFile(path, engine="openpyxl")
    try:
        if sheet not in xls.sheet_names:
            raise ValueError(f"{path.name} に {sheet} シートがありません。")
        return pd.read_excel(xls, sheet_name=sheet)
    finally:
        xls.close()


def as_float(value, default: float = float("nan")) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def row_for_model(summary: pd.DataFrame, model: str) -> Optional[pd.Series]:
    if "model" not in summary.columns:
        return None
    hit = summary[summary["model"].astype(str) == model]
    if hit.empty:
        return None
    return hit.iloc[0]


def decide_v2(compare_xlsx: Path) -> Dict[str, object]:
    summary = read_excel_sheet(compare_xlsx, "summary")
    v1 = row_for_model(summary, "v1")
    v2 = row_for_model(summary, "v2")
    if v1 is None or v2 is None:
        raise ValueError("v1/v2比較summaryから v1 または v2 を取得できません。")

    gains: Dict[str, float] = {}
    improved = 0
    non_worse = 0
    for metric in KEY_METRICS:
        a = as_float(v1.get(metric))
        b = as_float(v2.get(metric))
        gain = b - a
        gains[metric] = gain
        if pd.notna(gain) and gain > 0:
            improved += 1
        if pd.notna(gain) and gain >= -0.002:
            non_worse += 1

    spearman_gain = gains.get("mean_spearman", float("nan"))
    win_gain = gains.get("top1_win_rate", float("nan"))
    top3_gain = gains.get("top3_contains_winner_rate", float("nan"))

    # ROIは採用条件に使わず、表示だけ行う。
    roi_v1 = as_float(v1.get("win_roi_pct"))
    roi_v2 = as_float(v2.get("win_roi_pct"))
    roi_gain = roi_v2 - roi_v1

    # 予測力中心の保守的判定。
    # 1) Spearmanが大きく悪化せず、4指標中2つ以上改善 -> 採用候補
    # 2) Spearmanが明確悪化し、勝率またはTop3も悪化 -> 不採用
    # 3) それ以外 -> データ追加待ち
    if pd.notna(spearman_gain) and spearman_gain >= -0.002 and improved >= 2 and non_worse >= 3:
        decision = "ADOPT_V2_CANDIDATE"
    elif (
        pd.notna(spearman_gain)
        and spearman_gain < -0.010
        and ((pd.notna(win_gain) and win_gain < 0) or (pd.notna(top3_gain) and top3_gain < 0))
    ):
        decision = "REJECT_V2"
    else:
        decision = "HOLD_V2_MORE_DATA"

    return {
        "v2_decision": decision,
        "v1_race_count": int(as_float(v1.get("race_count"), 0.0)),
        "v2_race_count": int(as_float(v2.get("race_count"), 0.0)),
        "v1_top1_win_rate": as_float(v1.get("top1_win_rate")),
        "v2_top1_win_rate": as_float(v2.get("top1_win_rate")),
        "v2_minus_v1_top1_win_rate": gains.get("top1_win_rate"),
        "v1_top1_place_rate": as_float(v1.get("top1_place_rate")),
        "v2_top1_place_rate": as_float(v2.get("top1_place_rate")),
        "v2_minus_v1_top1_place_rate": gains.get("top1_place_rate"),
        "v1_top3_contains_winner_rate": as_float(v1.get("top3_contains_winner_rate")),
        "v2_top3_contains_winner_rate": as_float(v2.get("top3_contains_winner_rate")),
        "v2_minus_v1_top3_contains_winner_rate": gains.get("top3_contains_winner_rate"),
        "v1_mean_spearman": as_float(v1.get("mean_spearman")),
        "v2_mean_spearman": as_float(v2.get("mean_spearman")),
        "v2_minus_v1_mean_spearman": gains.get("mean_spearman"),
        "v1_win_roi_pct": roi_v1,
        "v2_win_roi_pct": roi_v2,
        "v2_minus_v1_win_roi_pct_point": roi_gain,
        "improved_key_metrics": improved,
        "non_worse_key_metrics": non_worse,
    }


def decide_confirmed(eval_xlsx: Path) -> Dict[str, object]:
    decision_df = read_excel_sheet(eval_xlsx, "decision")
    if decision_df.empty:
        raise ValueError("confirmed評価のdecisionシートが空です。")
    r = decision_df.iloc[0]
    return {
        "confirmed_decision": str(r.get("decision", "UNKNOWN")),
        "confirmed_best_alpha": as_float(r.get("best_alpha_from_train")),
        "confirmed_cutoff_date": str(r.get("cutoff_date", "")),
        "confirmed_test_spearman_gain": as_float(r.get("test_spearman_gain")),
        "confirmed_test_top1_win_gain": as_float(r.get("test_top1_win_gain")),
        "confirmed_test_top3_winner_gain": as_float(r.get("test_top3_winner_gain")),
        "confirmed_test_roi_gain_pct_point": as_float(r.get("test_roi_gain_pct_point")),
    }


def decide_overall(v2_info: Dict[str, object], conf_info: Dict[str, object]) -> str:
    v2 = str(v2_info.get("v2_decision"))
    conf = str(conf_info.get("confirmed_decision"))

    if v2 == "REJECT_V2":
        return "KEEP_V1"
    if v2 == "ADOPT_V2_CANDIDATE":
        if conf == "ADOPT_CONFIRMED_FEATURE_CANDIDATE":
            return "V2_PLUS_CONFIRMED_CANDIDATE"
        return "V2_BASE_CANDIDATE"
    return "HOLD_MORE_DATA"


def build_report(
    source_xlsx: Path,
    race_levels_xlsx: Path,
    compare_xlsx: Path,
    confirmed_eval_xlsx: Path,
    out_xlsx: Path,
    out_csv: Path,
    steps: List[Dict[str, object]],
) -> None:
    v2_info = decide_v2(compare_xlsx)
    conf_info = decide_confirmed(confirmed_eval_xlsx)
    overall = decide_overall(v2_info, conf_info)

    summary = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "overall_decision": overall,
        "source_xlsx": str(source_xlsx),
        "race_levels_xlsx": str(race_levels_xlsx),
        "v1_v2_compare_xlsx": str(compare_xlsx),
        "confirmed_evaluation_xlsx": str(confirmed_eval_xlsx),
        **v2_info,
        **conf_info,
    }
    summary_df = pd.DataFrame([summary])
    step_df = pd.DataFrame(steps)

    rules = pd.DataFrame([
        {
            "item": "V2採用候補",
            "rule": "mean_spearman悪化が-0.002以内、主要4指標中2つ以上改善、3つ以上が非悪化",
        },
        {
            "item": "V2不採用",
            "rule": "mean_spearmanが-0.010未満まで悪化し、勝率またはTop3捕捉率も悪化",
        },
        {
            "item": "confirmed採用候補",
            "rule": "evaluate_confirmed_race_level_v2.py の時系列TEST判定をそのまま採用",
        },
        {
            "item": "ROIの扱い",
            "rule": "採用重み探索には使わず、TESTでの確認指標としてのみ表示",
        },
        {
            "item": "main反映",
            "rule": "このランナーでは行わない。採用候補判定後に別途反映する",
        },
    ])

    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="FINAL_DECISION", index=False)
        step_df.to_excel(writer, sheet_name="PIPELINE_LOG", index=False)
        rules.to_excel(writer, sheet_name="DECISION_RULES", index=False)

        for sheet_name in ["FINAL_DECISION", "PIPELINE_LOG", "DECISION_RULES"]:
            ws = writer.book[sheet_name]
            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions
            for col_cells in ws.columns:
                max_len = min(max(len(str(c.value or "")) for c in col_cells[:300]) + 2, 60)
                ws.column_dimensions[col_cells[0].column_letter].width = max_len

    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary.keys()))
        writer.writeheader()
        writer.writerow(summary)

    print("\n" + "#" * 80)
    print("[FINAL DECISION]")
    print(f"overall_decision = {overall}")
    print(f"v2_decision      = {v2_info['v2_decision']}")
    print(f"confirmed         = {conf_info['confirmed_decision']}")
    print(f"report            = {out_xlsx}")
    print("#" * 80)


def main() -> None:
    parser = argparse.ArgumentParser(description="馬レベル・レースレベルv2統合検証パイプライン")
    parser.add_argument(
        "--excel",
        type=Path,
        default=DEFAULT_MASTER / "racedata_results.xlsx",
        help="元データ racedata_results.xlsx",
    )
    parser.add_argument(
        "--race-levels",
        type=Path,
        default=DEFAULT_MASTER / "race_levels.xlsx",
        help="v1/v2を格納する race_levels.xlsx",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=DEFAULT_MASTER,
        help="比較結果・評価結果・最終判定の保存先",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="既存race_levels.xlsxを使い、v1/v2生成を省略",
    )
    args = parser.parse_args()

    src = args.excel.resolve()
    race_levels = args.race_levels.resolve()
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    require_file(src, "元データ")
    for script in [SCRIPT_V2, SCRIPT_COMPARE, SCRIPT_CONFIRMED, SCRIPT_EVAL_CONFIRMED]:
        require_file(script, "必要スクリプト")

    compare_xlsx = work_dir / "race_levels_v1_v2_compare.xlsx"
    confirmed_eval_xlsx = work_dir / "confirmed_race_level_v2_evaluation.xlsx"
    final_xlsx = work_dir / "race_level_v2_final_decision.xlsx"
    final_csv = work_dir / "race_level_v2_final_decision.csv"

    steps: List[Dict[str, object]] = []
    py = sys.executable

    if not args.skip_generate:
        steps.append(run_step(
            "01_generate_v1_v2",
            [py, "-u", str(SCRIPT_V2), "--excel", str(src), "--out", str(race_levels)],
        ))
    else:
        require_file(race_levels, "既存race_levels.xlsx")
        steps.append({
            "step": "01_generate_v1_v2",
            "returncode": 0,
            "started_at": "",
            "ended_at": "",
            "elapsed_sec": 0.0,
            "command": "SKIPPED --skip-generate",
            "stdout_tail": "",
            "stderr_tail": "",
        })

    steps.append(run_step(
        "02_compare_v1_v2",
        [py, "-u", str(SCRIPT_COMPARE), "--input", str(race_levels), "--out", str(compare_xlsx)],
    ))

    steps.append(run_step(
        "03_build_confirmed",
        [py, "-u", str(SCRIPT_CONFIRMED), "--input", str(race_levels)],
    ))

    steps.append(run_step(
        "04_evaluate_confirmed",
        [py, "-u", str(SCRIPT_EVAL_CONFIRMED), "--input", str(race_levels), "--out", str(confirmed_eval_xlsx)],
    ))

    build_report(
        source_xlsx=src,
        race_levels_xlsx=race_levels,
        compare_xlsx=compare_xlsx,
        confirmed_eval_xlsx=confirmed_eval_xlsx,
        out_xlsx=final_xlsx,
        out_csv=final_csv,
        steps=steps,
    )


if __name__ == "__main__":
    main()
