from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from app.core.config import get_settings
from app.legacy_bridge.normalization import file_sha256, parse_date
from app.legacy_bridge.prediction_runner import (
    copy_prediction_output,
    execute_prediction_script,
)
from app.services.prediction_golden_master import compare_prediction_workbooks
from app.services.prediction_workspace import prepare_prediction_workspace


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="既存の2段階競馬予想を隔離ワークスペースで実行します。"
    )
    parser.add_argument("--date", required=True, help="対象日（YYYYMMDD）")
    parser.add_argument("--input", default=None, help="入力Excelファイル")
    parser.add_argument("--output-dir", required=True, help="最終Excel出力先")
    parser.add_argument("--run-id", default=None, help="再現・追跡用の実行ID")
    parser.add_argument("--golden-master", default=None, help="比較対象の既存予想Excel")
    parser.add_argument("--tolerance", type=float, default=1e-8, help="数値比較許容誤差")
    parser.add_argument(
        "--keep-workspace",
        action="store_true",
        help="成功後も隔離ワークスペースを保持する",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    race_date = parse_date(args.date)
    if race_date is None or race_date.strftime("%Y%m%d") != args.date:
        print("--date はYYYYMMDDの実在日を指定してください", file=sys.stderr)
        return 2

    settings = get_settings()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    source_workbook = (
        Path(args.input).resolve()
        if args.input
        else (settings.excel_input_dir / f"馬の競走成績_{args.date}.xlsx").resolve()
    )
    if not source_workbook.is_file():
        print(f"入力Excelが見つかりません: {source_workbook}", file=sys.stderr)
        return 2

    run_id = args.run_id or f"prediction-cli-{args.date}-{uuid4().hex[:12]}"
    runtime_root = output_dir / ".keiba_ai_runs"
    cli_settings = settings.model_copy(update={"exports_dir": runtime_root})
    stdout_path = runtime_root / "runs" / run_id / "stdout.log"
    stderr_path = runtime_root / "runs" / run_id / "stderr.log"
    stdout_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        workspace = prepare_prediction_workspace(
            cli_settings,
            run_id=run_id,
            race_date=race_date,
            source_workbook=source_workbook,
        )
        return_code = execute_prediction_script(
            script_path=workspace.script_path,
            workspace_dir=workspace.root,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            race_date=race_date,
            params={},
        )
        if return_code != 0:
            print(
                f"予想処理が失敗しました: return_code={return_code}, stderr={stderr_path}",
                file=sys.stderr,
            )
            return return_code or 1
        if not workspace.expected_output_path.is_file():
            print("予想処理の最終Excelが作成されませんでした", file=sys.stderr)
            return 1

        output_path = output_dir / f"馬の競走成績_with_feat_{args.date}.xlsx"
        copy_prediction_output(workspace.expected_output_path, output_path)
        manifest_output = output_dir / f"prediction_workspace_manifest_{args.date}.json"
        if manifest_output.exists():
            raise RuntimeError(f"マニフェスト出力が既に存在します: {manifest_output}")
        shutil.copy2(workspace.manifest_path, manifest_output)

        golden_result = None
        if args.golden_master:
            golden_result = compare_prediction_workbooks(
                Path(args.golden_master),
                output_path,
                absolute_tolerance=args.tolerance,
            )

        result = {
            "run_id": run_id,
            "race_date": race_date.isoformat(),
            "output_path": str(output_path),
            "output_sha256": file_sha256(output_path),
            "manifest_path": str(manifest_output),
            "input_snapshot_sha256": workspace.manifest["input_snapshot_sha256"],
            "code_bundle_sha256": workspace.manifest["code_bundle_sha256"],
            "golden_master": golden_result.to_dict() if golden_result else None,
            "finished_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if golden_result is not None and not golden_result.passed:
            print("既存予想と重要列が一致しません", file=sys.stderr)
            return 3

        if not args.keep_workspace:
            _remove_successful_workspace(runtime_root, run_id)
        return 0
    except Exception as exc:
        print(f"{exc.__class__.__name__}: {exc}", file=sys.stderr)
        return 1


def _remove_successful_workspace(runtime_root: Path, run_id: str) -> None:
    runs_root = (runtime_root / "runs").resolve()
    run_dir = (runs_root / run_id).resolve()
    # 削除先がruns配下の実行フォルダと確定できる場合だけ掃除する。
    if run_dir.parent != runs_root or run_dir.name != run_id:
        raise RuntimeError("安全に削除できない実行フォルダです")
    if run_dir.is_dir():
        shutil.rmtree(run_dir)


if __name__ == "__main__":
    raise SystemExit(main())
