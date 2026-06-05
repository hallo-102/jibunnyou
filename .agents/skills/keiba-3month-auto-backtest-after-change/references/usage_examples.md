# 使い方

## 0. Pythonの選び方

以下のコマンドは、必ずプロジェクト直下で実行する。

```powershell
# Skillフォルダ内にいる場合は、先にプロジェクト直下へ移動します。
Set-Location "C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026"
```

プロジェクト `.venv` がある場合はそれを使う。PATH に `python` がない環境では、Codex bundled Python を使う。

```powershell
# プロジェクトの仮想環境Pythonを使う例です。
$PY = ".\.venv\Scripts\python.exe"

# 仮想環境がない場合はCodex bundled Pythonを使う例です。
$PY = "C:\Users\okino\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
```

## 1. 対象日を集める

```powershell
# 直近3ヶ月の入力Excelを探し、対象日とスキップ日をreportsへ保存します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\collect_target_dates.py" `
  --config ".\config\keiba_backtest_config.json"
```

## 2. 初回バックテストを実行する

予想Runnerの引数がプロジェクト固有の場合は、設定JSONで `prediction_command` を指定する。

```powershell
# 設定JSONを指定して、対象日ごとに予想コードを実行します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\run_3month_predictions.py" `
  --project-root "C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026" `
  --config ".\config\keiba_backtest_config.json"
```

## 3. 評価する

```powershell
# 出力Excelとracedata_results.xlsxを照合して、日別、月別、全体成績を作ります。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\evaluate_3month_results.py" `
  --config ".\config\keiba_backtest_config.json"
```

## 4. 前回結果と比較する

```powershell
# 前回のevaluation JSONと今回のevaluation JSONを比較します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\compare_before_after.py" `
  --before ".\reports\evaluation_前回.json" `
  --after ".\reports\evaluation_今回.json"
```

初回は `--before` を省略する。

```powershell
# 初回は今回結果だけを基準値として保存します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\compare_before_after.py" `
  --after ".\reports\evaluation_今回.json"
```

## 5. 最終レポートを作る

```powershell
# Markdown、Excel、CSVの最終レポートをreportsへ出力します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\make_backtest_report.py" `
  --evaluation ".\reports\evaluation_今回.json" `
  --comparison ".\reports\comparison_今回.json"
```

## 設定JSON例

JSONはコメントを書けないため、実ファイルにはコメントを入れない。

```json
{
  "input_dir": "data/input",
  "input_patterns": ["馬の競走成績_{date}.xlsx", "*{date}*.xlsx"],
  "output_dir": "data/output",
  "output_patterns": ["馬の競走成績_with_feat_{date}.xlsx", "*with_feat*{date}*.xlsx", "*{date}*.xlsx"],
  "result_master_path": "data/master/race_levels.xlsx",
  "prediction_command": "{python} keibayosou_best_import_roi_runner.py --date {date} --input \"{input_file}\" --output-dir \"{output_dir}\""
}
```

## 出力の探し方

`reports/` に `target_dates_*.json`、`prediction_run_*.json`、`evaluation_*.json`、`comparison_*.json`、`backtest_report_*.md` が作られる。2回目以降は、直近の `evaluation_*.json` を変更前として使う。
