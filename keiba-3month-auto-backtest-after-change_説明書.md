# keiba-3month-auto-backtest-after-change 説明書

## 目的

`keiba-3month-auto-backtest-after-change` は、競馬予想コードを変更したあとに、過去約3ヶ月分の開催日で予想を再実行し、変更前の評価結果と比較するためのスキルです。

このスキルを使うと、次の判断材料をまとめて確認できます。

- 変更後の予想精度が上がったか
- 回収率が改善したか
- TOP5で3着以内馬を捕捉できているか
- 欠損馬や除外レースが増えていないか
- 変更を採用してよいか、見送るべきか

## 使うタイミング

次のような依頼をするときに使います。

- 予想ロジックを変更したので、変更前と比較したい
- 直近3ヶ月のバックテストを回したい
- ランキング1位、TOP5、3連複想定的中率、回収率を確認したい
- 前回の `evaluation_*.json` と今回結果を比較したい
- 採用候補、見送り候補、要確認の判定を出したい

## 対象ファイル

主に次のファイルやディレクトリを使います。

| 用途 | 既定パス |
| --- | --- |
| スキル本体 | `.agents/skills/keiba-3month-auto-backtest-after-change/` |
| 設定ファイル | `config/keiba_backtest_config.json` |
| 入力Excel | `data/input/馬の競走成績_YYYYMMDD.xlsx` |
| 予想出力Excel | `data/output/馬の競走成績_with_feat_YYYYMMDD.xlsx` |
| 結果照合Excel | `data/master/racedata_results.xlsx` |
| レースレベルExcel | `data/master/race_levels.xlsx` |
| レポート出力先 | `reports/` |

`race_levels.xlsx` は特徴量判断用の参照ファイルです。結果照合には `racedata_results.xlsx` を使います。

## 処理の流れ

スキルは次の順番で処理します。

1. 対象日を集める
2. 対象日ごとに予想コードを実行する
3. 予想出力Excelと実績Excelを照合して評価する
4. 前回評価と今回評価を比較する
5. Markdown、Excel、CSVの最終レポートを作る
6. `reports/backtest_history.jsonl` に履歴を追記する

## 事前確認

実行前に次を確認します。

- `.venv/Scripts/python.exe` が使えること
- `pandas` と `openpyxl` が使えること
- `data/master/racedata_results.xlsx` が存在すること
- 対象日の入力Excelが `data/input` にあること
- 対象日の `data/ozzu_csv/OZZU_YYYYMMDD.csv` が必要な場合は存在すること
- 変更前として使う `reports/evaluation_*.json` があること

既存の予想コードやマスタExcelは、このスキル実行中に勝手に修正しません。

## 設定ファイル

このプロジェクトでは、基本的に `config/keiba_backtest_config.json` を使います。

重要な設定は次の通りです。

| 設定キー | 意味 |
| --- | --- |
| `python` | 実行に使うPython |
| `months` | 指定がない場合に何ヶ月分さかのぼるか |
| `input_dir` | 入力Excelの場所 |
| `input_patterns` | 入力Excelのファイル名パターン |
| `output_dir` | 予想出力Excelの場所 |
| `output_patterns` | 予想出力Excelのファイル名パターン |
| `result_master_path` | 結果照合に使う実績Excel |
| `runner_script` | 予想Runner |
| `prediction_command` | 日付ごとの予想実行コマンド |
| `sanrenpuku_box_size` | 3連複想定の対象頭数 |
| `sanrenpuku_bet_points` | 1レースあたりの想定買い目数 |
| `stake_yen` | 1点あたりの金額 |

このプロジェクトのRunnerは `input()` で日付を受け取るため、`prediction_command` は `echo {date}` で日付を標準入力に流す設定になっています。

## 基本コマンド

以下はプロジェクト直下で実行します。

```powershell
# プロジェクト直下へ移動します。
Set-Location "C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026"

# プロジェクトの仮想環境Pythonを変数に入れます。
$PY = ".\.venv\Scripts\python.exe"
```

### 1. 対象日を集める

```powershell
# 直近約3ヶ月の入力Excelを探し、対象日JSONとCSVをreportsへ出力します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\collect_target_dates.py" `
  --config ".\config\keiba_backtest_config.json"
```

期間を固定したい場合は、開始日と終了日を指定します。

```powershell
# 変更前評価と同じ日付範囲で比較したい場合は、期間を明示します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\collect_target_dates.py" `
  --config ".\config\keiba_backtest_config.json" `
  --start-date 20260307 `
  --end-date 20260531
```

### 2. 対象日ごとに予想を実行する

```powershell
# 対象日ごとに予想Runnerを実行し、日別ログと実行結果JSONをreportsへ出力します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\run_3month_predictions.py" `
  --project-root "C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026" `
  --config ".\config\keiba_backtest_config.json"
```

対象日マニフェストを固定したい場合は、`--target-dates-file` を指定します。

```powershell
# 事前に作った対象日JSONだけを使って予想を実行します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\run_3month_predictions.py" `
  --project-root "C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026" `
  --config ".\config\keiba_backtest_config.json" `
  --target-dates-file ".\reports\target_dates_YYYYMMDD_HHMMSS.json"
```

### 3. 予想結果を評価する

```powershell
# 予想出力Excelとracedata_results.xlsxを照合し、全体、日別、月別の評価を作ります。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\evaluate_3month_results.py" `
  --config ".\config\keiba_backtest_config.json"
```

比較日付を厳密にそろえる場合は、評価対象の予想Excelを明示します。

```powershell
# 余分な日付が混ざらないよう、評価対象Excelを明示します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\evaluate_3month_results.py" `
  --config ".\config\keiba_backtest_config.json" `
  --prediction-files ".\data\output\馬の競走成績_with_feat_20260307.xlsx" ".\data\output\馬の競走成績_with_feat_20260308.xlsx"
```

### 4. 変更前後を比較する

```powershell
# 変更前のevaluation JSONと変更後のevaluation JSONを比較します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\compare_before_after.py" `
  --config ".\config\keiba_backtest_config.json" `
  --before ".\reports\evaluation_変更前.json" `
  --after ".\reports\evaluation_変更後.json"
```

初回で変更前評価がない場合は、`--before` を省略します。

```powershell
# 初回は今回結果だけを基準値として保存します。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\compare_before_after.py" `
  --config ".\config\keiba_backtest_config.json" `
  --after ".\reports\evaluation_今回.json"
```

### 5. 最終レポートを作る

```powershell
# Markdown、Excel、日別CSVの最終レポートを作ります。
& $PY ".agents\skills\keiba-3month-auto-backtest-after-change\scripts\make_backtest_report.py" `
  --config ".\config\keiba_backtest_config.json" `
  --evaluation ".\reports\evaluation_変更後.json" `
  --comparison ".\reports\comparison_変更後.json"
```

## 主な出力ファイル

| 出力 | 内容 |
| --- | --- |
| `target_dates_*.json` | 対象日、スキップ日、候補外日 |
| `prediction_run_*.json` | 日付ごとの予想実行結果 |
| `evaluation_*.json` | 評価結果の本体 |
| `evaluation_daily_*.csv` | 日別評価 |
| `evaluation_monthly_*.csv` | 月別評価 |
| `evaluation_races_*.csv` | レース単位の評価 |
| `comparison_*.json` | 変更前後比較の本体 |
| `comparison_*.md` | 比較結果のMarkdown |
| `backtest_report_*.md` | 最終レポート |
| `backtest_summary_*.xlsx` | Excelサマリー |
| `backtest_daily_*.csv` | 最終日別CSV |
| `backtest_history.jsonl` | バックテスト履歴 |

## 評価指標

主な評価指標は次の通りです。

| 指標 | 見ること |
| --- | --- |
| 対象日数 | 評価できた開催日の数 |
| 対象レース数 | 実績照合までできたレース数 |
| 除外レース数 | 予想または結果不足で評価できなかったレース数 |
| 欠損馬数 | 予想上位馬が結果側で照合できなかった件数 |
| ランキング1位の馬券内率 | 予想1位馬が3着以内に入った割合 |
| ランキング1から3位の馬券内率 | 予想1から3位馬の3着以内率 |
| ランキング1から5位の馬券内率 | 予想1から5位馬の3着以内率 |
| TOP5完全捕捉率 | 実際の1から3着馬すべてが予想TOP5内に入った割合 |
| 3連複想定的中率 | TOP指定頭数内に実際の1から3着馬がすべて入った割合 |
| 回収率 | 的中時払戻合計を購入想定金額で割った値 |
| 穴馬を拾えた件数 | 7番人気以下で3着以内の馬をTOP5に入れた件数 |
| 人気馬を危険馬として切れた件数 | 1から3番人気で4着以下の馬をTOP5外にできた件数 |

## 判定の考え方

比較結果は、主に次の3段階で判定します。

| 判定 | 意味 |
| --- | --- |
| 改善 | 主要指標が複数改善し、対象レース数、除外、欠損に大きな悪化がない |
| 悪化 | 軸馬精度、TOP5捕捉、回収率が下がる、または除外や欠損が増える |
| 要確認 | 的中率と回収率の方向が分かれる、母数が変わる、一部の日だけで押し上げている |

採用可否は、`採用候補`、`見送り候補`、`要確認`、`初回基準として保存` のように出力されます。

## 比較時の注意点

変更前後を正しく比較するには、日付セットと対象レース数をできるだけそろえます。

例えば、変更前が 2026-03-07 から 2026-05-31 の26日分なら、変更後も同じ26日分で評価します。今日基準の直近3ヶ月で走ると、6月分が入り、3月前半が抜けることがあります。この場合は、日付範囲や `--prediction-files` を明示して比較条件をそろえます。

## よくある失敗と対応

| 状況 | 確認すること |
| --- | --- |
| 対象日が0日になる | `data/input` に `馬の競走成績_YYYYMMDD.xlsx` があるか確認する |
| 予想実行が失敗する | 対象日の `OZZU_YYYYMMDD.csv` があるか、Runnerが日付を受け取れるか確認する |
| 出力Excelが見つからない | `output_patterns` と実際のファイル名が合っているか確認する |
| 評価で除外が増える | レースID、馬名、馬番、着順列が照合できているか確認する |
| 回収率が未計算になる | `racedata_results.xlsx` に三連複の払戻行があるか確認する |
| 比較結果が不自然 | 変更前後の日付セット、対象レース数、除外レース数が同じか確認する |

## 実行後に見るべきファイル

まず見るべきファイルは次の3つです。

1. `reports/backtest_report_*.md`
2. `reports/comparison_*.md`
3. `reports/backtest_summary_*.xlsx`

詳細調査が必要な場合は、`evaluation_races_*.csv` でレース単位の評価を確認します。

## 運用メモ

- `racedata_results.xlsx` と `race_levels.xlsx` は確認専用で、修正しません。
- 1日分が失敗しても、全体処理は止めずに失敗日として記録します。
- 初回は比較対象がないため、今回結果を基準値として保存します。
- 2回目以降は、直近または指定した `evaluation_*.json` を変更前として使います。
- レポート作成時に `reports/backtest_history.jsonl` へ履歴が追記されます。

