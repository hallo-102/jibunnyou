---
name: keiba-3month-auto-backtest-after-change
description: 競馬予想コードを変更したあと、過去約3ヶ月分の開催日を自動抽出し、日付ごとに予想コードを実行し、出力Excelと race_levels.xlsx の実績を照合してランキング馬の成績を集計し、前回結果と今回結果を比較して改善・悪化・要確認を判定する。予想ロジック変更後の自動バックテスト、3ヶ月分のランキング精度確認、回収率や的中率の履歴比較、採用可否レポート作成を依頼されたときに使う。
---

# Keiba 3month Auto Backtest After Change

## 1. このSkillの目的

競馬予想コード変更後に、直近約3ヶ月、目安24開催日程度の入力Excelを対象として予想を自動実行し、出力Excelのランキング馬と `data/master/racedata_results.xlsx` の実績を照合する。結果は日別、月別、全体で集計し、過去の評価結果がある場合は変更前後を比較して、採用可否を判断できるレポートとして残す。

## 2. CodexがこのSkillを使うべきタイミング

ユーザーが「予想コードを変更した」「過去3ヶ月を回して」「ランキング成績が改善したか」「前回精度と比較して」と依頼したら、このSkillを使う。対象は主に `keibayosou_best_import_roi_runner.py`、`keibayosou_utils.py`、`keibayosou_pipeline.py`、`keibayosou_penalties.py`、`keibayosou_loaders.py`、`keibayosou_features.py`、`keibayosou_config.py` だが、実ファイル名は存在確認と探索で判断する。

## 3. 実行前チェック

- 既存の予想コードは勝手に変更しない。
- `data/master/racedata_results.xlsx` は結果照合用として読み取り専用で扱う。
- `data/master/race_levels.xlsx` はレースレベルなどの特徴量判断用で、結果照合には使わない。確認専用で、絶対に修正しない。
- `pandas` と `openpyxl` が使える Python を選ぶ。PATH に `python` がない場合はプロジェクトの `.venv` または Codex bundled Python を使う。
- 入力Excel、出力Excel、予想実行コマンドは、まず既存プロジェクト構成から探索し、不明な場合は設定JSONまたは引数で指定する。
- Git作業ツリーに既存変更がある場合は、Skill実行と関係ない変更を触らない。

## 4. 3ヶ月バックテストの流れ

1. `scripts/collect_target_dates.py` で対象日とスキップ日を集める。
2. `scripts/run_3month_predictions.py` で対象日ごとに予想コードを実行する。
3. `scripts/evaluate_3month_results.py` で出力Excelと結果Excelを照合して集計する。
4. 前回評価結果がある場合は `scripts/compare_before_after.py` で比較する。初回は今回結果だけを基準値として保存する。
5. `scripts/make_backtest_report.py` でMarkdown、Excel、CSVの最終レポートを作る。

## 5. 対象日付の決め方

- 開始日と終了日が指定された場合はその範囲を使う。
- 指定がない場合は終了日を実行日、開始日を直近3ヶ月前にする。
- 土日を候補日にする。
- 祝日開催や変則開催を拾うため、範囲内に入力Excelが存在する日付は曜日に関係なく対象にする。
- 土日でも入力Excelがない日はスキップとしてログに残す。
- 平日で入力Excelがない日は候補外として扱い、必要に応じて対象日収集ログで確認する。

## 6. 入力Excelの探し方

既定では `data/input` から `馬の競走成績_{date}.xlsx` と `*{date}*.xlsx` を探す。`{date}` は `YYYYMMDD`。固定しすぎず、`--input-dir`、`--input-patterns`、`--config` で変更する。詳しい列名やファイル名候補は `references/excel_columns.md` を読む。

## 7. 予想コードの実行方法

`run_3month_predictions.py` は、設定された `prediction_command` を最優先する。コマンドには `{date}`、`{input_file}`、`{output_dir}`、`{project_root}`、`{python}` を埋め込める。指定がない場合は `keibayosou_best_import_roi_runner.py` などの候補を探索し、日付、入力Excel、出力ディレクトリを引数として実行する。実プロジェクトのRunner引数が異なる場合は、必ず設定JSONで `prediction_command` を指定する。

## 8. 出力Excelの探し方

既定では `data/output` から `馬の競走成績_with_feat_{date}.xlsx` と `*with_feat*{date}*.xlsx` と `*{date}*.xlsx` を探す。`--output-dir`、`--output-patterns`、`--config` で変更できる。出力が見つからない日付は失敗扱いにして、全体処理は止めない。

## 9. 結果付きExcelとの照合方法

`evaluate_3month_results.py` は出力Excelと `data/master/racedata_results.xlsx` を読み、レースキー、馬キー、予想ランキング、実着順を推定して照合する。列名が違う場合は候補名から推定し、必要なら設定JSONで補助する。`racedata_results.xlsx` と `race_levels.xlsx` は読み取り専用で扱う。

## 10. 評価指標

最低限、対象日数、対象レース数、除外レース数、欠損馬数、ランキング1位の馬券内率、ランキング1から3位の馬券内率、ランキング1から5位の馬券内率、TOP5完全捕捉率、3連複想定的中率、回収率、日別成績、月別成績、最高日、最低日、的中率のばらつき、回収率のばらつきを集計する。詳細な定義は `references/evaluation_rules.md` を読む。

## 11. 変更前後比較の方法

前回の `evaluation_*.json` または最終レポート作成時に保存された履歴を変更前、今回の `evaluation_*.json` を変更後として比較する。比較対象はランキング1位の馬券内率、ランキング1から3位の馬券内率、TOP5完全捕捉率、3連複想定的中率、回収率、対象レース数、除外レース数、欠損馬数、穴馬を拾えた件数、人気馬を危険馬として切れた件数。

## 12. 改善・悪化・要確認の判定基準

- 改善: 主要指標が上がり、対象レース数が大きく減らず、欠損や除外が増えすぎていない。
- 悪化: ランキング1位、TOP5完全捕捉率、回収率が下がる、または除外、欠損が増えた。
- 要確認: 的中率と回収率の方向が分かれる、対象レース数が減っている、一部の日だけ大勝ちして全体を押し上げている、穴馬捕捉と軸馬安定性がトレードオフになっている。

## 13. エラー時の対応

- 1日分のエラーで全体を止めない。
- 失敗日、スキップ日、標準出力、標準エラーをログに残す。
- Excel列が推定できない場合は、そのレースまたは日を除外し、除外理由を記録する。
- 結果Excelが存在しない場合は評価を止めず、レポートに未評価として残す。
- 既存コード修正が必要に見える場合でも、修正理由を明記し、ユーザー確認なしに変更しない。

## 14. 最終レポート形式

`reports/` に以下を出力する。

- `backtest_report_YYYYMMDD_HHMMSS.md`
- `backtest_summary_YYYYMMDD_HHMMSS.xlsx`
- `backtest_daily_YYYYMMDD_HHMMSS.csv`

レポートには対象期間、対象日数、対象レース数、実行成功日、実行失敗日、スキップ日、全体成績、日別成績、月別成績、変更前後比較、改善/悪化/要確認の判定、採用可否、理由、次に改善すべきポイントを含める。履歴比較用に `reports/backtest_history.jsonl` にも追記する。

## 15. Codexへの注意事項

- このSkillを使うときは、まず `SKILL.md` と必要な reference だけを読む。
- 初回実行では変更前比較を無理に作らず、今回評価を基準値として保存する。
- 2回目以降は直近の `evaluation_*.json` またはユーザー指定の変更前ファイルを比較対象にする。
- 日本語ファイル名、Windowsパス、Excelシート名に対応する。
- 実行例は `references/usage_examples.md` を読む。
- スクリプトは単体実行できるが、プロジェクト固有のRunner引数は設定JSONで明示するのが安全。
