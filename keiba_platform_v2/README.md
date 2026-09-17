# Keiba Platform V2

既存 `jibunnyou` の予想コード、`etc_py/`、`tokutyouryou_keisann/` を変更せず、`keiba_platform_v2/` だけで独立して動かす競馬予想・オッズ分析・SHADOW検証プラットフォームです。

## 現在の完成範囲

V2単体で次の循環を持ちます。

```text
過去結果の読取専用取込 / V2履歴DB
        ↓
直近N走特徴量
        ↓
LightGBM学習
        ↓
Walk-Forward検証
        ↓
当日netkeiba出馬表 + 発走時刻
        ↓
JRA当日オッズ
        ↓
品質検査
        ↓
予想確率 + 市場確率 + EV
        ↓
36Rから買う価値のあるレースだけ選別
        ↓
単勝 / 馬連 / 3連複 SHADOW候補
        ↓
T-5±30秒で最新オッズ再取得
        ↓
日次・1R上限を適用してSHADOW確定
        ↓
レース結果・払戻取得
        ↓
ROI・損益・馬券種別レポート
        ↓
履歴DB更新 → 次回学習
```

実資金投票はV2では有効化していません。T-5までをSHADOWで完全に検証し、実開催日の通し試験を合格させてから別ゲートとして追加する前提です。

## 既存システムとの分離

- 変更対象は `keiba_platform_v2/` のみです。
- 既存Excel/CSVを取り込む処理は読み取り専用です。
- V2のSQLite、モデル、MLflow、出力はすべて `keiba_platform_v2/data/` 配下へ保存します。
- 既存 `horse_betting` のAUTO/SHADOW処理には依存しません。

## 主な機能

- netkeiba当日出馬表・horse_id・発走時刻取得
- JRA単勝/複勝/馬連/3連複オッズ取得
- JRAページ日付照合
- race_id/馬番/馬名/オッズ/重複/頭数の品質ゲート
- SQLite履歴DB
- 直近N走の平均着順、人気、上り、勝率、3着内率、休養日数などの特徴量
- LightGBM学習とモデル保存
- レース単位・時系列分割による検証
- Walk-Forward評価
- モデル勝率、市場暗黙確率、Edge、Expected Value
- 36Rから0～Nレースを自然選別する価値判定
- Plackett-Luce近似による馬連/3連複の組合せ的中確率
- 実組合せオッズを使った単勝/馬連/3連複EV判定
- T-5±30秒、最大3回再取得
- T-5処理のSQLite冪等化
- 1R最大13点/1,300円、日次最大5,000円のSHADOW上限
- 日次上限は再起動後もSQLiteから累積
- 結果・払戻収集
- SHADOW精算、ROI、損益、馬券種別集計
- MLflow任意記録
- Prefect任意フロー
- Windows PowerShell運用スクリプト
- V2専用Windowsタスク登録/解除

## フォルダ概要

```text
keiba_platform_v2/
├─ config/
│  └─ settings.yaml
├─ scripts/
│  ├─ setup_windows.ps1
│  ├─ run_raceday.ps1
│  ├─ run_t5.ps1
│  ├─ run_results.ps1
│  ├─ retrain.ps1
│  ├─ task_runner.ps1
│  ├─ register_tasks.ps1
│  └─ unregister_tasks.ps1
├─ src/keiba_v2/
│  ├─ collectors/
│  │  ├─ netkeiba.py
│  │  ├─ netkeiba_results.py
│  │  ├─ jra_odds.py
│  │  ├─ schedule.py
│  │  └─ daily.py
│  ├─ adapters.py
│  ├─ legacy_history.py
│  ├─ contracts.py
│  ├─ validation.py
│  ├─ history.py
│  ├─ features.py
│  ├─ training.py
│  ├─ prediction.py
│  ├─ odds.py
│  ├─ race_selector.py
│  ├─ combination.py
│  ├─ strategies.py
│  ├─ t5_runtime.py
│  ├─ results.py
│  ├─ backtest.py
│  ├─ walkforward.py
│  ├─ reporting.py
│  ├─ tracking.py
│  ├─ storage.py
│  ├─ prefect_flow.py
│  ├─ doctor.py
│  ├─ orchestrator.py
│  └─ cli.py
├─ tests/
├─ pyproject.toml
└─ README.md
```

## 1. Windows 11 初回セットアップ

PowerShellでV2フォルダへ移動して実行します。

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_windows.ps1
```

内部でPython 3.11用 `.venv` を作成し、ML/収集/テスト依存をインストールし、Playwright Chromiumとpytestまで実行します。

セットアップ後の診断:

```powershell
.\.venv\Scripts\python.exe -m keiba_v2.cli doctor
```

`ok=true` が必須部分の合格です。`collection_ready`、`ml_ready`、`history_rows`も確認できます。

## 2. 既存結果データをV2履歴へ読み取り専用で投入

既存ファイルそのものは変更しません。

```powershell
.\.venv\Scripts\python.exe -m keiba_v2.cli import-history --input "C:\path\to\racedata_results.xlsx"
```

ファイル名や行から日付を解決できない場合だけ日付を指定します。

```powershell
.\.venv\Scripts\python.exe -m keiba_v2.cli import-history --input "C:\path\to\result.xlsx" --date 20260913
```

horse_idが無い旧データは、馬名を正規化したV2用補助IDで履歴化します。

## 3. 学習データ作成・Walk-Forward・学習

```powershell
.\.venv\Scripts\python.exe -m keiba_v2.cli build-training
.\.venv\Scripts\python.exe -m keiba_v2.cli walkforward --input data\training\training.csv --splits 4
.\.venv\Scripts\python.exe -m keiba_v2.cli train --input data\training\training.csv
```

`walkforward.csv` で未来方向の評価を確認してからモデルを使います。

まとめて実行する場合:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\retrain.ps1
```

## 4. 実開催日の朝

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_raceday.ps1 -RaceDate 20260919
```

主な生成物:

```text
data/raw/netkeiba_entries_YYYYMMDD.csv
data/raw/race_schedule_YYYYMMDD.csv
data/raw/jra_runner_odds_YYYYMMDD.csv
data/raw/jra_combination_odds_YYYYMMDD.csv
data/input/races_YYYYMMDD.csv
data/output/predictions_YYYYMMDD.xlsx
data/output/race_selection_YYYYMMDD.csv
data/output/strategy_bets_YYYYMMDD.json
```

朝時点の予想は参考値です。最終SHADOW判定はT-5で再計算します。

## 5. T-5 SHADOW運転

朝の収集後、開催中は次を起動します。

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_t5.ps1 -RaceDate 20260919
```

各レースについて発走5分前±30秒のみ処理し、取得失敗時は0秒/5秒/10秒の最大3回試行します。

T-5の重要仕様:

- 指定時間より早ければ待機
- ±30秒を過ぎたら `MISSED`
- 同じrace_idの成功済み処理は再実行しない
- JRA日付不一致なら停止
- 出馬表とJRAオッズのrace_id/馬番を一致確認
- 1R最大13点/1,300円
- 日次最大5,000円
- 日次上限はSQLite上の成功済みT-5券から再計算するため再起動でも維持
- 現段階はSHADOWのみ

## 6. レース後

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_results.ps1 -RaceDate 20260919
```

実施内容:

1. netkeiba結果・払戻取得
2. V2履歴SQLiteへ追加
3. T-5 SHADOW券だけを精算
4. 学習データ再生成
5. 集計レポート更新

朝時点の `strategy_bets_YYYYMMDD.json` はプレビュー扱いで、KPIにはT-5確定券のみを含めます。

個別に集計する場合:

```powershell
.\.venv\Scripts\python.exe -m keiba_v2.cli report --directory data\output --output data\output\performance_report.xlsx
```

## 7. Windowsタスクスケジューラへの登録

V2専用タスクとして土日だけ登録します。既存タスクは変更しません。

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\register_tasks.ps1
```

既定時刻:

```text
KeibaV2_Morning  08:20
KeibaV2_T5       09:00
KeibaV2_Results  19:00
```

実行ログは次に残します。

```text
data/runtime/task_logs/YYYYMMDD_morning.log
data/runtime/task_logs/YYYYMMDD_t5.log
data/runtime/task_logs/YYYYMMDD_results.log
```

解除:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\unregister_tasks.ps1
```

祝日・平日開催はこの土日登録には含めません。その場合は `task_runner.ps1` または各 `run_*.ps1` を当日手動実行します。

## 8. 手動CSV/Excel入力から予想する場合

最低限必要な列:

- `race_id`
- `horse_no`
- `horse_name`
- `win_odds`

検査:

```powershell
.\.venv\Scripts\python.exe -m keiba_v2.cli validate --input data\input\races.csv
```

予想:

```powershell
.\.venv\Scripts\python.exe -m keiba_v2.cli run --input data\input\races.csv --date 20260919
```

## 9. KPI

V2は的中率だけで採用判断しません。優先するのは次です。

1. Walk-Forward ROI
2. 投資額・払戻・利益
3. 最大損失の偏り
4. 馬券種別別ROI
5. 購入レース数
6. Top1/Top3/Top5精度

EV閾値は `config/settings.yaml` で管理します。

## 10. 完成判定

ソフトウェア側のSHADOW機能は、以下を満たすことを完成条件とします。

- `scripts/setup_windows.ps1` が成功
- pytestが全件成功
- `keiba-v2 doctor` の必須項目が成功
- 過去結果取込→学習→Walk-Forwardが成功
- 実開催日に朝収集が成功
- 単勝/馬連/3連複オッズが取得され、EV計算へ渡る
- 全レースのT-5処理が `SUCCESS` または意図した `NO_BET` で終了
- race_id/馬番/日付不一致時に停止する
- 日次5,000円上限を再起動後も超えない
- レース後の結果取得と全T-5 SHADOW券精算が成功
- performance_report.xlsx が生成される

実資金LIVEはこの完成条件とは分離します。SHADOWで十分な期間の再現性を確認した後に、独立したLIVEゲートとして設計します。
