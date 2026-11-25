# 3者ソース差分（2026-11-25）

基準main: `178855fd117301131c8e3ebf70ce8c5e9e327728`。Windowsは `keiba_yosou_2026-2`、NASはローカル `keiba_yosou_2026_nas` の静的コピー。稼働中NASの実体は未確認。候補の共通11ファイル・モデル設定・重みはmainのバイトをそのまま採用する。

表のSHAはバイトSHA256先頭12桁。**完全なバイトSHA256とLF正規化SHA256、取得パス、採用元・理由は [source_inventory.json](source_inventory.json)** に全件保存。LF正規化はCRLF/CR→LFのみ。存在しないものはnull。

| 共通パス | main SHA | Windows | NAS SHA | 採用理由 |
|---|---|---|---|---|
| `1_keibayosou_best_import_roi_runner.py` | `7a3ca48bdd06` | バイト一致 | `4c41ed644a10` | GitHub共通正本 |
| `1_keibayosou_config.py` | `4f61440cf498` | バイト一致 | `c7a3abfe6055` | GitHub共通正本 |
| `1_keibayosou_course_style.py` | `9be3cd487bfa` | バイト一致 | `9be3cd487bfa` | GitHub共通正本 |
| `1_keibayosou_features.py` | `d8bee5931206` | バイト一致 | `3e04076692df` | GitHub共通正本 |
| `1_keibayosou_loaders.py` | `96ff2bcb832f` | LFのみ一致 | `3a6369cb6284` | GitHub共通正本 |
| `1_keibayosou_pace.py` | `6c24c392fe9a` | バイト一致 | `6c24c392fe9a` | GitHub共通正本 |
| `1_keibayosou_penalties.py` | `9b7492932ef0` | バイト一致 | `846c5b70c9dc` | GitHub共通正本 |
| `1_keibayosou_pipeline.py` | `34afa42fc1b3` | バイト一致 | `d9769d89a6d3` | GitHub共通正本 |
| `1_keibayosou_ranking.py` | `a5b0c9804d00` | バイト一致 | `なし` | GitHub共通正本 |
| `1_keibayosou_utils.py` | `9561860c23c4` | LFのみ一致 | `3512930aafe2` | GitHub共通正本 |
| `model_registry.py` | `146fdf01a9fa` | バイト一致 | `なし` | GitHub共通正本 |
| `config/production_model.json` | `f1ef2923558b` | バイト一致 | `なし` | GitHub共通正本 |
| `config/forward_validation.json` | `0a1cb477a569` | バイト一致 | `なし` | GitHub共通正本 |
| `yosou_py/best_feature_weights_20260817.py` | `bb086384eb58` | バイト一致 | `なし` | GitHub共通正本 |


## 機能差と採用判断

| 機能 | NAS静的コピー | GitHub main / 候補 | 採用・理由 |
|---|---|---|---|
| 重み選択 | configの20260730固定指定、dailyの固定存在チェック | registryの実体/実効SHAと候補設定を二重検証 | 共通mainを直接import。正式NASは変更しない |
| 順位 | 表示scoreをdense順位化、同順位あり | score_raw→risk_score降順→extra_penalty昇順→data_confidence降順→馬番昇順 | mainの生成・検証をそのまま使用 |
| 過去特徴量 | 古いNOW特徴量が優先され得る、ペース/馬場分類に旧実装 | #9経由の再計算優先・ペース正規化・良/不良排他 | main採用。#6〜#8を重複適用しない |
| パス | KEIBA_BASE_DIR、INPUT/OUTPUT/MASTER/ODDS/WEIGHTS_DIR等 | mainにはWindowsのDATA_ROOTとCODE_DIRの既定値 | pipelineへ全入力パスを明示。SUCCESS_REPORTだけrun内へ差し替え |
| Excel | 全レース買い目行、対象外理由、B〜G/L〜R完成検査 | main単体は対象外レース行を出さない | NASの補完・検査をnas/scriptsへ取得。計算値/買い目は再実装しない |
| 市場情報 | NAS runnerで補完 | PR #10のcomplete_market_columns | 既存runnerの正規化と買い目関数を再利用 |
| 回顧 | 旧dense保存原本 | PR #11はscore/馬番で回顧だけ補正 | 取得したreviewを再利用。新予想のraw-score順位と区別 |
| 日次/開催判定 | scripts.run_scheduler / run_daily_pipeline / jra_raceday | 候補は固定入力の一回実行、任意の固定カレンダー | 元スケジューラーは温存。候補から収集/本番ジョブを実行しない |
| READY/STATUS | data/output/status、既存Drive同期 | runごとのREADY.json/STATUS.json/log、sync_plan.json | project/volume/パス/配信を分離 |
| マスタ更新 | scripts.update_masters / maintenance | 読取専用の固定コピーのみ | 候補に正式マスタ更新経路を持たせない |
| 依存 | 収集用Playwright/Selenium、torch等 | pandas/numpy/openpyxl/requests/torch/tzdata | torchは既存runner importに必要。GUI/COM/pywin32は除外 |

## 関連PRの着手時確認

| PR | 状態 / head SHA | 判断 |
|---|---|---|
| #10 | open Draft / c7fdf3397c01353e4cf162f13722bbd3ca0facf7 | PR #11にも含まれるarchive/runtime_metadata/shadowのみ取得。運用service/bootstrap/optimizer/正式マスタ更新は取り込まない |
| #11 | open / 46dd7da1c528214d670099e26a23c5417ee6df94 | reviewを取得。回顧補正の原本保全テスト追加 |
| #5 | open / f9e11aa79ed4739492b37c91605270c39d66d654 | 設定案を確認。既存世代の開始日を変更せず未統合 |
| #4 | open / 029fdaf3a13c4f2ffb13383f1f365d873f0ac868 | offline/前向き分離の方針を守る。条件付き順位は取り込まない |
| #6 | closed 未merge / 41ccbc5fbedaf320df8372e207fc92f7578b47b3 | #9統合に含まれる |
| #7 | closed 未merge / 237c7f5c166341a2efb98bd82b04a4f1a913f96f | #9統合に含まれる |
| #8 | closed 未merge / bb2e9e300b7c382f221e297ede7a9873de4d3a2d | #9統合に含まれる |
| #9 | merged / 3a2caba2e3ddba87dcb1b505728a8560b36dcf52 | mainの特徴量修正を採用。実データ差分の確認は今回も別途記録 |

PR全体の自動merge/cherry-pickはしない。取得ファイル単位の元SHAは [reused_sources.json](reused_sources.json)。#10の古いbase由来で見えるrace_purchase_filter等の削除も持ち込まない。Issue検索 `repo:hallo-102/jibunnyou is:issue NAS` では本Issueのみが該当し、#12/#14は対象外。

## NAS追加取得・静的/実行時の区別

ローカルコピーのapp、scripts、compose、Dockerfile、requirementsから取得した範囲のみを根拠とする。認証ファイルや.envは読んでいない。稼働中コンテナのimage、mount、envの実値、cron、ログ、READY/STATUS、Driveは未確認。後続担当は稼働中サービスを変更せず、許可された端末で対象ファイルの `sha256sum`、`docker compose ps`、`docker inspect` のimage/mount等の必要項目、当日の状態ファイルとログを読み取り、ローカル静的SHAと突合する。env全出力は秘密情報を含むため保存・共有しない。ソースは個別の必要ファイルだけを取得し、本番root全体や認証・生データをGitHubへ転送しない。
