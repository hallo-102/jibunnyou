# Keiba AI Studio 統合仕様書

> **2026-07-23 現在の正式AI連携仕様:** Keiba AI StudioはChatGPT用予想プロンプトを生成・編集・コピーし、既定ブラウザでChatGPTを開く。貼り付けと送信、回答のアプリへの取り込みは利用者が手動で行う。OpenAI API予想は廃止済みで、過去履歴だけを保持する。自動ログイン・自動貼り付け・自動送信・回答自動取得は行わない。

- 版数: 1.0
- 作成日: 2026-07-10
- 状態: Ver.1.0実装・受入照合済み
- 原典: 指定設計フォルダ内の全Markdown 27件
- 詳細調査: [02_設計仕様書調査結果.md](./02_設計仕様書調査結果.md)

## 1. システム全体像

Keiba AI Studioは、既存Python予想を再現可能な数値的基準として保持し、Python順位を見ない生成AI独立分析、両者の比較・統合、買い目候補、結果照合、成績分析、回顧、継続改善を一つのWebアプリで運用する個人向け競馬分析システムである。

```text
ブラウザ
  ↓
Nginx Gateway（外部公開は既定18080のみ）
  ├─ Next.js Web UI
  └─ FastAPI
       ├─ PostgreSQL（正式データ・履歴・監査）
       ├─ Redis（キュー・ロック・進捗）
       ├─ Celery Worker / Beat
       ├─ Collector Adapter（外部データ）
       ├─ Legacy Prediction Adapter（既存Python・Excel互換）
       └─ Generative AI Adapter（独立分析→比較→統合）

NAS/ローカル永続領域
  ├─ Raw
  ├─ Staging / Normalized
  ├─ Exports / Artifacts
  ├─ Logs
  └─ Backups
```

## 2. 利用者

- Ver.1.0は単一管理者による個人利用を前提とする。
- Windows PCを全機能の主対象とし、NASはDocker実行・保存・APIを担当する。
- タブレットとスマートフォンは閲覧、買い目確認、結果確認を中心に対応する。
- 公開範囲は家庭内LAN、VPN、またはNginx Basic認証通過端末に限定する。
- アプリ内ユーザー登録、複数ロールの本実装、外部公開はVer.1.0対象外とする。

## 3. 利用目的

1. 開催日と対象レースを選ぶ。
2. 出馬表、過去走、オッズ、馬場、天候、必要な定性情報を取得・検査する。
3. 既存Python予想を再現可能な条件で実行する。
4. Python最終順位を伏せた状態で生成AI独立分析を実行する。
5. 独立分析確定後にPython結果を比較し、一致・不一致・反対材料を確認する。
6. 統合順位、信頼度、不確実性、根拠を作る。
7. 予算と点数上限内で買い目候補を作り、人が最終判断する。
8. 実結果・払戻を取り込み、的中率、回収率、ドローダウン等を検証する。
9. 外れ原因を分類し、変更案をバックテスト・Shadow Mode経由で改善する。

## 4. 機能一覧

| ID | 機能 | 概要 | Ver.1.0 |
|---|---|---|---|
| F-001 | ホーム/ダッシュボード | 開催、取得、予想、AI、結果、KPI、稼働状態、次操作を表示 | 必須 |
| F-002 | 開催日・レース選択 | 日付・競馬場・条件・状態で対象を選択 | 必須 |
| F-003 | 外部データ取得 | 出馬表、過去走、オッズ、馬場、結果等をAdapterで取得 | 必須 |
| F-004 | データ品質 | race_id、馬番、馬名、日付、欠損、鮮度、取消を検査 | 必須 |
| F-005 | Python予想 | 既存2段階Pipelineを互換実行し順位・score・確率・危険馬を保存 | 必須 |
| F-006 | AI独立予想 | Python最終結果を伏せ、定性・仮説分析と独自順位を作成 | 必須 |
| F-007 | Python/AI比較 | 順位差、理由差、一致、不一致、反対意見を表示 | 必須 |
| F-008 | 統合予想 | 統合score・順位・信頼度・不確実性・根拠を保存 | 必須 |
| F-009 | 買い目作成 | Python案、AI統合案、手動案、見送り、資金・点数制御 | 必須 |
| F-010 | 予想履歴 | run、モデル、入力、結果、差分、成果物を履歴表示 | 必須 |
| F-011 | 結果・払戻 | 結果取込、照合、精算、回顧を保存 | 必須 |
| F-012 | 成績分析 | 的中率、回収率、損益、最大連敗、ドローダウン、条件別比較 | 必須 |
| F-013 | モデル/設定 | Python/AI/prompt/重みを版固定し、画面で買い目閾値を変更 | 必須。直接編集は後続 |
| F-014 | ジョブ管理 | 非同期実行、進捗、再試行、取消、ログ、二重実行防止 | 必須 |
| F-015 | 通知 | critical/error/warningを画面alert、品質、ジョブ、logで通知 | 必須。専用centerは後続 |
| F-016 | データ管理 | Raw/Normalized/Artifact、鮮度、取込履歴、バックアップ | 必須 |
| F-017 | ヘルプ | 用語、操作手順、初心者向け次操作、復旧案内 | 必須 |
| F-018 | バックテスト | Python変更時の3か月回帰。AI/統合の横断比較は後続 | 条件付き必須 |
| F-019 | Windows Companion | Codex App Serverとの分離連携 | 後続。AI Adapterが満たせる場合は代替可 |
| F-020 | 自動投票 | 実投票 | 対象外・既定無効 |

## 5. 非機能要件

### 5.1 性能

- 最新改訂の画面目標: 初期画面・一覧2秒以内、レース切替1秒以内、フィルター300ms以内、馬詳細500ms以内。
- 既存非機能の許容上限: 通常画面5秒、1日分Python予想30分以内。
- 500ms超の処理はローディングを表示し、長時間処理は非同期ジョブにする。
- 本番Python予想、外部データ取得、バックテストは原則同時実行数1とする。

### 5.2 可用性・復旧

- Codex/生成AI障害時もPython予想と履歴閲覧を継続できる。
- PostgreSQL等のRPOは最大24時間、run成果物とAI結果は原則0時間を目標とする。
- 主要サービスのRTOは60分以内とする。
- NAS/Docker再起動後に主要コンテナが自動復旧する。

### 5.3 保存・保持

- 入力、オッズ、出力Excel、JSON、AI分析、買い目履歴は最低2年保持する。
- 結果・払戻とGit履歴は原則削除しない。
- 実行ログは最低90日保持する。
- 重要Artifactは上書きせず、新しいrun_id/analysis_idを作る。

### 5.4 互換性

- 既存CLI、Excelシート、Python 2段階Pipeline、DL、オッズ連携を初期移行中は維持する。
- 同じ入力・設定・重み・モデル・コードでは重要結果が一致しなければならない。
- 日本語Windowsパス、OneDrive、UTF-8、NAS Linuxパスの双方を考慮する。

### 5.5 UX/アクセシビリティ

- 結論、主要理由、詳細の3段階で情報を表示する。
- 色だけで状態を伝えず、文言・アイコン・バッジを併用する。
- キーボード操作、フォーカス、スクリーンリーダーラベル、文字拡大を考慮する。
- 欠損、0、未取得、対象なし、計算不能、取得失敗、未更新を区別する。

## 6. 画面一覧と遷移

| 画面 | 主目的 |
|---|---|
| ホーム | 状態と次の操作 |
| 開催日ダッシュボード | 全レースの重要度と進行状況 |
| レース分析 | 結論、Python、AI、統合、展開、買い目 |
| 出走馬詳細ドロワー | 能力・適性・状態・根拠・欠損 |
| AI分析 | 独立分析、各観点、反対意見 |
| Python/AI比較 | 順位差・理由差・不一致レベル |
| 買い目作成 | 複数案、点数、金額、警告、確認 |
| 結果・回顧 | 結果、照合、外れ原因、改善仮説 |
| 検証ダッシュボード | KPI、推移、条件別分析 |
| モデル比較 | ベースライン、AI、統合、変更案比較 |
| データ管理 | 取込、鮮度、品質、Artifact |
| ジョブ管理 | 状態、進捗、再実行、ログ |
| 設定 | 一般、予想、資金、データ、開発者設定 |
| ログ・通知 | エラー、監査、復旧案内 |
| ヘルプ | 用語、ガイド、運用手順 |

```text
ホーム → 開催日 → レース分析
                   ├→ 出走馬詳細
                   ├→ AI独立分析 → Python比較 → 統合
                   └→ 買い目作成 → 最終確認 → 保存
結果取込 → 結果・回顧 → 検証 → モデル比較 → 改善候補
```

## 7. API一覧（統合版）

### 7.1 共通

- ベースURL: `/api/v1`
- JSONキー: snake_case
- 日付: `YYYY-MM-DD`
- 日時: ISO 8601（タイムゾーン付き）
- `race_id`: 文字列
- 変更系操作: Idempotency-Key、trace_id、監査ログ
- エラー: 初心者向けmessage、code、recommended_action。秘密値と生スタックトレースを返さない。

### 7.2 主要リソース

| 分類 | 主要API |
|---|---|
| Health | `GET /health`, `GET /ready`, `GET /version` |
| Race | `GET /race-days`, `GET /race-workbooks`, `POST /race-workbooks/select`, `GET /races`, `GET /races/{race_id}` |
| Jobs | `POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/retry`, `POST /jobs/{id}/cancel` |
| Quality | `GET /data-quality`, `POST /data-quality/recheck` |
| Python | `POST /predictions/python`, `GET /prediction-runs`, `GET /races/{id}/python-predictions` |
| AI独立 | `POST /ai/independent-analysis`, `GET /ai/runs/{id}` |
| 比較・統合 | `POST /ai/comparisons`, `POST /predictions/integrate`, `GET /races/{id}/comparison` |
| Bets | `POST /bet-previews`, `POST /bets`, `GET /bets`, `PATCH /bets/{id}` |
| Results | `POST /result-imports`, `GET /races/{id}/results`, `POST /settlements` |
| Analytics | `GET /analytics/dashboard`, `POST /backtests`, `GET /backtests/{id}/metrics` |
| Settings | `GET /settings/active`, `GET /settings/versions`, `POST /settings/activate` |
| Notifications | `GET /notifications`, `GET /notifications/summary`, `PATCH /notifications/{id}/read`, `POST /notifications/read-all` |
| Artifacts | `GET /runs/{id}/artifacts`, `GET /artifacts/{id}/download` |

表は論理API一覧である。実経路は`/api/openapi.json`を正とし、Settingsの直接変更、Artifact download、横断backtest APIは後続とする。専用NotificationsはVer.1.1で実装済み。既存APIは互換エンドポイントとして保持する。

## 8. データベース一覧

### 8.1 中核

- races, race_entries, horses
- prediction_runs, prediction_races, prediction_entries
- feature_snapshot_values, odds_snapshots
- data_quality_checks, artifact_files, job_runs, job_logs

### 8.2 AI・統合

- ai_analysis_runs: 独立/比較/統合のrunと入力可視範囲
- ai_agent_results: 観点別結果
- ai_horse_evaluations: AI独自順位、根拠、自信度、不確実性
- ai_python_comparisons: Python/AI差分と反対意見
- final_predictions: 統合score・順位・根拠
- evidence_records: 事実、推定、仮説、不足と出典
- model_versions, prompt_versions

### 8.3 買い目・結果・分析

- bet_recommendations, bet_legs, bet_status_history
- race_results, race_result_entries, payout_results, bet_settlements
- reviews, review_causes
- backtest_runs, backtest_variants, backtest_metrics, backtest_race_evaluations

### 8.4 運用

- audit_logs, config_versions, feature_weight_versions, notifications
- user_race_notes, ui_preferences（単一ユーザーでも設定保持に使用）

Ver.1.1ではnotificationsを通知正本とし、failed job_runsとwarning/errorのdata_quality_issuesを重複なく永続化する。既読状態と履歴はnotificationsに保持し、外部メール等の配信設定は後続とする。上記は論理名を含み、実テーブル名との対応はAlembic migrationとSQLAlchemy modelを正とする。

すべての履歴性データは原則論理削除または不変Artifactとして扱う。確率はDB内部0〜1、金額は整数円、日時はTIMESTAMPTZを標準とする。

## 9. 外部連携一覧

| 連携 | 方針 |
|---|---|
| JRA等の公式情報 | 一次情報を優先。規約・頻度・キャッシュを守り、制限回避をしない |
| 既存Excel/CSV | Raw保存後に正規化・照合し、既存形式を維持する |
| 既存Python | 読み取り専用legacy + Adapter + subprocess/正式入口 |
| 生成AI | Provider Adapter。独立入力と比較入力を物理的・論理的に分離 |
| Windows Companion | 必要な場合のみ端末トークンでNAS APIへアウトバウンド接続 |

## 10. バッチ・ジョブ一覧

- collection.race_info / past_performances / odds / training / results
- prediction.feature_generation / prediction.python / prediction.risk_evaluation
- ai.independent_analysis / ai.compare / ai.integrate
- bet.generate
- result.settlement / result.review_generation
- analytics.aggregate / backtest.run
- maintenance.data_quality_check / backup / retention（Ver.1.0のbackup/retentionはWindows運用script）

Ver.1.0の実装状態は`queued → running → completed|failed`を中核とし、買い目のreview/blocked/cancelledは別状態機械で扱う。ロック、冪等性、上限付き再試行、手動再実行理由を実装する。追加のwaiting/completed_with_warningsは後続拡張とする。

## 11. AI処理一覧と独立分析契約

```text
第1段階: AI独立分析
  入力: レース・出走馬・定性情報・Python最終判断に依存しない基礎データ
  禁止: Python最終順位、最終score、印、買い目、買い/見送り判定
  出力: AI独自順位、評価、根拠、リスク、不明点、自信度、順位範囲

第2段階: 独立結果の固定
  独立結果のハッシュと時刻を保存し、後から上書きしない

第3段階: Python比較
  Python結果を初めて開示し、一致・不一致・理由差・反対材料を整理

第4段階: 統合
  Python/AIのどちらを何の理由で優先したか、統合順位、信頼度、不確実性を保存
```

AIは情報不足を明示できること、存在しない馬番・根拠を拒否すること、自由文だけでなくJSON Schemaに適合することを必須とする。差分を無理に捏造しない。

## 12. 主要処理フロー

### 12.1 データ取得

```text
取得 → Raw不変保存 → 正規化 → race_id/馬番/馬名照合 → 品質検査
→ Normalized保存 → Prediction Input作成 → Artifact登録
```

### 12.2 予想

```text
品質ゲート → Python予想 → AI独立分析 → 独立結果固定
→ Python比較 → 統合 → 買い目プレビュー → 人の確認 → 保存
```

### 12.3 結果照合

```text
結果・払戻取得 → 確定状態確認 → 予想/買い目照合 → 的中・損益計算
→ 外れ原因分類 → 回顧 → KPI集計 → 改善候補
```

## 13. エラー処理

- critical: DB保存失敗、race_id日付不一致、取消馬混入、バックアップ失敗等。買い目をblockedにする。
- error: 対象処理失敗。成功済み範囲は保持し、復旧操作を示す。
- warning: 欠損、鮮度低下、AI未実行、不一致。誤認させず要確認にする。
- すべてにcode、trace_id、対象日、race_id、推奨対応を付ける。
- 秘密情報、Cookie、認証値、生スタックトレースは画面/APIへ出さない。

## 14. セキュリティ・権限

- DB/Redis/API/Frontendの個別ポートをNAS外部へ公開しない。
- `.env`、APIキー、DB秘密、IPAT情報をGit・ログ・AI入力へ含めない。
- AIは本番DB・本番コード・IPATへアクセスできない。
- ファイルパス、アップロード、外部入力を検証し、パストラバーサル・任意コマンドを防ぐ。
- AI出力をHTMLとして直接描画しない。
- 自動投票はVer.1.0に存在させない。
- 買い目の購入済み変更、設定有効化、復元は確認と監査を必須とする。

## 15. ログ・監査

- application、job、prediction、collector、ai、security、audit、errorを区分する。
- timestamp、level、service、run_id、race_date、race_id、trace_id、event_code、処理件数、処理時間を記録する。
- AI入力可視範囲、モデル、プロンプト、入力ハッシュ、独立結果ハッシュ、比較・統合結果を保存する。
- 買い目状態変更、設定変更、結果修正、復元はbefore/afterと理由を監査する。

## 16. バックアップ

- 毎日: PostgreSQL `pg_dump`、当日Artifact、設定、Gitのバックアップ。
- 毎週: 容量・失敗確認、復元可能性の確認。
- 毎月: テストDBへの復元試験、保持期限確認。
- 直近14日、週次8世代、月次12世代を初期目安とする。
- Docker Volumeの単純コピーを正式復旧手段にしない。

## 17. テスト

- Backend: 設定、検証、API、DB、ファイル、予想、特徴量、score、例外。
- Frontend: コンポーネント、フォーム、表、ボタン、API、loading、empty、error。
- E2E: 起動→日付→取得→Python→AI独立→比較→統合→買い目→保存→結果→分析。
- 異常系: ファイル/列/データ/APIキー/JSON/ネットワーク/DB/Redis/二重実行/不正ID。
- 回帰: 旧CLIと新Adapterの重要結果一致。
- UI: 主要画面幅、長い名称、18頭、取消、同着、欠損、AI未実行、一部失敗。

## 18. 運用

- 開催前にデータ、DB、Redis、Worker、容量、前回バックアップを確認する。
- 障害時は買い目停止→ログ/状態確認→安全な復旧→品質再検査→新run_idで再実行する。
- 本番変更はテスト、回帰、バックテスト、承認、バックアップ、ロールバック確認後に行う。
- 既存NAS cronからBeatへ移す際は同じジョブを二重実行しない。

## 19. 重要仕様変更記録

### 19.1 AI方式

- 変更前: Python予想を見たセカンドオピニオン・微調整。
- 変更後: Pythonを見ない独立分析を先に固定し、その後で比較・統合。
- 理由: Pythonの言い換え化を防ぎ、独自価値を検証可能にする。
- 影響: AI API、DB、プロンプト、画面、品質、テスト。
- 移行: 旧AI runを削除せず、入力可視範囲とモードを付けて履歴保持。
- 後方互換: Python予想・旧履歴の閲覧は維持する。

### 19.2 フロント技術

- 変更前: React + Vite + PWAの記述。
- 変更後: 既存Next.js 15 + TypeScriptを正式採用。
- 理由: 既存資産保護、ビルド成功、再構築リスク回避。
- 影響: frontendとDockerfile。API契約に影響なし。
- 移行: 既存Nextをページ・コンポーネント分割して改善する。
- 後方互換: 現在のURL `/` はダッシュボードとして維持する。

## 20. 完成判定

完成は、主要機能、主要画面、AI独立性、買い目安全性、結果分析、回帰・E2E、Docker/Windows起動、バックアップ・復元、設計追跡がすべて合格し、重大/高優先度不具合が0件の場合にのみ行う。自動投票は完成条件に含めない。
