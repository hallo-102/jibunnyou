# Phase 2 Loop 2 中核履歴DB検証結果

- 実施日: 2026-07-10
- 対象: Artifact、設定版、AI段階別保存、買い目状態履歴、監査、冪等性、job log
- Alembic revision: `0002_core_history`
- PostgreSQL: 16

## 実装結果

- `config_versions`、`feature_weight_versions`、`artifact_files`、`evidence_records`を追加した。
- `ai_analyses`と`ai_analysis_outputs`を追加し、独立分析・比較・統合の段階別保存領域を分離した。
- `bet_status_history`、`audit_logs`、`idempotency_records`、`job_logs`を追加した。
- `prediction_runs`へ設定版、特徴量重み版、コード版、実行引数、入力Manifest SHA-256を追加した。
- 重要な状態値、確率、金額、SHA-256、AI独立分析時のPython結果可視性をDB CHECK制約で保護した。
- Artifact登録時に実ファイルのSHA-256とサイズを記録し、同一パスの内容変更を拒否するようにした。
- 買い目を削除・再作成せず、状態履歴と監査を追記する方式へ変更した。

## 自動・受入テスト

| テスト | 結果 | 補足 |
|---|---|---|
| Host pytest | 成功 | 21 passed |
| Container pytest | 成功 | 21 passed |
| SQLite migration | 成功 | 30→20→30、schema差分0 |
| PostgreSQL migration | 成功 | 30→20→30、schema差分0 |
| AI独立可視性制約 | 成功 | `python_result_visible=true`をDBが拒否 |
| 買い目状態制約 | 成功 | 未定義statusをDBが拒否 |
| 状態遷移 | 成功 | candidate→planned許可、settled→planned拒否 |
| Artifact不変性 | 成功 | 同一内容再登録可、内容変更拒否 |
| 冪等性一意制約 | 成功 | scope＋key重複を拒否 |
| Legacy schema明示stamp | 成功 | baseline/current双方を構造比較して判定 |
| 検証環境の後片付け | 成功 | container/network/volume削除 |

## 判定

- 自己評価: 95/100
- 判定: Loop 2完了、Phase 2継続
- 次工程: 変更APIの冪等性、job/Artifact/監査の実行経路統合、入力契約、バックアップ契約を完成する。
