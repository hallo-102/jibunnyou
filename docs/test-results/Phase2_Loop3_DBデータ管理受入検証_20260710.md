# Phase 2 Loop 3 DB・データ管理受入検証結果

- 実施日: 2026-07-10
- 対象: API冪等性、job履歴、予想Artifact、入力契約、バックアップ・復元契約
- Alembic revision: `0002_core_history`

## 実装結果

- job作成・再試行APIと買い目状態更新APIへ`Idempotency-Key`契約を追加した。
- scope＋key＋正規化body hashで初回処理、同一応答の再利用、body不一致拒否を実装した。
- 無効な買い目状態遷移では冪等性レコードを`failed`にし、処理中として残さない。
- job作成、開始、完了、失敗を`audit_logs`と`job_logs`へ記録する。
- 予想runへ入力Manifest hashを保存し、入力/出力Manifest、予想Excel、engine JSON、stdout、stderrの6 Artifactを登録する。
- Excel/CSVの必須シート、必須業務キー、馬番重複の契約テストを追加した。
- DB、Raw、master、Artifact、ログの対象、RPO/RTO、Manifest、復元順序、失敗条件を設計契約として固定した。

## 自動・受入テスト

| テスト | 結果 | 補足 |
|---|---|---|
| Backend compileall | 成功 | app/tests |
| Host pytest | 成功 | 25 passed |
| Container pytest | 成功 | 25 passed |
| Frontend production build | 成功 | idempotency header追加後 |
| PostgreSQL migration head | 成功 | `0002_core_history (head)` |
| PostgreSQL table構成 | 成功 | 業務30表＋`alembic_version` |
| ヘッダーなしjob作成 | 成功 | HTTP 422 |
| 初回job作成 | 成功 | HTTP 202 |
| 同一key・同一body再送 | 成功 | HTTP 202、同一job ID |
| 同一key・異なるbody | 成功 | HTTP 409、`CONFLICT` |
| PostgreSQL保存件数 | 成功 | job 1、idempotency 1、job log 2、audit 1 |
| Excel必須シート欠落 | 成功 | 0件取込、品質問題を記録 |
| 馬番重複 | 成功 | DBは1件、品質問題を記録 |
| オッズ必須キー欠落 | 成功 | 0件取込、品質問題を記録 |
| 検証環境の後片付け | 成功 | container/network/volume削除 |

## 自己評価

| 評価項目 | 点数 | 理由 | 残課題 |
|---|---:|---|---|
| 目標達成度 | 20/20 | M2の5タスクと完了条件を達成 | なし |
| 正確性 | 15/15 | SQLite/PostgreSQL/API実測で確認 | なし |
| 完全性 | 14/15 | schema、履歴、冪等性、入力・復元契約を網羅 | 自動バックアップはPhase 10 |
| 設計整合性 | 10/10 | ADR-003と最新AI独立要件をDB制約へ反映 | なし |
| テスト品質 | 10/10 | 正常・異常・往復migration・実コンテナ | 継続拡張 |
| UI・操作性 | 8/10 | Frontendから冪等性keyを自動送信 | UI刷新はPhase 9 |
| 安全性 | 5/5 | 重要履歴を追記、不変Artifact、暗黙stamp拒否 | なし |
| 保守性 | 5/5 | 共通履歴・冪等性serviceへ集約 | なし |
| 性能 | 4/5 | hash・index設計を追加 | 大量データ実測はPhase 10 |
| ドキュメント | 5/5 | migration、復元契約、検証証跡を更新 | 自動化手順はPhase 10 |
| 合計 | 96/100 | Phase完了基準90点以上 | Phase 3へ進む |

## Phase 2完了判定

- 空DBのupgrade、downgrade/re-upgrade、schema差分0をSQLite/PostgreSQLで確認した。
- race_id・馬番・run_id・冪等性keyの一意性をDB制約で保護した。
- 予想run、Artifact、AI段階、買い目状態、監査、job logを上書きせず保持できる。
- 自己評価: 96/100
- 判定: Phase 2完了
- 次工程: Phase 3でデータ取得をCollector jobへ接続し、Raw→Normalized→Businessと品質ゲートを完成する。
