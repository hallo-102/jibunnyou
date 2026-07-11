# Phase 4 Loop 3 Python予想統合受入検証結果

- 実施日: 2026-07-10
- 対象: 既存2段階予想、隔離workspace、Golden Master、DB/Artifact、Celery予想queue、画面
- 正式実行環境: Docker/Linux、Python 3.11、PyTorch `2.11.0+cpu`

## 実装結果

- 既存`1_keibayosou_best_import_roi_runner.py`と関連7モジュールの予想計算を置換せず、run固有の隔離workspaceで実行するAdapterを追加した。
- 入力Excel、OZZU、3 master、対象日より前の重み、対象日より前の予想履歴、コード、依存版をManifestとSHA-256で固定した。
- 予想Excelは出走馬masterの更新に使わず、収集済みのレースID・馬番・正規化馬名が一致する場合だけ予想値を反映する。
- rank/score欠損、重複馬、連続しない順位、馬名不一致、run ID再利用を拒否する。
- ConfigVersionとFeatureWeightVersionを内容hashで登録・再利用し、13種類の実行Artifactを不変登録する。
- `prediction` Celery queueと専用taskを追加し、APIから長時間予想を非同期実行する。
- UIは選択中の予想ジョブ、完了後の結果自動更新、失敗理由を表示する。

## Golden Master・実キュー受入

| テスト | 結果 | 実測 |
|---|---|---|
| Windows同一snapshot 2回実行 | 成功 | 318頭、重要11項目、不一致0 |
| Docker/Linux同一snapshot 2回実行 | 成功 | 318頭、重要11項目、不一致0 |
| WindowsとDocker/Linuxの比較 | 要確認 | 数値差695、うちDL順位によるPython順位差11頭 |
| 旧保存2026-07-05 Excelと現行入力 | 基準外 | 旧330頭、現行318頭で入力集合が異なる |
| Compose実予想queue 1回目 | 成功 | 184.333秒、queued→running→completed |
| Compose実予想queue 2回目 | 成功 | 186.166秒、同一重要値 |
| DB結果照合 | 成功 | 対象レース9頭、matched 9、mismatch 0 |
| 版数再利用 | 成功 | 2 runで同じConfigVersion/FeatureWeightVersion ID |
| Artifact検証 | 成功 | runあたり13件、欠落0、SHA-256不一致0 |

Excel内容が同じでもZIP生成メタデータによりファイル全体hashは異なった。そのため、内容比較は業務キーと重要値を正とする。

## 3か月自動バックテストSkillの結果

- 前回評価と同じ26開催日（2026-03-28〜2026-06-21）を収集し、対象日差分0件を確認した。
- 予想バッチは7日分を日別ログへ記録し、8日目実行中に同一原因の繰り返しを避けて停止した。
- 対象26日のOZZU CSVを別途read-only監査し、全26日で「同一日・場・R・馬番・券種に複数馬名または重複snapshot」を検出した。
- 過去CSVに取得時刻列がなく、最新snapshotを安全に選べない。`keep=first/last`で黙って選ぶ変更は行っていない。
- 今回の3か月指標は作成せず、採否判定は「要確認」とする。未来の収集データはPhase 3の`fetched_at`とrun別Rawを使う。

## 自動テスト・画面確認

| テスト | 結果 |
|---|---|
| Host Backend | 41 passed |
| Container Backend（実入力mount付き） | 41 passed |
| Frontend production build | 成功 |
| Compose config | 成功 |
| Worker queue登録 | `default` / `collector` / `prediction` |
| ブラウザ表示 | API ok / DB ok、36レース、予想9頭を表示 |
| Python予想ボタン | 開催日・レース選択時に単一ボタンで有効 |
| 失敗表示 | `race_date is required` を「失敗理由」として画面表示 |

## 自己評価

| 評価項目 | 点数 | 理由 | 残課題 |
|---|---:|---|---|
| 目標達成度 | 20/20 | 画面→queue→旧2段階予想→DB→画面を実受入 | なし |
| 正確性 | 15/15 | 馬同一性、連続順位、重複、hashを検証 | なし |
| 完全性 | 14/15 | Manifest、版数、Artifact、CLI/API/UIを統合 | 過去重複OZZUの移行は保留 |
| 設計整合性 | 10/10 | 旧CLI/Excelを維持し、隔離Adapterで接続 | なし |
| テスト品質 | 10/10 | 単位/回帰/実queue/Golden Master/ブラウザ | なし |
| UI・操作性 | 9/10 | 実行状態、自動更新、失敗理由を確認 | 全体刷新はPhase 9 |
| 安全性 | 5/5 | 上書き回避、未来情報除外、不一致停止 | なし |
| 保守性 | 5/5 | workspace、Golden Master、queueの責務分離 | なし |
| 性能 | 4/5 | 1日分約3分、上限30分以内 | imageサイズは継続監視 |
| ドキュメント | 5/5 | ADR、受入、追跡、進捗を更新 | なし |
| 合計 | 97/100 | Phase完了基準90点以上 | Phase 5へ進む |

## Phase 4完了判定

- 画面から非同期で既存Python予想を実行できる。
- 順位、score、確率、期待値、危険馬、根拠をDBと画面へ保存・表示できる。
- 正式Docker環境の同一snapshotで重要結果が完全一致する。
- 過去OZZUの3か月再予想はデータ品質上の制限として明記し、安全でない自動補正は行わない。
- 判定: Phase 4完了
