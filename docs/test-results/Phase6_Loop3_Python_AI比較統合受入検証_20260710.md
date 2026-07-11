# Phase 6 Loop 1〜3 Python・AI比較統合 受入検証

- 実施日: 2026-07-10
- 判定: 合格
- 自己評価: 97/100

## 実装範囲

- 固定済み独立AI結果のhash再検証後だけPython結果を開示する `ai_comparison_input_v1`
- 全頭の順位差、評価差、一致度、反対材料を返す構造化比較
- 統合score、統合順位、判断基準、信頼度、不確実性、根拠を返す構造化統合
- 通常2順位、例外4順位、3順位以上は手動確認という安全上限
- 比較・統合を親の独立runとは別run・別output・別hash・別Artifactで固定保存
- 専用 `ai.compare_integrate` job/Celery task/API/5秒poll
- Python、独立AI、順位差、反対材料、統合score、統合順位の並列UI

## 自動テスト

| 対象 | 結果 |
|---|---:|
| 新規比較・統合テスト | 7 passed |
| Host Backend全体 | 55 passed |
| Container Backend全体 | 55 passed |
| Frontend production build | 成功 |

新規テストでは、固定独立結果なし、hash改変、Providerによる順位事実改変、例外上限超過、専用queue、API、独立最新結果の混入防止を確認した。統合scoreが全頭に存在し、統合順位に沿って同点なしの降順になることも検証した。

## Compose・実データ受入

- Gateway、Frontend、API、PostgreSQL、Redis、Worker、Beatの7サービスを起動。
- Worker登録taskに `keiba_ai_studio.ai.compare_integrate` を確認。
- 実DBの `202602010801`、9頭、Python run `ec0bb73a-4edc-4d3e-86cd-f8ccf4493e30` で決定論的Provider受入を実施。
- 独立runと比較・統合runが別IDであることを確認。
- 比較・統合はいずれも `is_locked=true`、hash 64文字、全9頭を保存。
- 一致度 `low`、重大反対材料あり、手動確認ありを確認。
- 独立結果のJSON/hash/可視性は変更されていない。

## Browser受入

デスクトップ:

- 比較・統合ボタン、レース別状態、統合score/順位/根拠を確認。
- 統合パネルに一致度、反対材料、統合方針、手動確認、両hashを表示。
- document横overflowなし、横長表は内部scroll。
- 初回pollが過去のAI失敗jobを新規失敗として再通知しないことを6秒後に確認。

390×844:

- document横overflowなし。
- 比較・統合ボタンと統合パネルがviewport内。
- 横長表の内部scrollを維持。

## 異常系と安全性

- 固定済み独立結果がなければ比較開始前に停止する。
- 独立outputの再計算hashが保存hashと違えば停止する。
- 馬集合、馬名、元順位、順位差、反対材料判定の改変を拒否する。
- 統合score欠落・非降順、順位重複、5順位以上の変更を拒否する。
- 比較成功後に統合が失敗した場合は比較outputを保持し `degraded` とする。
- 自動投票は行わない。

## 既知の制限

- `OPENAI_API_KEY`未設定のためライブOpenAI応答は未実施。APIキーなしでもPython予想と保存済み結果の閲覧は継続する。
- 現PhaseのUIは単一ページ内の段階表示。役割別ページ・詳細ドロワーはPhase 9で実施する。

## 完了判定

Phase 6原文の「順位比較、評価差比較、一致・不一致、AI再評価、統合score、統合順位、統合根拠、信頼度、不確実性」と、4つの完了条件を満たした。次はPhase 7で買い目生成の安全境界、券種、組合せ、資金上限、見送りを完成する。
