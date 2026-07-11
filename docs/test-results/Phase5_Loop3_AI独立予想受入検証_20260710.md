# Phase 5 Loop 3 AI独立予想受入検証

- 実施日: 2026-07-10
- 対象Phase: Phase 5 / M5 生成AI独立予想
- 判定: 合格（ライブOpenAI応答はAPIキー未設定のため未実施）

## 1. 現在のフェーズ

Phase 5：生成AI独立予想。

## 2. 今回のループ

Loop 3 / 5。

## 3. 今回実施した内容

- `ai_independent_input_v1`を許可項目方式で実装した。
- Python順位、score、推定馬券内率、期待値、危険馬、買い目、印を再帰的に禁止した。
- 過去走は対象日より前の日付だけを最大5件含め、未来情報を除外した。
- `ai_independent_result_v1`をPydanticの`extra=forbid`で固定した。
- OpenAI Responses APIとStructured OutputsのProvider Adapterを追加した。
- モデルsnapshot、プロンプト版・hash、入力hash、出力hash、token、処理時間を保存した。
- 馬番・馬名・全頭・順位重複・順位連続性・順位範囲・情報不足をアプリ側で再検証した。
- 独立結果を`python_result_visible=false`かつ`is_locked=true`で保存した。
- `ai` Celery queue、Worker task、冪等API、履歴取得API、5秒pollを追加した。
- UIへ独立順位、自信度、根拠、モデル、固定hash、失敗理由を追加し、旧AI補正と分離した。
- APIキー未設定時にAIだけを失敗させ、Python機能と画面を継続した。

## 4. 確認した設計仕様書

- `Keiba AI Studio 設計仕様書 改訂版.md` 12章、13章、31章、32章
- `Codex最終ジャッジ機能.md` 6章、7章、8章
- `API・連携インターフェース設計書.md`
- `データベース詳細設計書.md`
- `テスト設計書・受入試験仕様書.md`
- `docs/design/00_Keiba_AI_Studio_統合仕様書.md`
- `docs/adr/ADR-002-AI独立分析と段階的統合方式.md`
- `docs/adr/ADR-005-生成AIProviderと構造化出力の固定方式.md`

## 5. 発見した問題

1. 既存`ai.second_opinion`はPython結果を見るルールベースで、独立分析ではなかった。
2. 独立分析のProvider、入力Schema、出力Schema、専用queue、画面がなかった。
3. `OPENAI_API_KEY`が現環境に設定されていない。
4. スマートフォン幅でページ全体に数pxの横スクロールが残った。

## 6. 問題の原因

1. 旧仕様がPythonの説明・微調整をAIの役割としていた。
2. Phase 2ではDBの段階保存制約だけを先行実装していた。
3. 秘密情報をリポジトリへ保存しないため、運用環境で未設定だった。
4. 横長tableは内部scrollだったが、bodyの端数幅がdocument scrollへ影響した。

## 7. 変更したファイル

| ファイル | 変更内容 | 変更理由 |
|---|---|---|
| `backend/app/schemas/ai_independent.py` | 独立入力・出力の厳格Schema | 自由JSONと余分な項目を拒否するため |
| `backend/app/services/ai_provider.py` | OpenAI Responses Provider、prompt版、test mock | Provider固有処理を分離するため |
| `backend/app/services/ai_independent.py` | allowlist入力、検証、hash、Artifact、有限retry、queued executor | 独立性と固定履歴を保証するため |
| `backend/app/core/config.py` | Provider、SecretStr API key、固定model、timeout/retry | 秘密と実行条件を環境変数化するため |
| `backend/app/api/v1/endpoints/ai.py` | 実行・一覧・詳細・レース最新API | 画面と履歴から利用するため |
| `backend/app/api/v1/endpoints/jobs.py` | `ai.independent` jobとqueue dispatch | 長時間処理をHTTPから分離するため |
| `backend/app/core/celery_app.py`、`worker.py` | 専用`ai` queue/task | Python/Collectorと責務を分けるため |
| `backend/requirements.txt` | `openai==2.44.0` | 公式SDKを固定するため |
| `.env.example`、`docker-compose.yml` | AI設定、秘密注入、Worker queue | 再現可能な運用設定にするため |
| `backend/tests/test_ai_independent.py`、`test_worker.py` | 独立性、Schema、hash、retry、API、queue、キー欠損 | 仕様違反を自動検出するため |
| `frontend/app/page.tsx`、`globals.css` | 独立AI表示、状態、失敗、responsive | 旧補正と明確に分けて操作するため |

## 8. 実行したテスト

| テスト | 結果 | 補足 |
|---|---|---|
| Python構文/import | 合格 | 新規service/schema/API/worker |
| 独立入力allowlist | 合格 | Python列へ故意に異常値を入れてもProvider入力に不存在 |
| 過去走future leakage | 合格 | `past.race_date < target_date`を確認 |
| Pydantic Structured Output | 合格 | 全nested modelで`extra=forbid` |
| 馬番・馬名・全頭照合 | 合格 | 存在しない馬番を拒否 |
| 順位重複・非連続・情報不足 | 合格 | アプリ側validation |
| hash・lock・再実行履歴 | 合格 | 再実行は新ID/sequence、旧outputは不変 |
| 一時timeout有限retry | 合格 | 2回失敗後3回目成功、retry 2 |
| APIキー未設定 | 合格 | AI jobだけfailed、設定方法を表示 |
| 専用AI queue | 合格 | `keiba_ai_studio.ai.independent` → `ai` |
| Host新規テスト | 合格 | `test_ai_independent.py` 7件 |
| Container Backend全体 | 合格 | 48 passed / 42.72秒 |
| Frontend production build | 合格 | Next.js compile/type/static generation成功 |
| Docker image build | 合格 | API、Worker、Frontend |
| Compose実queue異常系 | 合格 | 2件ともqueued/running→failed、理由保存 |
| Browser desktop | 合格 | API/DB、button、実行中、失敗、一覧、詳細 |
| Browser mobile | 合格 | document横overflowなし、table内部scrollあり |

## 9. 設計仕様との照合結果

- 独立入力は`input_visibility=python_result_hidden`で、Python列を構造的に持たない。
- `AiAnalysisOutput.stage=independent`はDB CHECKでも`python_result_visible=false`を要求する。
- 独立結果はSHA-256、時刻、model snapshot、prompt version/hash付きで固定保存される。
- 出力は全頭の馬番・馬名と照合し、存在しない馬・重複順位・非連続順位を拒否する。
- 旧`ai.second_opinion`は互換履歴として残し、独立結果へ再分類していない。
- 比較・反対意見・統合は独立結果固定後のPhase 6として分離した。

## 10. 残っている問題

- 現環境に`OPENAI_API_KEY`がないため、ライブAPI応答の品質・token・料金・遅延は未測定。
- Python/AI比較、反対意見、統合順位はPhase 6で実装する。
- 複数AIエージェントの観点別表示はPhase 6/9で拡張する。

## 11. 自己評価

| 評価項目 | 点数 | 理由 |
|---|---:|---|
| 目標達成度 | 20/20 | 独立入力から画面まで必須導線を実装 |
| 正確性 | 15/15 | 全頭・同一性・順位・日付を再検証 |
| 完全性 | 14/15 | ライブAPIだけ秘密未設定で未実施 |
| 設計整合性 | 10/10 | 4段階方式の独立段階を分離実装 |
| テスト品質 | 10/10 | 正常・異常・API・queue・Docker・Browser |
| UI・操作性 | 9/10 | 独立/旧補正、実行中/失敗、mobileを区別 |
| 安全性 | 5/5 | API key非保存、暗黙mockなし、有限retry |
| 保守性 | 5/5 | Provider、Schema、serviceを分離 |
| 性能 | 4/5 | 非同期化済み、ライブ遅延は未測定 |
| ドキュメント | 4/5 | ADR・追跡・受入を同期、運用詳細はPhase 11 |
| 合計 | 96/100 | Phase完了基準90点以上 |

## 12. 判定

- フェーズ完了

APIキー未設定は安全な異常系として仕様どおり処理され、Python予想の継続を妨げない。ライブProviderの運用確認はキー設定後の受入項目として残すが、Phase 5の実装・自動検証・画面導線は完了した。

## 13. 次に実行する作業

Phase 6で、固定済み独立結果が存在する場合だけPython比較を許可し、一致・不一致・反対意見・統合順位・信頼度・不確実性を別Schema/別Artifactへ保存する。
