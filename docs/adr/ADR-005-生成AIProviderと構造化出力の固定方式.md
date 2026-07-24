# ADR-005: 生成AI Providerと構造化出力の固定方式

> **状態: 2026-07-23に廃止。** 本文は旧OpenAI API方式の意思決定履歴として保持する。現在の正式方式はChatGPT用プロンプト生成・コピーと利用者による手動送信・手動取り込みであり、API Providerは無効である。

- 状態: 採用
- 決定日: 2026-07-10

## 背景

Phase 5では、Python最終順位を見ない独立分析を生成AIで実行し、モデル・プロンプト・入力・出力を後から検証できる必要がある。自由文JSONの手動解析、モデルの浮動alias、無制限再試行、APIキー未設定時の暗黙mockは、再現性・安全性・監査性を満たさない。

## 検討した選択肢

1. HTTPを直接呼び、返却文字列を`json.loads`する。
2. Chat Completions APIとJSON modeを使う。
3. OpenAI公式Python SDKのResponses APIとPydantic Structured OutputsをProvider Adapterから使う。
4. 生成AIを使わず既存ルールベースを独立AIとして再分類する。

## 採用した方法

3を採用し、次を固定する。

- Provider境界は`IndependentAiProvider`とし、OpenAI固有処理を業務serviceから分離する。
- 正式ProviderはOpenAI Responses APIの`responses.parse(..., text_format=...)`を使う。
- 出力は`ai_independent_result_v1`のPydantic Schemaへ適合した場合だけ保存する。
- 初期モデルは`gpt-5.4-mini-2026-03-17`へ固定する。
- プロンプトはコード管理し、`ai-independent-v1.0.0`とSHA-256を入力Artifactへ保存する。
- 入力は`ai_independent_input_v1`の許可項目だけから作り、`raw`全体を転送しない。
- APIキー未設定時は明確に失敗し、Python予想・履歴閲覧は停止しない。
- `mock` Providerは`test`、`development`、`local`だけで明示利用でき、本番環境では拒否する。
- timeout、429、接続、5xxだけを最大2回まで有限再試行し、Schema不正は再試行しない。
- ProviderにはWeb検索等のToolを与えず、保存済み入力以外の事実を追加させない。

## 採用理由

- OpenAI公式のStructured Outputsは、JSON Schemaへの適合をSDKとPydantic型で扱える。
- Responses APIではアプリケーション規則を`developer` messageとして入力より優先できる。
- snapshot IDとプロンプトhashにより、モデルalias変更時の曖昧さを減らせる。
- 小型モデルは全レース継続実行の費用と待ち時間を抑えやすい。
- Provider Adapterにより、将来のモデル変更や別Provider追加を業務Schemaから切り離せる。

## 利点

- Python非可視入力、厳格出力、全頭照合、固定保存を機械的に検査できる。
- モデル・プロンプト・入力・出力のhashをrun単位で追跡できる。
- API障害時もPython側を継続できる。
- テストでは課金通信なしに成功・異常・再試行を再現できる。

## 欠点

- snapshotの廃止時は、設定・ADR・回帰試験の更新が必要になる。
- ライブAPIの品質・料金・遅延はAPIキーと利用枠に依存する。
- Pydantic Schema変更は新しいschema versionとして追加する必要がある。

## 影響範囲

- `backend/app/schemas/ai_independent.py`
- `backend/app/services/ai_provider.py`
- `backend/app/services/ai_independent.py`
- `backend/app/api/v1/endpoints/ai.py`
- `backend/app/api/v1/endpoints/jobs.py`
- `backend/app/core/config.py`
- `backend/app/core/celery_app.py`
- `backend/app/worker.py`
- `frontend/app/page.tsx`
- `frontend/app/globals.css`

## 公式資料

- [Structured model outputs](https://developers.openai.com/api/docs/guides/structured-outputs)
- [Text generation and developer messages](https://developers.openai.com/api/docs/guides/text?api-mode=responses)
- [GPT-5.4 mini model and snapshots](https://developers.openai.com/api/docs/models/gpt-5.4-mini)

## ロールバック

`ai.independent`のqueue/API/UI導線を無効化しても、既存Python予想と旧AI互換履歴は維持される。保存済み`AiAnalysis`、`AiAnalysisOutput`、Artifactは削除せず、履歴として保持する。
