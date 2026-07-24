# 変更履歴

## 1.2.0 - 2026-07-23

- OpenAI APIによる競馬予想の新規実行を廃止し、APIキー不要のChatGPT用プロンプト生成・編集・コピー・ブラウザ起動・手動回答保存へ変更。
- `chatgpt_manual_predictions`を追加し、既存AI履歴を削除せず手動回答履歴をrace_idへ関連付けて保存。
- ChatGPTへの自動ログイン、貼り付け、送信、回答取得は実装しない。

## 1.1.0 - 2026-07-22

- 失敗ジョブとデータ品質警告を永続化する専用通知センターを追加。
- 通知の未読件数、個別既読・未読、一括既読、対象画面への導線を追加。
- OS設定追従・端末保存・初回描画対応のライト／ダークテーマ切替を追加。
- テーマ操作を共通コンポーネント化し、後続の複数route化に備えた。
- 概要、レース、AI比較、買い目、成績、運用を領域別URLへ分離し、現在位置表示を追加。
- 各routeで担当パネルだけを表示し、画面見出しとskip linkの移動先を切り替えるよう改善。
- 共通API、route別日次一覧、route別pollingを分離し、不要な通信と選択レースAPIの二重取得を削減。
- 取得状況・ジョブ・品質チェックと関連型を`OperationsPanels`へ抽出し、共通画面本体を縮小。
- 成績集計条件、KPI、内訳、選択レース結果と関連型を`PerformancePanel`へ抽出し、各選択欄へ操作名を追加。
- 独立AI、Python/AI比較・統合、旧AI互換表示と関連型を`AiAnalysisPanels`へ抽出。
- 独立AIパネルの利用枠不足エラーを英語の生メッセージではなく、日本語のBilling復旧案内で表示。
- 買い目条件、予算上限、安全案内、候補一覧・状態操作と関連型を`BetPlanningPanel`へ抽出。
- 買い目の選択欄・金額欄・点数欄へ一意な操作名を追加。
- レース一覧、処理状況バッジ、出走馬の多列比較表、ソート見出しと関連型を`RaceWorkspace`へ抽出。
- API・DB状態、通知センター、テーマ、次操作、エラー表示を`WorkspaceHeader`へ抽出。
- 開催日Excel、検索、予想・AI・品質・結果の主要操作を`ActionToolbar`へ抽出。
- レース検索欄へ一意な操作名を追加。
- Next.jsを15.5.21へ更新し、sharp 0.35.3を固定して依存関係監査の高重要度脆弱性を解消。
- Alembic `0006_notifications`と通知API・回帰テストを追加。
- ローカルで更新される実データExcelの順位を固定値で比較せず、選択した入力との一致を検証するよう回帰契約を修正。
- Backend 78件、UI契約18件、Frontend 6route production build・Browser表示／操作検証に合格。
- Docker全service再起動、readiness、Celery ping、migration head、6画面受入、依存関係監査0件に合格。

## 1.0.0 - 2026-07-11

- OpenAI `insufficient_quota`を再試行せず、日本語のBilling案内へ変換するよう改善。
- 実`.env`をテストが読み込まないよう設定fixtureを隔離。
- Python予想の再現可能Adapter、Golden Master、実queue実行を完成。
- Python非可視のAI独立分析、固定hash、構造化検証を追加。
- Python/AI比較、反対材料、統合score・順位・根拠・安全上限を追加。
- Python案/AI統合案、3連複/ワイド、formation/box/wheel、資金上限を追加。
- 自動購入をDB CHECKとserviceで無効化。
- 構造化結果・払戻、複数的中、返還、条件別成績を追加。
- 次操作、6領域navigation、loading/error/help/accessibility/mobile/printを改善。
- Backend 71件、UI契約4件、Docker停止再起動、backup・隔離復元を受入。
- Windows向け運用、backup、restore、障害対応、FAQを追加。

## 0.2.0 - 2026-07-10

- FastAPI、Next.js、PostgreSQL、Redis、Celery、Nginxの7service基盤を構築。
- Alembic、履歴、監査、Artifact、Collector、データ品質を追加。
