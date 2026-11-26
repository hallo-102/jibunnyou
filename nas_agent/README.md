# NAS固定ジョブエージェント（Issue #18）

GitHub Issueを受付として、NAS自身がHTTPS outboundで取得・結果報告する。Windows、外部SSH、Tailscale設定変更、self-hosted Actions runner、任意shell APIを必要としない。

- poller: Docker権限なし。Issue番号/依頼hashだけをspoolへ渡し、集計結果をGitHubへ送る。
- worker: 別ユーザーの固定ランチャー。GitHubから再取得し、依頼元ID/権限・ローカル承認・固定SHA・期限を照合して逐次実行する。
- 初期handler: `healthcheck`、`issue16_acceptance`。後者はPR #17のSHA `3ae9ce0dc5eaf41c2aa5d26c6e62fcef8dc26b92` にあるnas.workspace/launch/candidate/compareを直接利用する。PR #17のマージやコード複製はしない。

**開発成果物のみ。NASへの導入・認証・新サービス起動は管理者の別承認が必要。** 設定例は初期状態で個別承認なし、build不可、重いジョブの時間帯未設定であり、そのままでは受入ジョブを実行しない。

[設計・信頼境界](../docs/nas_agent/design.md) / [初回導入](../docs/nas_agent/install.md) / [依頼・承認・復旧](../docs/nas_agent/operations.md) / [検証・未確認事項](../docs/nas_agent/validation.md)

実機向けBASEは `/volume1/keiba_nas_agent`。UGOS本体のGit/pip導入に依存せず、[専用領域の依存方式](../docs/nas_agent/dependencies.md)と[実機事前確認](../docs/nas_agent/preflight.md)を使う。初回healthcheckは標準Pythonと専用Gitだけで行い、venv・候補Docker build・サービス登録は後の別承認へ分ける。

## 特徴量監査ジョブ（Issue #27）

1. [ジョブ例](examples/full_feature_audit.job.json)のJSONを本文全体に貼り、タイトルを `[NAS-JOB] full_feature_audit_20260809_01` とするIssueを作る。再実行時は新しいジョブIDと有効期限を使う。
2. NAS管理者はローカルポリシーの `job_types` に `full_feature_audit`、`approved_commits` にレビュー済みの監査コードの完全SHAを登録する。ジョブ例ではPR #41の `83052a799cde0313eed0a17c0f2fc2dd056bc44a` を使用する。
3. `inputs.fixture_20260809` の `root` を `/volume1/keiba_nas_agent_inputs/20260809`、`raceday` を文字列 `20260809` とし、`sha256sum /volume1/keiba_nas_agent_inputs/20260809/manifest.json` で得た実値を `manifest_sha256` に登録する。フィクスチャは管理者が用意し、workerには読み取り権限だけを付与する。workerは実行前後にマニフェストのハッシュを照合する。
4. 既存の `inspect` と `approve` の手順で依頼ハッシュを確認して承認する。承認済みの初回用grantも使用できる。

workerの `validation_python` に監査スクリプトの依存パッケージを用意する。workerは承認済みSHAのクリーンなチェックアウトからスクリプトを起動し、ポリシー上のフィクスチャを `--data-dir` に、`raceday` をISO日付に変換して `--start-date` と `--end-date` の両方に指定する。入力の `input/` と `output/` は監査対象Excelの配置先で、マスターとオッズは監査スクリプトの既定規則に従う。マスター名は `race_levels.xlsx`、`場所_馬場_タイム.xlsx` または `base_time.xlsx` で、候補名ごとに `input/`、`master/` の順で探索する。オッズは `ozzu_csv/` 内の開催日入りCSVを更新時刻・ファイル名順で選ぶ。フィクスチャ直下に `settled_bets.csv` があれば購入明細として渡す。

出力先は新規作成した `<validation_root>/<job_id>/output/`。実行ログは同じジョブの `execution.log` に保存する。終了コード0かつ `observed_with_limits` の場合に成功となり、`partial` と `no_real_data` は集計を添えて失敗として報告する。ジョブIssueには明細から集計した開催日・レースID単位のレース数、レース内の馬番（なければ馬名）単位の頭数、および監査した過去走のExcel行数（ブック・今走レース・馬ごと）を報告する。D-01〜D-12の発生率は要件内の差異件数の合計を評価可能件数の合計で割った百分率で、評価可能件数が0の場合は `null`。報告には特徴量監査のステータス、独立した収益評価のステータス、終了コード、監査プロセスの実行秒数も含む。

## A・B比較ジョブ（Issue #35）

[準備ツール](../tools/prepare_ab_comparison.md)でフィクスチャを生成し、管理者が worker 読み取り専用で配置します。[ジョブ例](examples/ab_comparison.job.json)を使い、ポリシーの `job_types` に `ab_comparison`、`approved_commits` に PR #40 の固定 SHA `31a43fa9aa1d018f78de679d3bc17886f22339dd`、`inputs` に入力ID・開催日・ルート・マニフェスト SHA を登録します。既存の `inspect` / `approve` で依頼を承認できます。承認済みの初回用 grant も使えます。

実行には監査と同じ `validation_python` と PR #40 の依存（pandas、NumPy、openpyxl、Git、GNU patch）が必要です。比較は既存の環境で実行でき、`allow_build: false` でも実行します。固定コミットの比較処理は PR #31 の履歴を参照するため、このジョブのチェックアウトは履歴付きです。エージェントの単日ランナーは固定コミットの比較関数を使い、固定基準重みで A・B を評価します。`run.json` には `mode: "single_day_ab"`、`methods: ["A", "B"]`、対象日だけの検証期間を指定します。
従来のNASマニフェストの分割ハッシュ一覧と、PR #40の準備ツールが出力する `files_sha256` 形式に対応します。後者を登録する管理者は、発走前入力の取得証跡とグレードを照合してからマニフェストのSHA-256を承認します。従来の `run.json` v1 は発走前入力のグレード省略時にA、v2はCとして扱います。
正式検証（既定の `formal`）は発走前入力Aを使用します。`evaluation_mode: "reference"` は発走前入力Bも含め、成果物を `output/reference/` に分離します。両モードとも結果証跡B・Cを事後評価に使用でき、Dは集計の対象外です。単日比較では採用候補の選定を行いません。

worker はマニフェストと各入力の SHA-256 を照合したコピーを実行に用います。比較表、除外理由、実行マニフェストなどは `<validation_root>/<job_id>/output/` に保存します。返却対象は固定ファイル名、最大11件、1ファイル64 MiB、総計128 MiBです。GitHub の状態報告には検証済み成果物のファイル名と SHA-256 が含まれます。終了コード2でマニフェストまで生成された場合は照合失敗として報告し、検証済み成果物を参照できます。

## 実行中の通信と最終確認

WorkerはIssueを認証ありで原則60秒、認証なしで原則120秒ごとに再確認します。一時障害やAPI利用制限時は既存バックオフ・Retry-After・リセット時刻を考慮し、最後の正常確認から300秒を上限とします。取消・期限切れ・認証または権限の異常を検知すると停止します。
結果確定前にIssueを改めて取得し、承認した依頼ハッシュと一致することを確認します。確認できない場合は失敗をSQLiteに記録し、既存outbox経由で再送します。通信診断には固定エラーコードとHTTPステータスなどを記録します。
