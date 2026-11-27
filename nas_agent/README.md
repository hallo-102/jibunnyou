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
従来のNASマニフェストの分割ハッシュ一覧と、PR #40の準備ツールが出力する `files_sha256` 形式に対応します。従来の `run.json` v1 は発走前入力のグレード省略時にA、v2はCとして扱います。

### PR #40の準備ツールを使う場合

1. [NAS単日A・B比較用の設定例](examples/ab_comparison.prepare.json)を `snapshots.json` と `results.json` があるディレクトリにコピーし、`periods.validation` の両方を対象開催日（JST）に合わせます。各入力ファイルはこのディレクトリからの相対パスで指定してください。
2. PR #40 の固定 SHA `31a43fa9aa1d018f78de679d3bc17886f22339dd` のチェックアウトで、設定例を入力に準備します。出力ディレクトリには未作成のパスを指定します。確定結果が Excel の場合は設定の `results` をそのファイル名にして、`--result-evidence` に証跡 JSON を指定します。

   ```powershell
   $env:PYTHONPATH = (Join-Path (Get-Location) 'race_purchase_filter')
   python -m tools.prepare_ab_comparison --config C:/path/to/ab_comparison.prepare.json --output C:/path/to/new-ab-fixture
   ```

3. 管理者は発走前入力の取得証跡とグレードを照合し、出来上がったフィクスチャを変更せずに worker 読み取り専用で配置します。配置後の `manifest.json` の SHA-256 をポリシーに登録します。

NAS 用の設定は `mode: "single_day_ab"`、`methods: ["A", "B"]`、単日分の `periods.validation` を使います。PR #40 の汎用比較設定にある `evaluation_date` や `baselines` が残っていると、NAS は受付後のフィクスチャ検査で停止します。
正式検証（既定の `formal`）は発走前入力Aを使用します。`evaluation_mode: "reference"` は発走前入力Bも含め、成果物を `output/reference/` に分離します。両モードとも結果証跡B・Cを事後評価に使用でき、Dは集計の対象外です。単日比較では採用候補の選定を行いません。

worker はマニフェストと各入力の SHA-256 を照合したコピーを実行に用います。比較表、除外理由、実行マニフェストなどは `<validation_root>/<job_id>/output/` に保存します。返却対象は固定ファイル名、最大11件、1ファイル64 MiB、総計128 MiBです。GitHub の状態報告には検証済み成果物のファイル名と SHA-256 が含まれます。終了コード2でマニフェストまで生成された場合は照合失敗として報告し、検証済み成果物を参照できます。

## 実行中の通信と最終確認

WorkerはIssueを認証ありで原則60秒、認証なしで原則120秒ごとに再確認します。一時障害やAPI利用制限時は既存バックオフ・Retry-After・リセット時刻を考慮し、最後の正常確認から300秒を上限とします。取消・期限切れ・認証または権限の異常を検知すると停止します。
計算が返った時点で結果をSQLiteに保存し、成果物を SHA-256 付きで回収用ディレクトリに確定します。その後、Issue・ローカル承認・期限・入力SHAを再確認し、既存outboxから結果を報告します。一時障害が300秒の猶予を超えると `PENDING_FINALIZATION` と `GITHUB_RECHECK_TIMEOUT` を記録します。通信復旧時やworker再起動時は、保存した成果物から確定を再開します。依頼の変更・取消・期限切れ・権限異常は成功確定を停止します。


## 回収とSFTP引き渡し（Issue #49）

- workerの `<state_dir>/recovery/<job_id>/<状態>/` に状態JSON、回収用実行ログ、存在する固定出力、`SHA256SUMS` を保存します。失敗情報は `failure-diagnostics.json`、直近のGitHub通信診断はジョブ直下の `latest-github-diagnostic.json` に保存します。pollerのコメント送信診断はpollerの `<state_dir>/diagnostics/<issue番号>.json` に記録します。
- 回収用ログは状態履歴と実行ログ末尾最大1 MiBを含み、トークンと認証ヘッダーの値をマスクします。診断には失敗箇所、固定エラーコード、HTTP状態、API利用制限判定、通信種別を記録します。
- SFTPへの引き渡しは管理者ポリシーに `artifact_exchange` を設定すると有効になります。`root` は既存の交換領域を指すNAS側の絶対パス、`destination` はその配下に管理者が作成した結果ディレクトリです。`jobs` にジョブ種別ごとのファイル名を列挙してください。次の値は設定書式の例です。

```json
{
  "root": "/path/to/existing/exchange",
  "destination": "/path/to/existing/exchange/results",
  "jobs": {
    "checkpoint_smoke": ["execution.log", "nas-agent-status.json", "failure-diagnostics.json", "checkpoint.json", "result.json"]
  }
}
```

引き渡し単位は `<job_id>--<小文字の状態>/` で、選択されたファイルと `SHA256SUMS` を一括配置します。猶予を超えて確定保留となった出力には `--pending_finalization` が付き、成功後の出力には `--succeeded` が付きます。結果受領時は各ディレクトリで `sha256sum -c SHA256SUMS` を実行してください。管理者はworkerに結果ディレクトリへの書き込み権限を、SFTP側に発行済み結果の読み取り権限を設定します。setgidで既存の読み取り用グループを継承できます。検証ワークスペースと入力原本はworkerの私有領域や読み取り専用領域に留まります。

## 開催日一覧での監査事前承認

管理者ポリシーの `grants` に、`job_type: "full_feature_audit"`、固定監査SHAの `commit_sha`、期限の `expires_at`、開催日を入力IDへ対応付ける `inputs` を設定します。たとえば `"inputs": {"20260809": "fixture_20260809", "20260816": "fixture_20260816"}` です。各入力IDをポリシーの `inputs` に登録し、開催日とマニフェストSHAを固定します。1件のgrantで最大168開催日を承認できます。各開催日の依頼には一意のジョブIDを指定してください。受付と最終確認では依頼・承認・有効期限を照合し、監査入力の内容は実行前後と確定時にハッシュで照合します。

## 検証用フィクスチャの固定準備ジョブ

`prepare_fixture` は導入済みエージェントの `agent_commit` と個別承認で実行します。入力として管理者が読み取り専用の準備用ルートを登録します。ルートの `manifest.json` は `schema_version: 1`、`evaluation_mode`（`formal` または `reference`）、`files_sha256` を持ちます。ハッシュ一覧には `snapshot_index.json`、`capture_evidence.json`、`settled.xlsx` または `settled.json`、使用する `archive/` 以下のファイルをすべて含めます。入力形式は[既存の準備ツール](../tools/prepare_ab_comparison.md)に準拠します。正式評価では各レースに発走前証跡Aを指定します。参考評価では証跡区分の未指定をCとして記録します。

`validation_python` で準備処理を実行し、`<validation_root>/<job_id>/output/` に既存のA・B比較へ渡せるフィクスチャを保存します。生成マニフェストは、レースごとの `input_evidence`、`evaluation_mode`、`original_files_sha256`、`generated_files_sha256` を記録します。管理者は生成後のマニフェストSHAを、次の比較ジョブの入力ポリシーに登録できます。
