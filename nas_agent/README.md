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
2. NAS管理者はローカルポリシーの `job_types` に `full_feature_audit`、`approved_commits` にレビュー済みの監査コードの完全SHAを登録する。ジョブ例ではPR #26の `26128923d8c6243466976c72feaa3bfafc0c92f5` を使用する。
3. `inputs.fixture_20260809` の `root` を `/volume1/keiba_nas_agent_inputs/20260809`、`raceday` を文字列 `20260809` とし、`sha256sum /volume1/keiba_nas_agent_inputs/20260809/manifest.json` で得た実値を `manifest_sha256` に登録する。フィクスチャは管理者が用意し、workerには読み取り権限だけを付与する。workerは実行前後にマニフェストのハッシュを照合する。
4. 既存の `inspect` と `approve` の手順で依頼ハッシュを確認して承認する。承認済みの初回用grantも使用できる。

workerの `validation_python` に監査スクリプトの依存パッケージを用意する。workerは承認済みSHAのクリーンなチェックアウトからスクリプトを起動し、ポリシー上のフィクスチャを `--data-dir` に、`raceday` をISO日付に変換して `--start-date` と `--end-date` の両方に指定する。入力の `input/` と `output/` は監査対象Excelの配置先で、マスターとオッズは監査スクリプトの既定規則に従う。マスター名は `race_levels.xlsx`、`場所_馬場_タイム.xlsx` または `base_time.xlsx` で、候補名ごとに `input/`、`master/` の順で探索する。オッズは `ozzu_csv/` 内の開催日入りCSVを更新時刻・ファイル名順で選ぶ。フィクスチャ直下に `settled_bets.csv` があれば購入明細として渡す。

出力先は新規作成した `<validation_root>/<job_id>/output/`。実行ログは同じジョブの `execution.log` に保存する。終了コード0かつ `observed_with_limits` の場合に成功となり、`partial` と `no_real_data` は集計を添えて失敗として報告する。ジョブIssueには明細から集計した開催日・レースID単位のレース数、レース内の馬番（なければ馬名）単位の頭数、および監査した過去走のExcel行数（ブック・今走レース・馬ごと）を報告する。D-01〜D-12の発生率は要件内の差異件数の合計を評価可能件数の合計で割った百分率で、評価可能件数が0の場合は `null`。報告には監査ステータス、終了コード、監査プロセスの実行秒数も含む。
