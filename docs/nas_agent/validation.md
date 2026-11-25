# 開発時検証と実機受入

2026-11-25にWindows上で検証した。実NASへの接続・認証・導入は行っていない。Issue #18が今回を開発のみ、導入は別承認と指定しているため、既存のSSH/Tailscale設定を変更したり接続経路を追加したりしていない。

## 開発時の証拠

基準mainは `178855fd117301131c8e3ebf70ce8c5e9e327728`。独立ブランチ `codex/issue18-nas-agent` から、PR #17の `3ae9ce0dc5eaf41c2aa5d26c6e62fcef8dc26b92` を固定参照する。予想・特徴量・比較ロジックを複製していない。

```sh
# 標準ライブラリのagentと模擬APIの受入テストだけを実行する。NASへ接続しない。
python -m pytest tests/test_nas_agent.py -q
```

35件成功。次を確認した。

| 対象 | 確認内容 |
|---|---|
| 受付→承認→実行→報告 | 模擬GitHubで実際のAPIルーティング・依頼元ID検査・CLI承認・healthcheckハンドラー・SQLite・状態コメントを連携 |
| 拒否 | 未承認SHA、任意命令/URL/パス、不正JSON、未許可アカウント、変更後の再承認、別Issueの同一job_id（未実行時を含む） |
| 継続/障害 | 重複実行防止、期限切れ/close、実行中close、timeout、子プロセス停止、RUNNINGからの中断復旧と管理者確認待ち |
| 報告 | POST保存後の応答喪失でも同じコメントを再利用、処理自体は再実行しない、秘密/生ログの公開除外 |
| 固定受入ハンドラー | 模擬ハンドラーで固定パラメータ・集計報告を確認。376頭/27レース等のテスト値は架空fixtureであり実NAS結果ではない |
| ホスト境界 | 二重worker拒否、本番と重なるroot拒否、環境変数から秘密除外、GitHub認証ヘッダー/redirect拒否/backoff |
| Compose | 本番rw、socket、root、外部network、資源上限欠落、秘密環境変数を拒否 |

追加smokeでは公開GitHubからPR #17の完全SHAを実際に取得し、clean checkout、既存 `nas.launch.checked_environment`、実際の `docker compose config --format json` と独自安全検査を通した。入力は新規temp内の合成fixtureだけ。Docker build/runは実行していない。[機械可読記録](smoke_result.json)を参照。

GitHub ActionsにはUbuntu/Python 3.11で同じ模擬テストを実行する専用workflowを追加した。クラウド実行であり、NAS runner、NAS認証、デプロイ、定期起動は含まない。workflowの実結果はPRのChecksで確認する。

## 実NASで未検証の事項

- 最新のUGREEN ARM64/UGOS状態、Python/Compose互換性、ユーザー分離・権限・systemdテンプレート、NAS発HTTPS通信。
- 対象repoは開発確認時にpublic。実NASのPATの可視性/期限/権限とIssuesへの書込は未確認。`verify-github`と最初のhealthcheck報告で確認する。
- ARM64イメージbuild/run、実入力/マスタ/オッズ、READY/STATUS/公開ファイルSHA、保護対象の実行前後一致、実NASの空容量/性能。
- Docker daemonのbuild資源上限とclient停止後のbuild状態。container上限やworker unitの上限をdaemon build上限とみなさない。不明なら `allow_build=false` を維持する。
- 同一snapshot/modelのWindows比較。参照を登録しない実行はPARTIALであり、Windows一致を表さない。
- Windows電源OFFでの一連動作、NAS停止中の依頼、電源断後の復旧、長時間運用時のrate limit。
- Driveはdry-run固定。検証専用先への転送機能は今回有効化していない。既存rclone設定を使用/変更していない。

## 別承認後の最小実機受入

[初回導入](install.md)の権限表・設定例をNAS管理者がレビューし、新規領域と2役割だけを導入する。[運用手順](operations.md)のコマンドで以下を確認する。

1. 新規配置をレビュー済みagent SHAで固定し、`verify-github`を実行する。
2. healthcheck Issueを登録し、`inspect`→hashの`approve`→worker→pollerでSUCCEEDEDと履歴を確認する。
3. PR #17の既存snapshotで20260809の原本を読取コピーし、manifest SHAをpolicyへ固定する。build資源・時間帯を別承認する。
4. issue16_acceptanceを個別承認して一度だけ実行する。候補workspaceのprivate監査、READY/STATUS/成果物SHA、公開集計を照合する。Windows基準がなければ比較保留を維持する。
5. Windows OFFで新jobを処理する。NAS停止中の未期限切れ依頼、RUNNING電源断→INTERRUPTED→管理者確認待ち、報告のみ再送を確認する。
6. 本番/既存定期処理/Tailscale設定に変更がないことを確認し、結果だけをGitHubへ報告する。未確認のまま本稼働扱いにしない。

本番監査は指定ソース・非秘密設定・当日原本/公開物・指定container稼働設定に限定する。全NASファイルの不変や、本番schedulerによる正当な更新の原因までは証明しない。差分発生時は成功と認定せず、管理者が調査する。agentは本番を復元/修復しない。
