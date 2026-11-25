# 開発時検証と実機受入

2026-11-25にWindows上で検証した。実NASへの接続・認証・導入は行っていない。Issue #18が今回を開発のみ、導入は別承認と指定しているため、既存のSSH/Tailscale設定を変更したり接続経路を追加したりしていない。

## 開発時の証拠

基準mainは `178855fd117301131c8e3ebf70ce8c5e9e327728`。独立ブランチ `codex/issue18-nas-agent` から、PR #17の `3ae9ce0dc5eaf41c2aa5d26c6e62fcef8dc26b92` を固定参照する。予想・特徴量・比較ロジックを複製していない。

```sh
# 標準ライブラリのagentと模擬APIの受入テストだけを実行する。NASへ接続しない。
python -m pytest tests/test_nas_agent.py -q
```

初回35件成功。一次レビュー修正後は53件成功（既存35件と追加18件）。次を確認した。

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

GitHub ActionsにはUbuntu/Python 3.11で同じ模擬テストを実行する専用workflowを追加した。[初回実行は成功](https://github.com/hallo-102/jibunnyou/actions/runs/45578325574)。クラウド実行であり、NAS runner、NAS認証、デプロイ、定期起動は含まない。最新commitの結果はPRのChecksで確認する。

## 実NASで未検証の事項

2026-11-25の読み取り調査でOS/CPU/標準Python/Docker/Compose/systemd、現ユーザーのDocker参照、パスmode、公開GitHub HTTPS、既存imageの一部依存、日次の起動条件を確認した。[実機事前確認](preflight.md)と[再利用調査](dependencies.md)に記録した。以下の導入・実行上の未確認は残る。

- 新規専用userの分離・権限、private Gitの選定/展開/ABI・HTTPS clone、systemdテンプレートの実起動/UGOS再起動後の保持、private venv/wheelhouseの互換性。新BASEは未作成。
- 対象repoは開発確認時にpublic。実NASのPATの可視性/期限/権限とIssuesへの書込は未確認。`verify-github`と最初のhealthcheck報告で確認する。
- ARM64イメージbuild/run、実入力/マスタ/オッズ、READY/STATUS/公開ファイルSHA、保護対象の実行前後一致、実NASの空容量/性能。
- Docker daemonのbuild資源上限とclient停止後のbuild状態。container上限やworker unitの上限をdaemon build上限とみなさない。不明なら `allow_build=false` を維持する。
- 同一snapshot/modelのWindows比較。参照を登録しない実行はPARTIALであり、Windows一致を表さない。
- Windows電源OFFでの一連動作、NAS停止中の依頼、電源断後の復旧、長時間運用時のrate limit。
- Driveはdry-run固定。検証専用先への転送機能は今回有効化していない。既存rclone設定を使用/変更していない。

配置変更ではexamples、両service、手動CLIを `/volume1/keiba_nas_agent` と専用Git PATHへ整合させた。初回healthcheckへ不要なpip/venv/buildの準備を外し、非開催日12〜17時を条件付き候補にした。初期policyのbuild=false/時間帯null/grants空は変更しない。NASへ実際の配置・依存展開・user作成・build・unit登録は一切行っていない。

この変更後も既存53件が成功。設定/両unitのBASE・Git PATH・保護対象本番root・安全な初期値を静的確認し、deploy/gitと手順書内の計8シェルブロックをbash -nで構文確認した。開発PCのPython -S（site-packagesを読まない起動）で実際のhealthcheckハンドラーも動作した。これは初回手順のNAS実行やprivate Git/venvの適合確認を代替しない。

## PR #19 一次レビューへの対応

対象レビューは [comment-7157650552](https://github.com/hallo-102/jibunnyou/pull/19#issuecomment-7157650552)。今回の5指摘に限定した。

| 指摘 | 修正・回帰確認 |
|---|---|
| S1 誤PASS | 双方のverify_completed、同一レビューSHA、data/model/implementation、全必須比較列、非空の馬/レース、全レース分類・対象外理由、通常券/固定券/TOP5を検査。不足列/異実装/異SHA/通常券差/理由差/空集合fixtureを拒否。候補runtimeとimageのARM64証拠不足はPARTIAL、既知の異architectureは停止 |
| S2 復旧バリア | 終端・履歴・バリアを同一SQLite transactionで保存。meta挿入にABORT triggerを注入し、通常の失敗更新とRUNNING復旧の両方が全体rollbackすることを確認。再復旧後はINTERRUPTEDとバリアが揃う |
| S3 管理者境界 | root所有policyと全親の非書込検査、rootのみの承認CLIへ変更。worker所有/親書込可/親worker所有/リンク/worker承認を拒否。管理者CLIがworker config/stateを使わないことを確認。POSIX所有情報を模擬したテストであり実NASの権限試験ではない |
| A1 監査・導入 | 当日OZZU CSVの変更を前後hashで検出するfixture。監査の限定範囲と未確認schedulerを明示。新規user/group→管理者ディレクトリ→専用venv→認証→個別承認→受入→専用サービスの順序を具体化 |
| A2 滞留 | inbox/outboxに古い履歴を1000件以上残し、後続の新規依頼実行と報告が次回巡回で到達することを確認。永続cursorだけを追加し、履歴削除や大規模キューは導入しない |

追加比較fixtureはPR17 compareの返すレポート構造を用いた合成データで、実データのWindows/Linux一致の証拠ではない。ジョブSUCCEEDEDは固定処理完了、acceptance=PASSは完全な受入確認として分ける。必須比較情報欠落/不一致は明示停止し、ARM64証拠不足や参照未登録によるPARTIALをPASSへ昇格しない。

## 別承認後の最小実機受入

[初回導入](install.md)の権限表・設定例をNAS管理者がレビューし、新規領域と2役割だけを導入する。[運用手順](operations.md)のコマンドで以下を確認する。

1. 新規配置をレビュー済みagent SHAで固定し、`verify-github`を実行する。
2. healthcheck Issueを登録し、`inspect`→hashの`approve`→worker→pollerでSUCCEEDEDと履歴を確認する。
3. PR #17の既存snapshotで20260809の原本を読取コピーし、manifest SHAをpolicyへ固定する。build資源・時間帯を別承認する。
4. issue16_acceptanceを個別承認して一度だけ実行する。候補workspaceのprivate監査、READY/STATUS/成果物SHA、公開集計を照合する。Windows基準がなければ比較保留を維持する。
5. Windows OFFで新jobを処理する。NAS停止中の未期限切れ依頼、RUNNING電源断→INTERRUPTED→管理者確認待ち、報告のみ再送を確認する。
6. 本番/既存定期処理/Tailscale設定に変更がないことを確認し、結果だけをGitHubへ報告する。未確認のまま本稼働扱いにしない。

本番監査は指定ソース・非秘密設定・当日原本/公開物・指定container稼働設定に限定する。全NASファイルの不変や、本番schedulerによる正当な更新の原因までは証明しない。差分発生時は成功と認定せず、管理者が調査する。agentは本番を復元/修復しない。
