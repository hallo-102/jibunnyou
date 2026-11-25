# 依頼・承認・状態・停止/復旧

## ChatGPTから依頼

GitHub Issueの新規作成権限があるChatGPT接続を使い、対象repoにタイトル `[NAS-JOB] healthcheck_20261125_01`、本文をexamples/healthcheck.job.jsonの**JSONそのもの**として登録する。コードフェンスや説明文を本文に混ぜない。job_idは毎回新規、expires_atは実際の依頼期限に変更する。例の日付は自動更新されない。Issue本文に認証情報を書かない。

最初の実受入はexamples/issue16_acceptance.job.json。固定SHAは3ae9ce0、input_idは管理者のpolicy辞書に登録済みのものだけ。Issue #18自体は開発管理用であり、ジョブ受付として実行しない。ラベルは表示補助に使えても承認の根拠にはならない。

## 個別承認

NAS管理者がworkerの専用権限で以下を実行する。repo/type/SHA/inputの上位許可範囲はpolicyで先にレビューする。approveはhashを追加するだけで、上位範囲を緩めない。

```sh
# 現在のGitHub本文・作成者・hashを独立取得してレビューする。
python3.11 -m nas_agent inspect --config /volume1/docker/keiba_nas_agent/config/worker.json --issue <ジョブIssue番号>
# inspectで確認した完全hashを明示する。本文が変われば拒否する。
python3.11 -m nas_agent approve --config /volume1/docker/keiba_nas_agent/config/worker.json --issue <ジョブIssue番号> --expected-hash <64桁hash>
```

同一の許可SHA/type/input/date範囲を繰り返す場合、管理者は期限付きgrantsをpolicyへ追加できる。例は以下の形。初回は個別承認のみ、grantsは空のまま始める。新しいSHA、Dockerfile/依存、host helperの変更は範囲へ自動追加しない。

```json
{
  "job_type": "issue16_acceptance",
  "commit_sha": "3ae9ce0dc5eaf41c2aa5d26c6e62fcef8dc26b92",
  "input_id": "fixture_20260809",
  "from_date": "20260809",
  "through_date": "20260809",
  "expires_at": "2026-11-26T00:00:00+09:00"
}
```

初回観測前に編集/コメント等でupdated_atが進んでいるIssueも保守的に個別承認になる。NAS観測後に意味のあるJSON変更があれば、新hashを再承認する。実行済みjob_idは再使用不可。新しい依頼は新job_idを使う。

## 状態とキャンセル

GitHubのmarker付き状態コメントを確認する。状態履歴・開始/終了/heartbeat・commit・集計・成果物SHAだけを投稿する。元Excel/馬別比較/生ログはNASのprivate workspaceに残す。Driveはdry-runで、既存同期先には転送しない。

QUEUEDは実行可能時間等を待つ状態、WAIT_APPROVALはローカル承認/範囲/build確認待ち。RUNNINGの本文変更は実行に反映しない。キャンセルはIssueをcloseする。開始前または実行中の次回確認（通常15秒、HTTP応答待ちと停止処理の時間を加算）で停止する。close後にreopenしても終端jobは自動再実行しない。

```sh
# GitHub接続やtokenが失効していても、ローカル台帳を照会できる。
python3.11 -m nas_agent status --config /volume1/docker/keiba_nas_agent/config/worker.json
python3.11 -m nas_agent status --config /volume1/docker/keiba_nas_agent/config/poller.json
```

GitHubでheartbeatが古い/依頼に応答がない場合、NAS OFF・poller/worker停止・API認証/通信障害などがある。失敗や実行済みと推測せず、ローカル状態と照合する。NAS停止中もIssueはGitHubに残り、起動後に拾う。起動までに期限切れならCANCELLEDになる。

## 中断・timeout・報告失敗

再起動でRUNNINGが残っていればINTERRUPTED。副作用ジョブは自動再実行しない。GitHub報告の失敗だけならoutboxとSQLiteから再送し、処理自体は再実行しない。POST応答だけ失われた場合も既存markerコメントを再利用する。

issue16_acceptanceの失敗/キャンセル/中断後はrecovery_requiredで後続全体を停止する。管理者がprivate log、候補成果物、該当instanceのDocker container、daemon側buildの停止/完了を調べる。既存本番のstop/downや無差別docker pruneを行わない。

```sh
# NAS管理者がagent固有containerとbuild idleを確認した後だけ解除する。
python3.11 -m nas_agent ack-interrupted --config /volume1/docker/keiba_nas_agent/config/worker.json --confirm-build-idle
```

この操作は中断jobを再試行しない。必要なら新job_idの依頼を再承認する。Docker clientの停止だけでdaemon buildの停止を断定しない。

## 認証更新・agent停止/更新

401/403/404等はGITHUB_AUTH_OR_PERMISSION、rate/backoffは待機期限を記録する。NAS管理者は対象repo限定の新PATをNASのprivateファイルへ置換し、その役割の専用サービスだけを再起動する。新PATをIssue/ログ/コマンドラインへ入れない。権限はverify-githubとhealth報告で再確認する。

停止は新規nas-agent-poller/workerサービスだけ。worker停止時は候補ジョブがINTERRUPTEDになり得る。既存本番schedulerやTailscaleサービスは操作しない。更新は新たにレビューしたagent commitを別の新規配置へ導入し、configのagent_commitを管理者が更新する。daemonによるgit pullや自己更新は行わない。
