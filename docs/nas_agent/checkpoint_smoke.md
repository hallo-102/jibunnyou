# NAS計算ジョブの中断・再開の受入

## Issue #37の調査結果

#37は2026年11月26日20:53:21 JSTに開始し、21:07:35 JSTに `GITHUB_UNAVAILABLE` で終了した。MUSEが照合した `execution.log` は計算の途中で途切れており、完了結果は記録されていない。 [NASの状態報告](https://github.com/hallo-102/jibunnyou/issues/37#issuecomment-7188618842) · [MUSEの回収記録](https://github.com/hallo-102/jibunnyou/issues/37#issuecomment-7191742212)

参照コードでは、workerが実行中にIssueの開閉状態を15秒間隔で再確認する。再取得で `Unavailable` が伝播すると、実行ラッパーが子プロセスを終了させ、workerが `FAILED/GITHUB_UNAVAILABLE` を保存する。コメント送信は別のpollerが担当する。workerの実行時に使われた正確なエージェントSHA・journal・HTTPステータス・通信エラー種別は未確認。Issueの `commit_sha` は計算コードのSHA。変更ブランチは、当該計算SHAを許可するGitHub上の既存エージェントブランチ `codex/dev_20261126_03` の `a3bdf48` を基準にした。

無認証workerであれば、14分14秒に実行中確認だけで約58回、受付や実行直前の確認を加えると、GitHubの無認証上限60リクエスト/時に接近する。これは上限到達の仮説。停止時の証拠がなければHTTP 403/429、401、通信タイムアウトのどれかは特定できない。 [GitHub REST APIの利用制限](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api)

workerは再確認失敗の段階、HTTPステータス、レート制限判定、通信エラー種別をSQLiteとstderrに記録する。pollerはコメント送信失敗を別に記録し、既存outboxとrevisionから再送する。worker側でoutboxへのコピーだけが失敗した場合、SQLiteに残った結果から次の周期で再発行する。停止・期限・GitHub再確認の停止条件は既存どおり。

## 合成計算の承認と再開

1. 管理者が実行中ジョブの有無とNASの現行SHAを確認し、その版との差分を確認する。承認済みの配備SHAと既存設定を照合し、`checkpoint_smoke` と配備SHAを専用の許可対象に追加する。既存の個別承認CLIで各Issueのハッシュを個別承認する。
2. 依頼は既存形式の `[NAS-JOB] codex-smoke-…`。本文の `job_type` は `checkpoint_smoke`、`input_id` は `synthetic-v1`、`scope` は `offline`。通常の必須フィールドを揃え、最初の試験だけ `interrupt_after_checkpoint` にJSONの `true` を指定する。合成データ1,024件のうち384件を記録した時点で `INTERRUPTED` になる。
3. 再開は新しいIssueと新しい `job_id` を使う。`resume_from` に先のジョブID、`checkpoint_sha256` にそのチェックポイントのSHA-256を指定し、個別承認する。再開依頼の `interrupt_after_checkpoint` は省く。元ジョブ、コード、入力、計算条件、件数、部分集計を検証したうえで続きから進める。
4. 同条件で中断の指定や再開情報を省いた新規ジョブも個別承認し、完走結果と再開結果を照合する。進捗の正本は128件ごとにアトミックに保存する `checkpoint.json`、エラーコードの正本は既存SQLite台帳。実データへの適用時は処理単位と保存間隔を個別に決める。

## 手動引き渡し

作業者が用意した私有の空ステージを使用し、既存workerの実行ユーザーと同じ検証済み設定でエクスポートする。固定名のファイルだけをまとめる。管理者はマニフェストの照合後、既存の手動引き渡し方法でCodex専用領域の `/exchange/results/nas-jobs/<job_id>/` にコピーする。

```sh
# 確認済みの時点のagentディレクトリで実行し、絶対パスの私有ステージを指定する。
python3.11 -B -m nas_agent export-smoke --config "$WORKER_CONFIG" --job-id "$JOB_ID" --export-dir "$PRIVATE_STAGE"
# SFTPへ引き渡す前に全ファイルを照合する。
cd "$PRIVATE_STAGE/$JOB_ID" && sha256sum -c SHA256SUMS
```

完走ジョブの出力は `result.json`、`nas-agent-status.json`、`execution.log`、`checkpoint.json`、`SHA256SUMS`。中断時は部分集計をチェックポイントで確認する。Codex側では `sftp nas-codex` で回収し、`Get-FileHash -Algorithm SHA256` でマニフェストを照合する。

## 実データ比較の前提

合成受入のあと、#37と同等の実データは新しいIssueとジョブIDを使う。登録済み比較fixtureの実体、入力マニフェスト、発走・確定時刻の証跡、現在のworkerが許可する比較SHA、予定実行時間に対するGitHub APIの認証と利用枠を読み取り確認する。その後に新規依頼のハッシュを管理者が個別承認する。
