# NASへの初回導入（管理者による別承認後のみ）

この開発では以下のコマンドを実NASで実行していない。既存本番、main、モデル/データ、定期処理、SSH/認証設定、Tailscale ACL/SFTP設定を変更しない。初回だけ、NASの管理画面または既に許可された管理経路で導入する。対話shellやDockerの権限を既存Tailscale/SFTPへ追加する方式は採らない。

## 1. 読み取り確認と承認

ARM64 Linux、Docker/Compose、Python 3.11、Git、HTTPS outbound、NASの空容量と08:00〜22:30の実際の稼働時間、本番の処理時間を確認する。既存コンテナのworking_dir labelで本番rootが特定できること、agentのworkerが指定の本番非秘密ソース/原本を読む権限を既に得られることを確認する。権限不足時は停止し、本番ACL・socketのchmodやsudo範囲を自動で拡げない。

レビュー済みagent commitとPR17 commit、Dockerfile/依存/build時間帯/資源、下記ディレクトリ/新規専用ユーザー/新規サービスの作成を別承認する。UGOSのアカウント/タスク管理でこの分離ができない場合は導入方式を再レビューする。既存サービスの実行ユーザーや既存グループ設定は変更しない。

| 新規領域/役割 | 所有/権限 |
|---|---|
| `/volume1/docker/keiba_nas_agent/code` | 管理者所有、poller/workerはread only。レビューcommitでdetached checkout |
| config/poller.json、secrets/github-issues.token | poller所有0600。秘密ディレクトリは管理者管理、相互にtokenを読めないようにする |
| config/worker.json、config/policy.json、任意のread token | worker所有0600。pollerに読取/書込を与えない |
| poller-state | poller所有0700 |
| worker-state、checkouts | worker所有0700 |
| spool親 | 管理者所有0750、共有group=nas-agent-bridge、group書込なし |
| spool/inbox | poller所有、group=bridge、2750。workerはread only |
| spool/outbox | worker所有、group=bridge、2750。pollerはread only |
| `/volume1/docker/keiba_nas_agent_validation` | worker所有0700。新規候補出力だけ |
| `/volume1/docker/keiba_nas_agent_inputs` | 管理者が作成した固定入力、workerはread only |

専用pollerはDockerグループに入れない。固定workerのみ専用新サービスにDocker操作権限を与える。Docker権限がroot相当であることを承認する。`deploy/*.service` は新規サービス用テンプレートで、既存unit/cronは触らない。pollerのsocket禁止等のsystemd制約をNASで実測するまで本稼働扱いにしない。

## 2. 新規配置と設定

NAS管理者が上記ユーザー/権限を用意してから、新規パスだけへ配置する。既存パスがあれば上書き/再帰削除せず別名を選ぶ。

```sh
# 以下は管理者が承認した新規領域の例。既存rootへ配置しない。
BASE=/volume1/docker/keiba_nas_agent
test ! -e "$BASE"
mkdir "$BASE"
git clone --single-branch --branch codex/issue18-nas-agent https://github.com/hallo-102/jibunnyou.git "$BASE/code"
git -C "$BASE/code" checkout --detach <レビュー済みagent完全SHA>  # 自動pull/自己更新を設定しない。
git -C "$BASE/code" status --porcelain  # cleanであることを確認する。
```

管理者はexamplesのpoller.json/worker.json/policy.jsonをcodeの**外**のconfigへコピーし、表の所有/権限を個別に設定する。instance_idは両役割で同じランダム16桁hex、agent_commitはレビュー済み完全SHAへ置換する。予想用validation-venvはcode/checkout/秘密と別の新規パスでPython 3.11とPR17 nas/requirements.txtを導入する。agent自身は標準ライブラリのみ、validation helperだけがこのvenvを使う。

fine-grained PATはブラウザで対象repoだけを選び発行し、NAS管理者がNASローカルの0600ファイルへ保存する。token値をチャット、Issue、shell引数、ログ、Git、コンテナへ入れない。既存Windowsの認証情報や本番rclone設定を転送しない。worker read tokenは必要な場合だけ別ファイルへ保存する。

## 3. 起動前確認と最初のhealthcheck

以下はそれぞれの専用ユーザーの権限で行う。`PYTHONDONTWRITEBYTECODE=1` を設定し、agent codeから実行する。agent自身のrevision/clean検査は起動時に必須。

```sh
# poller利用者。実repo可視性/read権限を確認する。writeは初回報告まで未確認扱い。
python3.11 -m nas_agent verify-github --config /volume1/docker/keiba_nas_agent/config/poller.json
python3.11 -m nas_agent poll --config /volume1/docker/keiba_nas_agent/config/poller.json

# worker利用者。healthcheck依頼はoperations.mdの手順で個別hashを承認する。
python3.11 -m nas_agent worker --config /volume1/docker/keiba_nas_agent/config/worker.json
python3.11 -m nas_agent status --config /volume1/docker/keiba_nas_agent/config/worker.json
```

初期policyはapproved_requests/grantsが空、allow_build=false、実行時間帯nullなので重いジョブは実行しない。最初のhealthcheckを登録/承認し、worker→pollerを1回ずつ動かしてGitHubにSUCCEEDEDと履歴が出ることを確認する。

## 4. NASだけで固定snapshotを準備する

管理者がPR17を別の新規checkoutへ固定し、既存nas.workspace/snapshotを使って20260809の入力・2マスタ・オッズを新規の準備workspaceへコピーする。これらは現在の本番ファイルを読み取りコピーするだけで、昔のWindows固定snapshotと同一とは限らない。

```sh
# PR17の既存APIを使う。準備用venvと新規checkoutは管理者が別途用意する。
git checkout --detach 3ae9ce0dc5eaf41c2aa5d26c6e62fcef8dc26b92
python3.11 -m nas.workspace --root /volume1/docker/keiba_agent_input_preparation --production-root /volume1/docker/keiba_yosou_2026_nas
python3.11 -m nas.snapshot --workspace /volume1/docker/keiba_agent_input_preparation \
  --root /volume1/docker/keiba_yosou_2026_nas --destination /volume1/docker/keiba_agent_input_preparation/snapshots/20260809 --day 20260809
sha256sum /volume1/docker/keiba_agent_input_preparation/snapshots/20260809/manifest.json  # 実測SHAをpolicyへ固定する。
```

policy.inputsのrootをこの固定コピーへ向け、manifest_sha256とracedayを固定する。読み取り専用の所有/権限にし、workerの入力元にする。NAS内部で完結するためWindowsは必要ない。同じsnapshot/modelを使ったWindows基準がNASに保存済みならreference_root/reference_manifest_sha256も固定する。なければPARTIAL/Windows比較未確認を明示し、結果を比較済みと認定しない。

## 5. 受入ジョブと常駐化

本番と重ならずtimeout全体が収まるJST時間帯をpolicyへ設定する。例12:00〜17:00は例示であり、実際の本番予定/余力を確認して決める。Dockerfile/依存/contextとbuild daemonの資源を確認し、別承認後にだけallow_build=trueにする。RAM2GiB/CPU1/PID256は検証containerの上限。build daemonの上限は別に確認が必要で、不明ならbuildを許可しない。

issue16_acceptanceの個別依頼hashを承認し、--serveなしで1回の実機受入を行う。成功/失敗/保護対象前後差分/成果物SHAを確認してから、管理者がテンプレートを実NASに合わせ確認し、**専用2サービスだけ**を起動/自動起動に登録する。既存本番unit、cron、SSH/Tailscale、SFTP、rclone設定を変更しない。

Windows電源OFFでGitHub依頼→NAS実行→GitHub結果を確認する。NAS停止中に作った未期限切れ依頼の起動後処理と、RUNNING時電源断のINTERRUPTEDを確認する。この実機試験を完了するまではWindows不要の実証済みとは扱わない。
