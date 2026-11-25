# NASへの初回導入（管理者による別承認後のみ）

この開発では以下のコマンドを実NASで実行していない。既存本番、main、モデル/データ、定期処理、SSH/認証設定、Tailscale ACL/SFTP設定を変更しない。初回だけ、NASの管理画面または既に許可された管理経路で導入する。対話shellやDockerの権限を既存Tailscale/SFTPへ追加する方式は採らない。

## 1. 読み取り確認と承認

ARM64 Linux、Docker/Compose、Python 3.11、Git、HTTPS outbound、NASの空容量と08:00〜22:30の実際の稼働時間、本番の処理時間を確認する。既存コンテナのworking_dir labelで本番rootが特定できること、agentのworkerが指定の本番非秘密ソース/原本を読む権限を既に得られることを確認する。権限不足時は停止し、本番ACL・socketのchmodやsudo範囲を自動で拡げない。

UGOSのタスク管理画面と、利用できる場合だけ既存cron/systemd timerの設定・次回予定を読み取り確認する。agentの前後監査が測るのは指定ファイルとcontainerのID/image/mount/restart/user/runningだけで、scheduler設定、全環境変数、秘密設定、全NAS状態は測らない。確認権限がない領域は未確認として記録し、処理時間帯を決められなければbuildを許可しない。現在のUGOSサービス対応・実権限・build資源は未検証。

レビュー済みagent commitとPR17 commit、Dockerfile/依存/build時間帯/資源、下記ディレクトリ/新規専用ユーザー/新規サービスの作成を別承認する。UGOSのアカウント/タスク管理でこの分離ができない場合は導入方式を再レビューする。既存サービスの実行ユーザーや既存グループ設定は変更しない。

| 新規領域/役割 | 所有/権限 |
|---|---|
| `/volume1/docker/keiba_nas_agent/code` | 管理者所有、poller/workerはread only。レビューcommitでdetached checkout |
| config/poller.json、secrets/github-issues.token | poller所有0600。秘密ディレクトリは管理者管理、相互にtokenを読めないようにする |
| config/worker.json、任意のworker read token | worker所有0600。pollerに読取/書込を与えない |
| admin、admin/policy.json | root:nas-agent-policy、親2750・ファイル0640。workerは専用group経由read only、pollerはアクセス不可。全親もroot所有かつgroup/other書込不可 |
| admin-secrets、管理者専用read token | root所有0700/0600。承認CLIだけで使用しworkerへ渡さない |
| poller-state | poller所有0700 |
| worker-state、checkouts | worker所有0700 |
| spool親 | 管理者所有0750、共有group=nas-agent-bridge、group書込なし |
| spool/inbox | poller所有、group=bridge、2750。workerはread only |
| spool/outbox | worker所有、group=bridge、2750。pollerはread only |
| `/volume1/docker/keiba_nas_agent_validation` | worker所有0700。新規候補出力だけ |
| `/volume1/docker/keiba_nas_agent_inputs` | 管理者が作成した固定入力、workerはread only |

専用pollerはDockerグループに入れない。固定workerのみ専用新サービスにDocker操作権限を与える。Docker権限がroot相当であることを承認する。`deploy/*.service` は新規サービス用テンプレートで、既存unit/cronは触らない。pollerのsocket禁止等のsystemd制約をNASで実測するまで本稼働扱いにしない。

## 2. 新規配置と設定

以下は別承認後の初回順序。UGOSが通常のuseradd/systemdをサポートするかを先に確認し、未対応なら管理画面で同じ分離を用意する。使えないコマンドを推測で置換したり既存ACL/SSHを緩めたりしない。既存パス・同名ユーザー/group/unitがあれば上書きせず停止する。

1. 管理者が新規専用group/userを作る。pollerはbridgeのみ、workerはbridge・新規policy groupと承認済みDocker group。既存ユーザーの所属は変更しない。
2. 管理者所有の新規BASE・code・config・admin・spool親を作り、worker/poller専用の書込領域を分ける。
3. レビューSHAのagentとPR17を別checkoutへ固定し、管理者所有の新規validation-venvだけに依存を導入する。
4. 管理者がconfig/policyを配置・編集し、tokenを各役割専用ファイルへ保存する。policyはroot所有・worker読取専用。初期build=false/grants空を維持する。
5. 所有/親の書込権限、API read、healthcheckの管理者承認と報告を確認する。
6. snapshotとbuild資源の受入後だけ、UGOSで承認された専用サービス2個を登録する。

```sh
# root管理者がUGOS対応と名前の未使用を確認してから実行する例。既存user/groupを変更しない。
groupadd --system nas-agent-bridge
groupadd --system nas-agent-policy
useradd --system --no-create-home --shell /usr/sbin/nologin --gid nas-agent-bridge nas-agent-poller
useradd --system --no-create-home --shell /usr/sbin/nologin --gid nas-agent-bridge --groups nas-agent-policy,docker nas-agent-worker
# 以下は管理者が承認した新規領域だけ。既存rootへ配置しない。
BASE=/volume1/docker/keiba_nas_agent
test ! -e "$BASE" || exit 1
umask 027  # 新規コードを常駐userから書込不可にする。
install -d -o root -g nas-agent-bridge -m 0750 "$BASE"
git clone --single-branch --branch codex/issue18-nas-agent https://github.com/hallo-102/jibunnyou.git "$BASE/code"
git -C "$BASE/code" checkout --detach <レビュー済みagent完全SHA>  # 自動pull/自己更新を設定しない。
git -C "$BASE/code" status --porcelain  # cleanであることを確認する。
chown -R root:nas-agent-bridge "$BASE/code"  # 作成した新規checkoutだけを対象にする。
install -d -o root -g nas-agent-bridge -m 0750 "$BASE/config" "$BASE/secrets" "$BASE/spool"
install -d -o root -g nas-agent-policy -m 2750 "$BASE/admin"
install -d -o root -g root -m 0700 "$BASE/admin-secrets"
install -d -o nas-agent-poller -g nas-agent-bridge -m 0700 "$BASE/poller-state"
install -d -o nas-agent-worker -g nas-agent-bridge -m 0700 "$BASE/worker-state" "$BASE/checkouts" /volume1/docker/keiba_nas_agent_validation
install -d -o nas-agent-poller -g nas-agent-bridge -m 2750 "$BASE/spool/inbox"
install -d -o nas-agent-worker -g nas-agent-bridge -m 2750 "$BASE/spool/outbox"
install -o root -g nas-agent-policy -m 0640 "$BASE/code/nas_agent/examples/policy.json" "$BASE/admin/policy.json"
install -o nas-agent-worker -g nas-agent-bridge -m 0600 "$BASE/code/nas_agent/examples/worker.json" "$BASE/config/worker.json"
install -o nas-agent-poller -g nas-agent-bridge -m 0600 "$BASE/code/nas_agent/examples/poller.json" "$BASE/config/poller.json"
```

管理者がconfigのinstance_idを両役割で同じランダム16桁hex、agent_commitをレビュー済み完全SHAへ置換する。policyの上位許可SHA/入力/build/時間帯/個別hashも管理者だけが更新する。parentごとworker書込不可でなければ起動時に拒否する。Docker権限はroot相当なので、悪意あるworkerを完全隔離する保証ではない。

```sh
# 管理者が別の新規checkoutとvenvを作る。既存本番のPython環境は変更しない。
git clone https://github.com/hallo-102/jibunnyou.git "$BASE/approved-source"
git -C "$BASE/approved-source" checkout --detach 3ae9ce0dc5eaf41c2aa5d26c6e62fcef8dc26b92
python3.11 -m venv "$BASE/validation-venv"
"$BASE/validation-venv/bin/python" -m pip install -r "$BASE/approved-source/nas/requirements.txt"
chown -R root:nas-agent-bridge "$BASE/approved-source" "$BASE/validation-venv"  # 新規作成領域だけ。
cd "$BASE/code"  # 以下のagentコマンドはこのレビュー済みコードから実行する。
export PYTHONDONTWRITEBYTECODE=1  # 読取専用コードへcacheを書かない。
runuser -u nas-agent-worker -- test ! -w "$BASE/admin/policy.json"  # workerから上書きできない。
runuser -u nas-agent-worker -- test ! -w "$BASE/admin"  # atomic replaceによる差し替えもできない。
runuser -u nas-agent-worker -- python3.11 -c 'from nas_agent.config import load_policy; load_policy("/volume1/docker/keiba_nas_agent/admin/policy.json")'  # 全親の所有/権限を検査。
```

fine-grained PATはブラウザで対象repoだけを選び発行し、NAS管理者がNASローカルの0600ファイルへ保存する。poller write tokenはpoller所有、worker read tokenはworker所有、承認CLI用read tokenはroot所有に分ける。token値をチャット、Issue、shell引数、ログ、Git、コンテナへ入れない。既存Windowsの認証情報や本番rclone設定を転送しない。公開repoの匿名workerはrate limitが小さいため、初回実認証でAPI全Issue走査の回数/枠を確認し、長時間利用にはread-only tokenを用意する。

## 3. 起動前確認と最初のhealthcheck

以下はそれぞれの専用ユーザーの権限で行う。`PYTHONDONTWRITEBYTECODE=1` を設定し、agent codeから実行する。agent自身のrevision/clean検査は起動時に必須。

```sh
# poller利用者。実repo可視性/read権限を確認する。writeは初回報告まで未確認扱い。
python3.11 -m nas_agent verify-github --config /volume1/docker/keiba_nas_agent/config/poller.json
python3.11 -m nas_agent poll --config /volume1/docker/keiba_nas_agent/config/poller.json

# worker利用者。承認操作はこのuserで行わず、operations.mdの管理者用CLIで実施する。
python3.11 -m nas_agent worker --config /volume1/docker/keiba_nas_agent/config/worker.json
python3.11 -m nas_agent status --config /volume1/docker/keiba_nas_agent/config/worker.json
```

初期policyはapproved_requests/grantsが空、allow_build=false、実行時間帯nullなので重いジョブは実行しない。最初のhealthcheckを登録/承認し、worker→pollerを1回ずつ動かしてGitHubにSUCCEEDEDと履歴が出ることを確認する。

## 4. NASだけで固定snapshotを準備する

管理者がPR17を別の新規checkoutへ固定し、既存nas.workspace/snapshotを使って20260809の入力・2マスタ・オッズを新規の準備workspaceへコピーする。これらは現在の本番ファイルを読み取りコピーするだけで、昔のWindows固定snapshotと同一とは限らない。

```sh
# root管理者が先に作った別checkout/venvを使う。agent codeのcheckoutは変更しない。
cd "$BASE/approved-source"
git rev-parse HEAD  # 承認したPR17 SHAであることを読み取り確認する。
test ! -e /volume1/docker/keiba_agent_input_preparation || exit 1  # 新規準備領域だけ。
"$BASE/validation-venv/bin/python" -m nas.workspace --root /volume1/docker/keiba_agent_input_preparation --production-root /volume1/docker/keiba_yosou_2026_nas
"$BASE/validation-venv/bin/python" -m nas.snapshot --workspace /volume1/docker/keiba_agent_input_preparation \
  --root /volume1/docker/keiba_yosou_2026_nas --destination /volume1/docker/keiba_agent_input_preparation/snapshots/20260809 --day 20260809
sha256sum /volume1/docker/keiba_agent_input_preparation/snapshots/20260809/manifest.json  # 実測SHAをpolicyへ固定する。
chown -R root:nas-agent-policy /volume1/docker/keiba_agent_input_preparation  # 新規コピーだけ。原本のACLを変更しない。
chmod -R u=rwX,g=rX,o= /volume1/docker/keiba_agent_input_preparation  # workerへ読取だけを与える。
cd "$BASE/code"  # 承認CLIと常駐workerはagent codeから実行する。
```

管理者がpolicy.inputsのrootをこの固定コピーへ向け、manifest_sha256とracedayを固定する。読み取り専用の所有/権限にし、workerの入力元にする。NAS内部で完結するためWindowsは必要ない。同じsnapshot/model・同じレビューSHAのWindows基準runがNASに保存済みならreference_root/reference_manifest_sha256も固定する。基準にもREADY/STATUS/manifest/runtime/predictionが必要で、verify_completedで検査する。基準がなければPARTIAL/Windows比較未確認を明示し、結果を比較済みと認定しない。

## 5. 受入ジョブと常駐化

本番と重ならずtimeout全体が収まるJST時間帯をpolicyへ設定する。例12:00〜17:00は例示であり、実際の本番予定/余力を確認して決める。Dockerfile/依存/contextとbuild daemonの資源を確認し、別承認後にだけallow_build=trueにする。RAM2GiB/CPU1/PID256は検証containerの上限。build daemonの上限は別に確認が必要で、不明ならbuildを許可しない。

issue16_acceptanceの個別依頼hashを承認し、--serveなしで1回の実機受入を行う。成功/失敗/保護対象前後差分/成果物SHAを確認してから、管理者がテンプレートを実NASに合わせ確認し、**専用2サービスだけ**を起動/自動起動に登録する。既存本番unit、cron、SSH/Tailscale、SFTP、rclone設定を変更しない。

```sh
# systemd対応を確認し、上記の実受入と専用サービス導入を別承認した後だけ実行する例。
test ! -e /etc/systemd/system/nas-agent-poller.service || exit 1  # 既存unitを上書きしない。
test ! -e /etc/systemd/system/nas-agent-worker.service || exit 1  # 同名unitがあれば調査して停止する。
install -m 0644 "$BASE/code/nas_agent/deploy/nas-agent-poller.service" /etc/systemd/system/nas-agent-poller.service
install -m 0644 "$BASE/code/nas_agent/deploy/nas-agent-worker.service" /etc/systemd/system/nas-agent-worker.service
systemctl daemon-reload  # 新規2unitを読み込む。既存unitの内容/有効状態は変更しない。
systemctl enable --now nas-agent-poller.service nas-agent-worker.service  # 専用2unitだけ。
systemctl status nas-agent-poller.service nas-agent-worker.service  # 新規サービスの稼働を確認する。
# 停止/再起動が必要な場合も対象は専用unitだけ。既存本番/定期処理を指定しない。
systemctl stop nas-agent-worker.service nas-agent-poller.service
systemctl start nas-agent-poller.service nas-agent-worker.service
```

Windows電源OFFでGitHub依頼→NAS実行→GitHub結果を確認する。NAS停止中に作った未期限切れ依頼の起動後処理と、RUNNING時電源断のINTERRUPTEDを確認する。この実機試験を完了するまではWindows不要の実証済みとは扱わない。
