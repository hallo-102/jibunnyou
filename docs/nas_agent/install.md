# UGREEN NASへの初回導入（まだ実行しない）

2026-11-25の読み取り調査に適合した手順。**導入は別承認後だけ**。このPRでNASへのファイル配置、user/group作成、パッケージ導入、Docker build/run、サービス登録は行っていない。

## 採用する配置と依存方式

BASEは `/volume1/keiba_nas_agent`。既存 `/volume1/docker` はroot所有でも777のため、管理者policyの全親検査を通らない。既存フォルダをchmod/chownせず、root:root/755だった `/volume1` 直下の未使用名を使う。

| 新規領域 | 所有・用途 |
|---|---|
| BASE、code、tools、config、secrets、spool親 | root管理、常駐userから書込不可。BASEはroot:bridge/0750 |
| admin、admin/policy.json | root:nas-agent-policy、2750/0640。workerはread only。全親もroot所有・group/other書込不可 |
| poller-state、spool/inbox | poller所有、0700/2750。inboxのgroup=bridge、workerはread only |
| worker-state、checkouts、spool/outbox | worker所有、0700/2750。outboxのgroup=bridge、pollerはread only |
| config/poller.json、secrets/github-issues.token | poller所有0600。PATはNASローカルだけ |
| config/worker.json | worker所有0600。公開repoの最初のhealthcheckではtoken_file=null |
| /volume1/keiba_nas_agent_validation | worker所有0700、候補run専用 |
| /volume1/keiba_nas_agent_inputs | 管理者所有の固定入力、workerは読取だけ。healthcheckには不要 |
| BASE/validation-venv | 後工程で作成する専用Python環境。healthcheckには不要 |

Git/Pythonの準備方針と既存imageの確認結果は [dependencies.md](dependencies.md)、管理者cron/UGOSタスク/起動後の処理条件と検証時間帯は [preflight.md](preflight.md) を参照。

初回healthcheckに必要なのは **既存のPython 3.11標準ライブラリと専用Git**。pip/ensurepip/venv、予想ライブラリ、Docker build、systemd登録を先に要求しない。Gitのclean checkout検査は省略しない。初期policyはbuild=false、時間帯null、grants/個別hash空を維持する。

## 1. 一度だけ行う管理者準備

以下は**将来の承認後**にNAS管理者(root)が実行する最小の準備。UGOSで新規system user/groupが保持されるか管理画面で先に確認する。未対応なら停止し導入方式を再確認する。既存のACL、user、group、sudo範囲、SSH、Tailscaleを変更しない。同名パス/user/group/unitがあれば上書きせず停止する。

管理者が事前に別の信頼できるDebian環境で用意・レビューした **bookworm/arm64のGit .deb** と、署名済みDebianメタデータに基づく完全SHA256が必要。版・SHA・取得元は現時点では未選定。NASのAPT候補は空なので、NASでapt update/installを実行しない。既存6ライブラリは検出済みだが、選定debの実依存は展開後のlddで確認する。

```sh
# 別承認後のrootシェル。値は実際にレビューしたartifactへ置換する。
set -eu
BASE=/volume1/keiba_nas_agent
AGENT_SHA='<レビュー済みagent完全SHA>'
GIT_DEB='<NAS上の承認済みGit_arm64.debの絶対パス>'
GIT_SHA256='<Debian署名検証済みメタデータから得た64桁SHA256>'
test ! -e "$BASE"  # 未使用の新規領域だけ。以後、失敗時は原因を調べてから再開する。
# 新規アカウントだけ。UGOS対応/同名未使用を先に確認済みであること。
/usr/sbin/groupadd --system nas-agent-bridge
/usr/sbin/groupadd --system nas-agent-policy
/usr/sbin/useradd --system --no-create-home --shell /usr/sbin/nologin --gid nas-agent-bridge nas-agent-poller
/usr/sbin/useradd --system --no-create-home --shell /usr/sbin/nologin --gid nas-agent-bridge --groups nas-agent-policy,docker nas-agent-worker
umask 027  # 新規コード/設定の他userからの書込を禁止する。
install -d -o root -g nas-agent-bridge -m 0750 "$BASE" "$BASE/tools" "$BASE/tools/git" "$BASE/tools/bin"
printf '%s  %s\n' "$GIT_SHA256" "$GIT_DEB" | sha256sum -c -  # 取得元と完全性を確認する。
test "$(dpkg-deb -f "$GIT_DEB" Package)" = git
test "$(dpkg-deb -f "$GIT_DEB" Architecture)" = arm64
dpkg-deb -x "$GIT_DEB" "$BASE/tools/git"  # 展開だけ。dpkg -i/maintainer script/UGOSのDB変更はしない。
ldd "$BASE/tools/git/usr/bin/git"  # not foundやABI不一致があれば停止。既存libを上書きしない。
ldd "$BASE/tools/git/usr/lib/git-core/git-remote-https"  # HTTPS helperの依存も同様に確認する。
"$BASE/tools/git/usr/bin/git" --version  # ここから専用Gitだけを使う。
"$BASE/tools/git/usr/bin/git" --exec-path="$BASE/tools/git/usr/lib/git-core" -c credential.helper= clone --single-branch --branch codex/issue18-nas-agent https://github.com/hallo-102/jibunnyou.git "$BASE/code"
"$BASE/tools/git/usr/bin/git" -C "$BASE/code" checkout --detach "$AGENT_SHA"  # mainを変更しない。
chown -R root:nas-agent-bridge "$BASE/code"  # 今作成した新規checkoutだけ。
install -o root -g nas-agent-bridge -m 0755 "$BASE/code/nas_agent/deploy/git" "$BASE/tools/bin/git"
export PATH="$BASE/tools/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export PYTHONDONTWRITEBYTECODE=1  # 読取専用codeにcacheを書かない。
# 設定/状態/片方向spoolだけを作る。validation-venvは作らない。
install -d -o root -g nas-agent-bridge -m 0750 "$BASE/config" "$BASE/secrets" "$BASE/spool"
install -d -o root -g nas-agent-policy -m 2750 "$BASE/admin"
install -d -o nas-agent-poller -g nas-agent-bridge -m 0700 "$BASE/poller-state"
install -d -o nas-agent-worker -g nas-agent-bridge -m 0700 "$BASE/worker-state" "$BASE/checkouts" /volume1/keiba_nas_agent_validation
install -d -o nas-agent-poller -g nas-agent-bridge -m 2750 "$BASE/spool/inbox"
install -d -o nas-agent-worker -g nas-agent-bridge -m 2750 "$BASE/spool/outbox"
install -o root -g nas-agent-policy -m 0640 "$BASE/code/nas_agent/examples/policy.json" "$BASE/admin/policy.json"
install -o nas-agent-worker -g nas-agent-bridge -m 0600 "$BASE/code/nas_agent/examples/worker.json" "$BASE/config/worker.json"
install -o nas-agent-poller -g nas-agent-bridge -m 0600 "$BASE/code/nas_agent/examples/poller.json" "$BASE/config/poller.json"
install -o nas-agent-poller -g nas-agent-bridge -m 0600 /dev/null "$BASE/secrets/github-issues.token"
# 新規configのSHA/共通instanceだけを設定する。秘密はこの処理に含めない。
python3.11 - "$BASE" "$AGENT_SHA" <<'PY'
import json, secrets, sys
from pathlib import Path
base, commit = Path(sys.argv[1]), sys.argv[2]
instance = secrets.token_hex(8)  # 2役割で同じinstanceを使う。
for role in ('poller', 'worker'):
    path = base / 'config' / (role + '.json')
    config = json.loads(path.read_text())
    config.update(agent_commit=commit, instance_id=instance)
    path.write_text(json.dumps(config, indent=2) + '\n')  # 新規ファイルの所有/0600を維持する。
PY
cd "$BASE/code"  # 以後のCLIもレビュー済みagentから実行する。
```

NASローカルで対話入力してpoller用repo限定PATを0600ファイルに保存する。Issues read/write、Contents read、Metadata readのみ。トークンをチャット/Issue/コマンド引数へ書かない。

```sh
# rootシェルの同じPATHから実行。getpassは端末から非表示で入力する。
python3.11 -c 'import getpass; from pathlib import Path; p=Path("/volume1/keiba_nas_agent/secrets/github-issues.token"); p.write_text(getpass.getpass("NAS poller PAT: ").strip()+"\n")'
# workerからpolicyとその親が書込不可で、全親がroot所有であることを実測する。
/usr/sbin/runuser -u nas-agent-worker -- test ! -w "$BASE/admin/policy.json"
/usr/sbin/runuser -u nas-agent-worker -- test ! -w "$BASE/admin"
/usr/sbin/runuser -u nas-agent-worker -- python3.11 -c 'from nas_agent.config import load_policy; load_policy("/volume1/keiba_nas_agent/admin/policy.json")'
```

Gitのdeb展開はNASで未実施。lddに不足があれば進まない。private Gitでも必須libが増える場合、別承認でtools配下への専用展開を再評価し、UGOSの既存libやパッケージ管理へ自動で追加しない。Docker権限はroot相当という既存の信頼境界を維持する。

## 2. 最初のhealthcheck（サービス登録なし）

上記準備が済んだrootシェルから以下だけを行う。最初にChatGPTがexamples/healthcheck.job.jsonのJSONを新しい `[NAS-JOB]` Issueとして作り、job_idとexpires_atを更新する。Issue番号とinspectのhashを管理者が確認する。公開repoのこの短い試験はworker/管理者readを無認証でも行えるが、API枠が不足する場合は別のread-only PATを用意する。pollerのwrite権限は実報告まで未確認。

```sh
# 上記と同じBASE/AGENT_SHA/PATH/PYTHONDONTWRITEBYTECODEを使う。
cd "$BASE/code"
# runuserによるPATH初期化に依存せず、専用Gitとcache禁止を明示して起動する。
poller() { /usr/sbin/runuser -u nas-agent-poller -- /usr/bin/env PATH="$PATH" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3.11 -m nas_agent "$@"; }
worker() { /usr/sbin/runuser -u nas-agent-worker -- /usr/bin/env PATH="$PATH" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3.11 -m nas_agent "$@"; }
poller verify-github --config "$BASE/config/poller.json"
JOB_ISSUE='<新しいhealthcheckのIssue番号>'  # 初回はhealthcheckだけ。
python3.11 -m nas_agent inspect --policy-file "$BASE/admin/policy.json" --agent-commit "$AGENT_SHA" --issue "$JOB_ISSUE"
REQUEST_HASH='<inspectで確認した64桁hash>'  # 本文/作成者/期限を確認してから承認する。
python3.11 -m nas_agent approve --policy-file "$BASE/admin/policy.json" --agent-commit "$AGENT_SHA" --issue "$JOB_ISSUE" --expected-hash "$REQUEST_HASH"
poller poll --config "$BASE/config/poller.json"
worker worker --config "$BASE/config/worker.json"
poller poll --config "$BASE/config/poller.json"
worker status --config "$BASE/config/worker.json"
```

GitHubにSUCCEEDED・状態履歴・NASのPython/architectureが出たところで初回healthcheckを終了する。候補予想の受入PASS、Windows OFF、本番不変、ARM64 Docker buildが済んだとは扱わない。失敗時に同じjob_idを再実行せず、原因を調べ必要なら新規IDを再承認する。

## 3. healthcheckの後に別承認するもの

- [dependencies.md](dependencies.md) の専用validation-venvと固定PR17 checkout、固定入力/参照のmanifest。全て新規rootへ配置する。
- [preflight.md](preflight.md) の管理者タスク・起動後の日次完了・非開催日12〜17時の競合確認。build資源未確認ならallow_build=falseのまま。
- issue16_acceptanceの単発実受入。その後で初めて、実NASで確認した `deploy/*.service` を専用unitとして登録する。テンプレートは新BASEとprivate Git PATHに変更済み。systemdの制約/UGOS再起動後の永続性を実測するまで自動起動を登録しない。

元の `/volume1/docker/keiba_yosou_2026_nas` は読取対象のまま。既存サービス、モデル、データ、定期処理、ACL、SSH/Tailscale、rcloneを変更しない。
