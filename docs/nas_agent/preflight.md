# 実機事前確認と初回検証時間帯

2026-11-25にSSH `okino@192.168.1.92:22` で読み取り調査した。SSHはユーザーが一時有効化した経路。既存SSH/Tailscale設定は変更していない。Debian 12/UGOS 1.21.1.0016、Linux 6.1.84 ARM64、8 cores、Python 3.11.2、Docker 29.6.2/Compose 5.1.3、systemd 252。メモリ約7.5GiB（available約4.8GiB）、volume1空き約3.2TiB。これは調査時点の値であり実行直前にも確認する。

GitHub/web/API/rawのHTTPS疎通は成功。PAT/Issue書込は未確認。7containerが稼働し、本番scheduler・回顧・観測・OSM・SFTP・Tailscaleが並行稼働していた。専用agentのuser/group/unit/新規rootは未作成。useradd/groupadd/runuserは/usr/sbinに存在するが、UGOSが新規system account/unitを再起動や更新後も保持するかは未検証。

## 管理者限定cronとUGOSタスクの読み取り手順

一般権限のsystemd timers、/etc/crontab、/etc/cron.dは確認済み。root/個別user crontabとUGOSの全タスクは未確認。sudo -n -lは認証要求となった。Docker権限を使ってhostの読取制限を迂回しない。

管理者がNASの既存SSH端末でsudo認証し、以下の**表示だけ**を行う。パスワード/cron本文の秘密をチャットへ転記せず、必要な実行時刻・対象処理・最大所要時間だけを記録する。

```sh
# 管理者認証が必要。-e、-r、編集、reload/start/stop/enableは使わない。
sudo crontab -l -u root  # rootの実行時刻を確認する。
sudo crontab -l -u okino  # ユーザー定期処理の時刻も確認する。
systemctl list-timers --all --no-pager  # 次回/前回時刻。停止や再設定はしない。
systemctl show taskmgr_serv jobmgr_serv jobmgr_system_model jobmgr_user_model -p ActiveState -p SubState -p FragmentPath  # UGOS管理機能の状態だけ。
```

UGOS管理画面でタスク管理/ジョブ管理/バックアップ・同期/電源スケジュールを**表示**し、名称、実行user、曜日/時刻、起動時実行、再試行、次回実行、所要時間を控える。名称や画面経路は実機版で確認し、DBパスを推測して編集/実行しない。systemdサービスが存在するだけではUGOSタスク全件を確認したことにならない。権限不足や画面未確認が残れば安全な時間帯は未確定とする。

## NAS起動後に日次予想が動く条件

実際のscheduler環境値はAsia/Tokyo、KEIBA_DAILY_RUN_TIME=06:45、poll=30秒。稼働ソース `scripts/run_scheduler.py` のscheduler_decisionは次の順序。

| 現在条件 | 判断 |
|---|---|
| 当日06:45より前 | WAIT |
| 当日READYが既存実装で完成検証できる | SKIP_READY |
| 当日のSCHEDULED_ATTEMPTファイルが存在 | SKIP_ATTEMPTED |
| 上記以外 | RUN |

**08時や12時に起動しても、06:45を過ぎREADY/attemptがなければ約30秒のpollで当日処理を開始し得る。** 非開催日でもまず日次処理が動きNO_RACEを判定する可能性がある。過去の「08:00〜22:30稼働」は現在の電源設定の確認ではなく、調査時uptimeは17日超だった。

実行直前は当日の `data/output/status/SCHEDULED_ATTEMPT_YYYYMMDD.json`、`STATUS_YYYYMMDD.json`、`READY_YYYYMMDD.json` の存在/状態/時刻だけを読み、日次/回顧/最適化の子処理が残っていないことを照合する。READY完成はファイルの存在だけで断定しない。attemptがFAILEDでも自動再試行とは限らず、原因未確認なら候補検証を始めない。

```sh
# 既存JSONの状態フィールドだけを表示する。保存/モジュール起動/生ログ転記はしない。
python3.11 -B - <<'PY'
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
day = datetime.now(timezone(timedelta(hours=9))).strftime('%Y%m%d')
root = Path('/volume1/docker/keiba_yosou_2026_nas/data/output/status')
for kind in ('SCHEDULED_ATTEMPT', 'STATUS', 'READY'):
    path = root / (kind + '_' + day + '.json')
    if not path.is_file():
        print(kind, 'MISSING')  # 起動直後は未実行の可能性がある。
        continue
    data = json.loads(path.read_text())
    print(kind, {k: data[k] for k in ('status', 'started_at', 'completed_at', 'exit_code') if k in data})
PY
```

`python -m scripts.run_scheduler --check` は使用しない。読み取り調査でmainが--checkの前にloggerを初期化すると確認したため、ログ作成の可能性がある。確認のために本番Pythonモジュールをimport/起動せず、ソースと状態ファイルを読み取る。

## 非開催日12:00〜17:00（JST）の候補条件

これは**候補枠であって許可済み枠ではない**。policyの初期値はexecution_window_jst=null、allow_build=falseのまま。healthcheckだけなら予想依存/build不要だが、初回の実予想受入は次を全て満たしてから単発で行う。

1. 実行日がJRA非開催であることを信頼できるカレンダーと当日NO_RACE結果で確認する。入力対象日20260809とは別。曜日だけで決めない。
2. 管理者cron/UGOSタスクを確認し、12〜17時にバックアップ/同期/重い処理や電源OFF予定がない。未確認なら保留する。
3. 起動後の日次処理が正常終端し、RUNNING/失敗未解決がない。回顧serviceは06〜22時に動き、マスタ更新は10時以降もあり得る。前日分回顧/再試行/同期の残りも確認する。
4. 最適化は非開催日07〜12時に開始し得る。12時を過ぎても開始済み処理が残る可能性があるため、状態/子プロセス/負荷で完了を確認する。常駐OSM等は停止しない。
5. 目安としてavailableメモリ4GiB以上、1分load<4、空き20GiB以上を開始条件とし、docker stats/free/uptimeを間隔を空けて2回確認する。これは今回の8core/8GB機の保守的な運用目安であり、競合ゼロの証明ではない。悪化すれば延期する。
6. 候補containerは既存上限CPU1/2GiB/PID256、timeout7200秒。17時の30分前にtimeout全体が終わるよう、開始は12:00〜14:30までとする。遅れれば翌候補日へ延期する。
7. Docker daemon側buildの資源上限/依存取得時間は未検証。container/worker上限でdaemon buildを制限したと思い込まない。新規専用builder等の方式・資源を別承認し確認するまでbuild=falseを維持する。既存daemon設定は変更しない。

```sh
# 読み取りだけで負荷/稼働を確認する。何も起動・停止しない。
date -Is
free -b
uptime
df -h /volume1
docker stats --no-stream
docker ps --format '{{.Names}} | {{.Status}}'
docker top keiba_yosou_2026_nas-keiba-scheduler-1 -eo pid,ppid,comm,etime  # argv/秘密を出さず子処理を確認する。
docker top jra-continuous-improvement-keiba-review-1 -eo pid,ppid,comm,etime
```

全条件を満たした後だけ管理者が例 `{ "start": "12:00", "end": "17:00" }` をローカルpolicyへ設定し、入力SHA・新job_idを個別承認する。初回は--serve/systemd自動起動を使わない。現行agentは非開催日/負荷の自動判定を追加しておらず、管理者の事前確認を承認条件とする。
