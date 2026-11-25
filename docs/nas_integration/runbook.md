# 隔離環境での実行・受入手順

この手順は候補環境だけを作る。本番compose、.env、scheduler、データ、READY/STATUS、Drive宛先、Gmailを変更しない。本Issue内では実NASで実行していない。コマンド中のcommitはレビュー済みPR headの**40桁SHA**へ置換する。`main` や可変ブランチ名を実行バージョンに使わない。

## ソースと固定入力

以下はNAS上の本番とは別ディレクトリで行う。`/volume1/docker/keiba_issue16` は新規の候補専用配置例。既存ファイルのある場所を使わない。認証/本番設定はコピーしない。

```sh
# レビュー済みcommitを独立checkoutする。ここでは本番rootをcheckout先にしない。
git clone https://github.com/hallo-102/jibunnyou.git /volume1/docker/keiba_issue16
cd /volume1/docker/keiba_issue16
git checkout --detach <レビュー済み40桁commit>
git status --porcelain  # 出力がないことを確認する。
git rev-parse HEAD  # レビューSHAと照合する。

# コピー専用の作業領域を新規作成する。既存なら別名にする。
mkdir -p /volume1/docker/keiba_issue16_artifacts
python3.11 -m nas.snapshot \
  --root /volume1/docker/keiba_yosou_2026_nas \
  --destination /volume1/docker/keiba_issue16_artifacts/snapshot_20260809 \
  --day 20260809
```

snapshotツールには `nas/requirements.txt` のPython環境が必要。NASホストに導入せず、別venvまたはイメージ内で実行してよい。元データは読取専用でマウントし、出力先だけを書込可能にする。snapshotは入力・当日オッズ・必要な2マスタ・存在すれば旧保存予想だけをコピーし、コピー前後SHAを照合する。保存予想は同一マスタ/時点の証明にはならない。必要なら独立の開催カレンダーを `--calendar <calendar.txt>` で固定する。

NAS稼働中の確認は読み取りに限る。`docker compose ps` と稼働image/mount、当日ログ・状態、対象コードSHAを記録する。ローカルコピーと一致しない場合は本番から必要ファイルだけを新しい読取専用ソースコピーへ取得し、差分表を更新してから実験する。コード取得時に.env/credentials/認証や生データをGitHubへ登録しない。

## ARM64候補コンテナ

```sh
# source/outputの双方が本番とは別の実パスであることを確認する。
export CANDIDATE_COMMIT="$(git rev-parse HEAD)"
export CANDIDATE_SNAPSHOT=/volume1/docker/keiba_issue16_artifacts/snapshot_20260809
export CANDIDATE_OUTPUT=/volume1/docker/keiba_issue16_artifacts/candidate_output
mkdir "$CANDIDATE_OUTPUT"  # 新規の空ディレクトリを確保する。
export CANDIDATE_UID="$(id -u)"
export CANDIDATE_GID="$(id -g)"
docker compose -p keiba-issue16-candidate -f nas/compose.yaml config --quiet  # 構文/必須変数を検査する。
docker compose -p keiba-issue16-candidate -f nas/compose.yaml build  # 候補イメージだけを作る。
docker compose -p keiba-issue16-candidate -f nas/compose.yaml run --rm candidate  # 一回だけ実行する。
```

ネットワークを禁止しているため、開催判定は既存固定入力を根拠にする。明示カレンダーを使う場合、commandに `--calendar /snapshot/calendar.txt` を追加する。カレンダーもmanifestのSHAに含まれていないと停止する。非開催時はNO_RACEのみでREADYを作らない。実行失敗時はFAILED、READYなし。再実行は新しいrunになるため過去結果を上書きしない。

本番schedulerの06:45実行は変更しない。候補を日次確認する場合も、固定コピーの準備後に上記一回実行を手動で行う。候補の定期起動、収集接続、コピーしたマスタの更新、実Drive検証先への転送は別レビューで設計する。本Issueではschedulerやrcloneの実行経路を接続しない。

## 同一入力の旧NAS再生・比較

旧NASコードは必要なapp/*.pyと正式20260730重みを取得した**静的コピー**を指定する。コードはさらに新規作業ディレクトリへコピーされ、子プロセスで実行される。本番run_daily_pipelineは起動しない。

```sh
# 同じsnapshotを参照し、旧コードのdense順位はそのまま保存する。
python3.11 -m nas.legacy_replay --source <NASソース静的コピー> \
  --snapshot "$CANDIDATE_SNAPSHOT" --output <候補専用legacy出力>

# model/implementation/dataの検証状態を日本語で記したprovenance.jsonを用意する。
python3.11 -m nas.compare --left <legacy-run>/prediction.xlsx \
  --left-raw <legacy-run>/raw_scores.json --right-run <candidate-run> \
  --provenance <provenance.json> --output <候補専用comparison.json>

# Linux基本テスト。ARM64実機であるかは別途uname -mで記録する。
python3.11 -m pytest tests/test_nas_integration.py tests/test_unique_ranking.py -q
```

provenance.jsonには `model`（例:20260730 vs 20260817）、`implementation`（旧NASソースSHA vsレビューcommit）、`data`（両runのinput_manifest SHA一致）を記す。比較結果は生データ由来なのでGitHubへ登録しない。

Windows側でも同じレビューcommitを独立checkoutし、同じsnapshotのSHAを照合して `python -m nas.candidate --snapshot <snapshot> --output <Windows候補専用出力>` を実行する。そのpredictionをleft、Linux runをrightとして比較する。同じモデルの実体・実効SHAを確認した上で、features/scoreはatol=rtol=1e-9以内、順位/TOP5/買い目は完全一致を要求する。差が出た場合はPython/pandas/numpy/torch版と各レース差分を保存し、OS差を未説明のまま採用しない。

## 回顧・受入

候補runはPR #11の `continuous_improvement.review.review(pre, results)` が読めるmanifest/runtime/prediction/inputを保存する。結果マスタのコピーと、**候補出力配下のrunだけ**をpreへ渡す。旧NAS preを渡すと既存reviewがpre.parentへ結果を書くため、本番preを直接指定しない。結果の全馬/払戻が揃わなければ停止する。候補はretrospectiveとなりhistoryや旧前向き世代へ自動算入しない。結果取得・正式マスタ更新・Drive同期はこの呼出しに接続しない。

受入では次を確認する。

- manifest固定SHAと、モデル実体/実効SHA、実行commitを照合する。
- STATUS=COMPLETED、READYのSHAとprediction.xlsx、完成検査の全レース/対象外理由、1位/TOP5/馬集合を確認する。
- 比較JSONで特徴量、丸め前score、順位、TOP5、買い目、対象外理由、片側のみの馬を確認する。モデル差と実装差が残る比較は原因を断定しない。
- 非開催、同点、欠損/取消、失敗時READYなしのfixtureテストを実行する。
- 稼働中NASのコード/重み/設定/対象原本/本番READY/STATUS/定期処理を実行前後に照合する。アクセスできなかった項目は未確認と記す。
- sync_planのdestination=null / executed=falseを確認する。既存Drive/Gmailへ送信しない。
