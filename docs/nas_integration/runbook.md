# 隔離環境での実行・受入手順（PR #17修正後）

main・本番・既存モデル/データ・定期処理・配信を変更しない。Python 3.11の候補専用venvにnas/requirements.txtを導入する。ソースcheckoutと作業workspaceは別ディレクトリにする。既存の本番data/outputにマーカーを後付けして許可する手順はない。

## 初期化・固定入力

```sh
# 新規の独立checkoutでレビュー済み40桁commitを固定する。
git clone https://github.com/hallo-102/jibunnyou.git /volume1/docker/keiba_issue16
cd /volume1/docker/keiba_issue16
git checkout --detach <レビュー済み40桁commit>
git status --porcelain  # cleanであることを確認する。

# workspaceはまだ存在しない専用パスを指定する。本番rootの指定は必須。
python3.11 -m nas.workspace --root /volume1/docker/keiba_issue16_artifacts \
  --production-root /volume1/docker/keiba_yosou_2026_nas
export TASK_WORKSPACE=/volume1/docker/keiba_issue16_artifacts  # 候補だけの書込ルート。

# 指定日の入力・オッズ・2マスタ・旧保存予想だけを新規snapshotへコピーする。
python3.11 -m nas.snapshot --workspace "$TASK_WORKSPACE" \
  --root /volume1/docker/keiba_yosou_2026_nas \
  --destination "$TASK_WORKSPACE/snapshots/20260809" --day 20260809
```

必要ならsnapshot作成時に `--calendar <独立した開催カレンダー>` を追加する。calendar.txtがsnapshotにあればcandidateは自動使用し、非開催日はNO_RACEのみで予想しない。カレンダー未同梱時は既存固定入力を根拠にしたoffline再生と記録する。未固定カレンダーを黙って使用しない。

nas.workspaceはコード/既知NAS/指定したproduction-rootと重なるルートや既存ディレクトリを拒否する。追加保護パスは `--protect <path>` を指定できる。runs/legacy/comparisons/snapshots以外への出力、固定入力への書込、symlink/junctionを通した出力を拒否する。

## ARM64 composeのホスト側検査と一回実行

```sh
# 本番と独立したworkspace・snapshot・runs実パスを、Docker起動前に検査する。
export TASK_COMMIT="$(git rev-parse HEAD)"
python3.11 -m nas.launch --workspace "$TASK_WORKSPACE" \
  --snapshot "$TASK_WORKSPACE/snapshots/20260809" --output "$TASK_WORKSPACE/runs" \
  --production-root /volume1/docker/keiba_yosou_2026_nas --commit "$TASK_COMMIT" --dry-run

# 候補専用出力への書込権限がある非root UID/GIDを指定する。本番の権限は変更しない。
export CANDIDATE_UID="$(id -u)"
export CANDIDATE_GID="$(id -g)"
python3.11 -m nas.launch --workspace "$TASK_WORKSPACE" \
  --snapshot "$TASK_WORKSPACE/snapshots/20260809" --output "$TASK_WORKSPACE/runs" \
  --production-root /volume1/docker/keiba_yosou_2026_nas --commit "$TASK_COMMIT"
```

composeを直接実行して任意のホストパスをrwマウントしない。必ずnas.launchを使用する。ラッパーはproduction-root/ホストworkspace実パスを初期化時の記録と突合し、dirty checkout・commit不一致・危険な出力先を拒否する。コンテナへはsnapshotとマーカーだけro、検査済みrunsだけrwで渡す。別project、networkなし、restart=noを維持する。Docker engineやARM64実機での実行は今回未確認。

## ホストでの候補/旧NAS再生・統制比較

```sh
# 共通特徴量・補正を固定し、重みだけ変える診断を通常候補と同時に行う。
python3.11 -m nas.candidate --workspace "$TASK_WORKSPACE" \
  --snapshot "$TASK_WORKSPACE/snapshots/20260809" --output "$TASK_WORKSPACE/runs" --weight-control

# 旧NAS静的ソースをさらにコピーして別プロセスで実行する。本番dailyは呼ばない。
python3.11 -m nas.legacy_replay --workspace "$TASK_WORKSPACE" --source <NASソース静的コピー> \
  --snapshot "$TASK_WORKSPACE/snapshots/20260809" --output "$TASK_WORKSPACE/legacy"

# 入力/model/実装の判定は保存済みSHAから自動生成する。出力は新規JSONだけ。
python3.11 -m nas.compare --workspace "$TASK_WORKSPACE" --left <legacy-run>/prediction.xlsx \
  --left-raw <legacy-run>/raw_scores.json --right-run <candidate-run> \
  --output "$TASK_WORKSPACE/comparisons/comparison_20260809.json"

# 変更範囲のfixtureと共通順位テストを実行する。
python3.11 -m pytest tests/test_nas_review.py tests/test_nas_integration.py tests/test_unique_ranking.py -q
```

旧NAS再生は予想に必要なappの10モジュールと20260730重みだけを新規legacy runへコピーし、source_manifestでSHAを検証する。子プロセスのcwd/KEIBA_BASE_DIR/OUTPUT/WEIGHTSはコピー内、入力/MASTER/ODDSはsnapshotへ限定する。SUCCESS_REPORT・予想・丸め前診断・市場補完・メタデータの書込先は同じrun内。収集、upload、maintenance、schedulerはコピー/起動しない。これは取得済み旧コードの書込経路監査であり、任意の第三者PythonをOSレベルでサンドボックス化する機能ではない。

provenance JSONを `--provenance` で渡しても未検証メモにしか使用しない。両側のinput_manifest SHA、実体/実効モデルSHA、ソースSHAを照合し、欠損は未確認と表示する。weight_control.jsonは通常予想と同じスコア入力を採取して2重みだけを変更する。補正値の完全一致と明示20260817/通常候補の一致を確認する。旧NAS対候補の複合差全体や買い目/ROIへの寄与を完全分解したとは扱わない。

## PR #10/#11との接続契約と後続受入

| 段階 | 読み取り/受渡し契約 | 後続の受入条件 |
|---|---|---|
| #10 発走前pre | manifestのday/mode/files、input.xlsx、odds.csv、mastersを別snapshotとしてro参照可能 | preのSHA/締切/マスタ時点を#10側で確認。candidateはprospective入力でもoffline/retrospective出力に限定 |
| 正式READY | 本番data/output/status/READYは監視/更新しない | 候補READYを本番READYへコピーしない。#10の本番watcherへ候補runを登録しない |
| 候補完成 | STATUS=COMPLETED、READY=READY、公開manifestとprediction.xlsxのSHA一致を全て要求 | nas.candidate.verify_completed(run)で照合。失敗/NO_RACE/途中状態を成功と扱わない |
| #11 回顧 | manifest/runtime/prediction/inputはreview APIと互換。原順位と回顧順位を分離 | 候補のコピーだけで全馬/払戻が揃う結果コピーと照合する。正式preをAPIに直接渡さない。書込post先も候補workspace内で新規確保する別アダプターをレビューするまで接続しない |
| history/前向き | legacy_forward_generation=null、retrospectiveを維持 | #10 historyや旧世代へ算入しない。新しい承認/コード固定後の開催日から別世代を設計する |
| Drive/Gmail | sync_planはdestination=null、executed=false | 実転送は別途明示された検証先のみ。既存配信先やGmailは接続しない |
| マスタ更新/定期処理 | snapshotはro。既存scheduler/daily/update_mastersを温存 | コピーの更新と候補ジョブだけを別レビューし、正式マスタ・06:45本番ジョブを変更しない |

Windows/ARM64 Linuxは同じレビューcommit・snapshotで再生し、モデル実体/実効SHAを確認する。特徴量/scoreはatol=rtol=1e-9、順位/TOP5/固定券は完全一致で比較する。差は依存版/各レースまで説明する。実NASのimage/mount/ログ/コード/モデル/READY/STATUS/定期処理の実行前後検査は未実施のまま。ローカル静的コピーの不変確認で代替しない。切替はtransition.mdの別承認とする。
