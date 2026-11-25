# 検証報告（2026-11-25）

本番採用の判定ではなく、隔離した統合候補の検証結果。実データ由来のExcel/CSV、入力、馬/レース単位比較JSON、ログ、認証はGitHubへ登録していない。集計のみ [real_day_summary.json](real_day_summary.json) に記載する。

このファイルの旧23件・実データ集計はd60258e以前の初回検証記録。PR #17の修正内容・最新39件の最小回帰は [pr17_review.md](pr17_review.md)、988e1fdでの最終通し検証は [review_summary.json](review_summary.json) を参照する。共通特徴量/補正を固定した重みだけの統制比較と機械的な出所照合を追加した。最新手順はworkspace必須のrunbookに更新済み。

## 検証済み

- 着手時main SHAは `178855fd117301131c8e3ebf70ce8c5e9e327728`。Windows `keiba_yosou_2026-2` の主要11ファイルはmainと一致（loaders/utilsのみ改行差）。候補でも共通11ファイル・production/forward設定・重みを変更していない。
- モデルは20260817、legacy、31グループ/3131要素。実体SHA256 `bb086384eb58c5abae6ef7104de94e83005c318b3be3900ef5c4df15fca1020d`、実効SHA256 `8325f5c0748aa49076631b02424f2104a7924d0b39a4148d4703f687a937a789` が既存registryの検証と一致。
- ローカルNAS静的コピーから20260809の入力・2マスタ・オッズをSHA固定し、旧NAS20260730と候補20260817を別プロセス・新規出力先で実行した。**入力36レース495頭、予想対象27レース376頭、対象外9レース**。候補は完成Excel検査、原入力全レース収録、未分類0、予想馬集合、丸め前score非有限なし、各レースの一意順位・1位・TOP5を満たし、候補READYを生成した。
- 旧NAS再生も同一snapshot manifest SHAを使用。共通376頭、片側だけの予想馬0。特徴量の実装差としてペース条件一致と馬場関連の差を検出。旧出力の `field_size_adjusted_finish` は未保存のため比較不能と明示する。
- 丸め前score/totalは376頭で異なり、順位は233頭、1位は8レース、TOP5集合は17レース、**固定された3連複/馬連/ワイドの馬番または購入額は11レース**で異なる。異なるモデルと実装を使用した結果で、一致や改善を要求・認定していない。score/gap等を含む買い目行全体の差と、実際の固定券の差は分けて記録する。
- 旧保存Excelとの参考比較も行ったが、その当時のマスタ/オッズ時点は不明。同一snapshotでの再生比較と混同しない。
- ローカルNAS静的ソース・Windows比較対象・本番相当のローカル入力/オッズ/2マスタ/旧保存予想は、読取前後のSHAが一致した。mainの保護対象ファイルに差分なし。
- `docker compose -f nas/compose.yaml config --quiet` は必須変数を指定して終了0。Docker engineへの接続やイメージビルドを伴わない構文検査。

実データの詳細比較は候補専用 `outputs/` 配下に保存する。入力manifest SHA、両予想Excel SHA、runtime_idは集計JSONに記載。数値比較はatol=rtol=1e-9、順位/TOP5/固定券は完全一致で比較した。

## 基本テスト

実行コマンド:

```sh
# 合成fixtureの隔離/改変/非開催/失敗/取消/回顧契約と、既存順位テストをまとめて実行する。
python -m pytest tests/test_nas_integration.py tests/test_unique_ranking.py -q
git diff --check  # 空白エラーがないことを確認する。
```

結果は **23 passed**。DockerfileのCOPY対象だけを別ディレクトリに再現したimport検査でも、モデル読込と前向きBLOCKEDの再現を確認した。実データ通し実行の検証対象commitは `9cd152ffd808c9faacfa3c19c776d8f647ef7621`。実行時core/adapter SHAと現在ファイルの一致、入力・旧静的ソースの不変、候補READYと予想SHAの一致を確認した。

既存の全テストには実データ成果物や旧モデル期待値に依存するものがあり、今回は変更範囲に対応する上記の最小セットに限定した。Linux x86基本テスト用のGitHub Actionsを追加したが、GitHubへの書込制約により未実行。Actionsが成功してもARM64/NAS実証を代替しない。

## 未検証・停止条件

| 項目 | 状態 / 後続手順 |
|---|---|
| 実NASの稼働image/mount/設定/ログ/定期処理 | 接続していないため未確認。静的コピーとのSHA突合が必要 |
| 実NASの本番ファイル/READY/STATUS不変 | ローカルコピーの対象ソース/入力は不変確認済み。実NAS側は未確認であり、不変と断定しない |
| Linux ARM64ビルド・実行 | Docker engineが起動しておらず未実施。runbookの別projectでbuild/runする |
| Windows/main vs Linux同一20260817一致 | 共通ソース/モデルは同じ。Linux側の通し比較は未実施。固定snapshotを両OSで再生して確認する |
| モデル差と実装差の完全な因果分解 | 初回は複合差のみ。PR #17修正で共通特徴量と補正を固定した重みだけの統制比較を追加。旧NAS特徴量との相互作用を含む複合差全体や買い目/ROIの完全分解は未実施 |
| 実開催の発走前固定〜夕方回顧 | 今回は20260809のoffline再生。前向き実績なし。PR #10/#11の稼働確認を別途行う |
| 回顧全工程/払戻/Drive実転送/マスタ更新 | PR #11の回顧順位原本保全テストのみ。実結果との全工程は未実施。Driveはdry-run計画のみ、更新・配信経路は接続しない |
| 前向き凍結規則 | 既存SHA不一致でBLOCKED。規則・日付・HOLDOUTは変更していない。#5を自動適用せず、新規承認世代の設計が必要 |

NASの実運用承認を判断するには、[runbook.md](runbook.md) の実機受入と [transition.md](transition.md) の別承認が必要。
