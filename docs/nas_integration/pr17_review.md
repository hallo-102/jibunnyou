# PR #17 一次レビューへの対応

対象コメント: https://github.com/hallo-102/jibunnyou/pull/17#issuecomment-7155072023

同じ `codex/issue16-nas-integration` ブランチを継続使用する。main・本番環境・正式モデル・既存データ・READY/定期処理を変更せず、前向きSHA BLOCKEDとoffline限定を維持する。新規PRや既存PRのマージは行わない。

| 指摘 | 修正と確認 |
|---|---|
| S1 出力境界 | nas.workspaceで新規初期化したルートの4カテゴリに限定。コード/指定本番root/既知NAS/固定入力との包含、symlink/junction、既存比較JSONを拒否。nas.launchでホスト実パスを照合しDockerより先に停止。比較JSONは全体を直列化してからO_EXCLで新規作成し、固定名.tmpやhardlinkを使用しない |
| S1 旧NAS再生 | sourceの10予想モジュールと重みだけを検証コピー。子プロセスcwd・KEIBA出力/重みはrun内、入力/マスタ/オッズはsnapshotへ固定。worker直接呼出しにも境界・コピーSHA検査を適用。収集/配信/マスタ更新を起動しない |
| S2 公開順序 | prediction.xlsxのコピー自体をSHA検証、公開manifestをverify_archiveしてからCOMPLETED→READYの順で公開。verify_completedは両状態とmanifest/Excel SHAを全て要求。失敗時はREADYを除去。終盤STATUS書込故障のfixtureで残留しないことを確認 |
| S3 分類/順位 | TARGETレース集合と買い目の数値予想済み集合、全入力レースと分類集合を突合。対象外の順位・馬番・購入額・ROI固定券を拒否。日本語出力の同点キーを共通名へ戻しcreate_unique_rank_seriesで保存順位を再計算照合。欠落/逆順/対象外券をfixture検証 |
| A1 出所 | legacy source_manifest/trace/runtimeのSHA、candidate manifest/runtime/モデル実体・実効SHAを機械照合。input_manifest SHAの両側一致からdata判定を作る。説明JSONは未検証メモのみ。記録欠損はUNVERIFIED/部分確認、改変は停止 |
| A1 統制比較 | weight_controlは本番registryを変更せず、共通pipelineの同じスコア入力DataFrame/場所馬場map/補正関数で2重みだけを変更。既存configのmerge/符号補正を利用し、両実体/実効SHA、補正値完全一致、明示20260817と通常候補の完全一致を要求 |
| A2 接続/受入 | runbookに#10 pre→候補、READY分離、#11回顧、history/前向き、配信、マスタ/定期処理の受渡し契約と後続受入を明記。固定calendar.txtをCLI省略時も自動使用し、非開催は予想しない |

## 最小回帰

修正前にレビューの5つの症状（比較JSON上書き、分類不一致、逆順、カレンダー無視、終盤失敗READY残留）を5件の失敗として再現した。修正後は旧23件を含め39件成功。追加は境界、公開順、分類/順位、出所改変、固定カレンダーの契約に限定した。共通コード・モデル設定・重みは変更していない。

```sh
# 旧契約と今回の失敗再現を同じ最小セットで確認する。
python -m pytest tests/test_nas_review.py tests/test_nas_integration.py tests/test_unique_ranking.py -q
git diff --check  # 空白の不正がないことを確認する。
```

代表日の生の比較・入力・Excel・ログ・weight_controlの馬別記録は候補専用workspaceに保存し、GitHubへ登録しない。最終通し検証の集計は [review_summary.json](review_summary.json) に記録した。

検証実装commitは `988e1fdc534e145e5757d903241c036e70e70904`。20260809の36レース495頭を新規workspaceで固定し、候補27レース376頭・対象外9を確認した。TARGET/分類/一意順位再計算、公開コピー、COMPLETED/READY、全manifest SHA、実行時core/adapter SHAが一致した。ローカルNAS静的ソース・Windows比較対象・元入力/オッズ/2マスタ/旧予想のSHAも不変。

同一snapshotの旧NAS再生と候補は、両側ともVERIFIED_SAVED_RECORDS、data=SAME、model/implementation=DIFFERENT。共通特徴量/補正上の重みだけの統制比較は27レース376頭で補正値が完全一致し、20260817結果は通常候補と完全一致した。重み差だけでは順位225頭・1位7レース・TOP5集合16レースが変化した。旧NAS対候補の順位233頭・1位8・TOP5 17という複合差と直接引き算して寄与率を出さず、相互作用を含む全分解やROI改善を認定しない。

ホスト検査を経たcompose configは終了0。比較JSONは実際のOneDrive上で新規保存でき、同じ結果名での再実行を拒否して既存結果を保持した。クラウドCIは修正コミットのPush前のため未実行。実機に関する未検証事項は以下の通り維持する。

OneDrive上でhardlink作成がWinError 5になったため、比較結果の保存はO_EXCLの新規ファイル作成へ変更した。書込途中のI/O故障は失敗として例外を返す。新規の不完全ファイルが残った場合も上書き再試行せず、別の新規結果名を使用する。比較JSONをREADYと同じ原子的成功マーカーとして扱わない。

## 残る限界

- 実NAS/ARM64ビルド・稼働中image/mount/本番READY/STATUS/定期処理不変、Windows/Linuxの通し一致は未検証。
- 共通特徴量上の重み寄与は切り分けるが、旧NAS特徴量×重みの相互作用を含む複合差全体の完全分解、統制条件での買い目再生成、ROI/精度改善認定は行わない。
- #10/#11の稼働サービス、実開催の発走前〜夕方回顧、history、実Drive/Gmail、正式マスタ更新は候補へ接続しない。接続・新前向き世代・本番切替は別レビュー/承認が必要。
- 旧コードのコピー実行は既知ファイルの書込経路を限定する。任意のPythonコードを封じ込めるホストOSサンドボックスの代替ではない。
