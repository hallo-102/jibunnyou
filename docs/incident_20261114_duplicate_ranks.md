# 2026-11-14 順位重複による回顧停止の復旧

## 原因とPR #11の修正判断

NAS固定ソース `pre/source/app/1_keibayosou_pipeline.py` の239–240行は、totalをレース内標準化し小数2桁へ丸めたscoreにdense順位を付ける。福島10R `202603030310` の1・6番が6位、京都12R `202608050312` の13・16番が1位で重複していた。ShadowのTARGETには順位重複がない。

既存NASの `_build_bet_sheet()`（1417–1418行）は **score降順、馬番昇順**。risk・extra_penaltyはtotalに既に反映されているが、買い目の同点解消キーではない。信頼度も補助キーに使わない。mainの新しいrankingヘルパーにあるraw score→risk降順→penalty昇順→confidence降順→馬番という規則を、旧NASの事前資料に遡及適用しない。

PR #11のtotal降順は京都12Rの16番を1位に変えるため採用せず、旧NAS買い目の順序を回顧だけに適用する。予想再計算、重み・本番ランキング・買い目ルール変更は実施しない。

## 監査と評価

原rank、original_pred_rank、pred_rankを別列に保持する。順位補正レース全頭、選択理由、原順位/回顧順位の1位・TOP5集合、両評価をreview.jsonへ残す。結果反映Excelにも原本順位と回顧用順位を追記する。

京都12Rは原順位1位集合が[13,16]、回顧用は[13]。原TOP5は[4,7,9,13,14,16]の6頭、回顧用は[4,7,13,14,16]の5頭。福島10Rの1位・TOP5集合は変わらない。原順位評価は同率1位全頭購入の参考値であり、一頭選択の成績と同列に扱わない。

公式結果はJRA `app.jra.jp/JRADB/accessS.html` の当日全36レースを逐次取得。HTMLキャッシュ、SHA、開催日、全馬番・着順、全払戻キーと金額の照合を `official_verification/verification.json` に保存する。36レース516頭が一致。予想対象は双方30レース432頭で馬集合が完全一致し、対象外6レースも結果を保存する。

単複は予想1位への各100円の仮想購入。取消・除外は除き29頭で集計。ワイド・馬連・3連複は発走前Excelに記録された候補だけを100円単位で仮想精算する。実購入の証明はない。比較JSONには共通レース集合での券種別購入額・払戻・損益・ROIを保存する。購入ゼロの券種はROI未定義。

## 再開方法

`scripts/verify_official_20261114.py <prospective/20261114>` は取得済み公式HTMLから再照合可能。

NASでは `scripts/recover_20261114.py` を `/opt/jra-loop/repair_20261114/recover.py` に配置し、既存追加コンテナ内のPythonで実行する。固定原本検証→保存結果からpostprocess→既存マスタ更新→保護ファイルSHA比較→history冪等確認→Drive copy/check→status/history同期の順。失敗時はstatusのrecovery_errorに残す。発走前固定やShadow生成を呼び直さない。出力は既存の日付・runtime・結果SHAの同じパスを使い、historyは日付・runtime・roleで重複排除する。

修正対象は回顧と付随同期のみ。mainへ直接pushせず、既存PR #11のブランチに追加する。追加サービスだけを再起動して新コードを読み込み、既存朝処理と他コンテナを維持する。
