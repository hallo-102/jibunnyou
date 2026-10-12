# 条件付き再ランキングの本番統合レビュー

本PRは元rank・買い目・本番モデルを変更せず、別の条件付き比較順位を追加します。
本番フォルダへの反映・mainマージは未実施です。

## 再生検証と前向き運用

- 現active_generationは`cr_offline_20261013`、purposeは`offline_integration_replay`です。
- 過去日20261012の再生には、隔離ハーネス内で`KEIBA_FORWARD_SCOPE=offline_replay`を明示します。
- 指定のない通常本番プロセスではoffline世代を拒否します。SHA監査は維持します。
- 保存先は`data/output/shadow_comparison/offline/cr_offline_20261013/`です。
- JSONにはgenerationとvalidation_scopeを記録し、前向き集計ツールはoffline世代を拒否します。
- 旧2世代とその登録SHA・閾値は変更しません。再生結果は前向き開催日数・レース数・ROIへ加算しません。

## 承認が必要な本番用世代の提案

| 項目 | 提案 | 承認条件 |
| --- | --- | --- |
| 世代ID | `cr_fwd_<承認開始日YYYYMMDD>` | offline世代と別のIDを採用。Windows保存パス長も検査 |
| 開始日 | 承認後、結果確定前の入力・オッズを取得できる最初の対象開催日 | 具体的なYYYYMMDDを承認。過去日へ遡及しない |
| purpose | `production_forward` | offlineの名称・結果を流用しない |
| status | 承認前は`pending_approval`、承認後のみ`active` | approval flagを承認後にFalseへ切替 |
| 保存先 | `data/output/shadow_comparison/forward/<世代ID>/` | 新規専用フォルダを承認 |
| 集計 | generationとvalidation_scopeが一致するpost JSONだけ | 旧世代・offline・欠損タグの記録を拒否 |
| HOLDOUT | 新しい開始日と同じ日付 | 旧世代の保留データ・統計は再利用しない |
| 指標・閾値 | 8開催日かつ300レース、利益面50レース等の既存ゲートを維持 | 期限日を別途承認。旧20261031を無条件に流用しない |
| 固定監査 | レビュー・テスト済みファイルSHAを世代ごとに登録 | 任意の実体へ自動更新しない |

本番用世代はまだ有効化していません。この決定と本番フォルダへの適用は、PRへのpushとは別の承認事項です。
現在の本番/shadowは同じ20260817モデルです。整合性比較を精度向上・収益改善の証拠として扱いません。

## 検証と公開対象

前回の83件回帰とshadow有効runner完走の証拠はローカルreportsに保存済みです。
今回のscope分離を含む再検証結果はPR本文と最終報告書に記録します。
CIでは保存済み実データに依存する7件を除外し、その他の回帰を人工fixtureで実行します。
実Excel・CSV・DB・重みの変更・隔離実行データ・秘密情報は本PRへ追加しません。
