# NAS A〜D比較のフィクスチャ（Issue #35）

[PR #34 の比較プログラム仕様](https://github.com/hallo-102/jibunnyou/blob/ee79e825cd1669aa3428ada70a28cbafb43d9f9f/tools/compare_ab_models.md)に沿って、2026-08-09（JST）の発走前保存ファイルと確定結果をまとめます。保存日時・発走時刻・確定時刻は保存履歴を使って記入します。ファイルの更新日時や最新のマスターからの推定は避けてください。

## 準備

1. 保存済み Excel と任意のマスター・オッズファイルを索引と同じディレクトリの `archive/` 以下に置きます。Excel は比較プログラムのシート形式に合わせます。発走前人気、保存済み DL 出力、マスター類の取得時刻などは比較プログラムと同じキーで記入できます。
2. `index.json` の `snapshots` にレースごとのメタデータを記入します。ファイル指定は索引からの相対パスです。既知の `*_sha256` を記入した場合は保存時のハッシュと照合します。
3. 確定結果には、既存の学習ツールと同じ結果・払戻シートを持つ Excel、または PR #34 の `results.json` 形式の確定結果を指定します。Excel の着順シートには馬番が必要です。対象レースと確定時刻を `finalized_at` に記入します。同着や返還で単一の上位3頭と払戻に変換できないレースは、`exclude_results` に理由を記入します。比較プログラムではこれらを着順・払戻の欠落として扱います。券種ごとの払戻は正常に確定した全組番を収録してください。

```json
{
  "snapshots": [
    {
      "race_id": "202604020601",
      "start_at": "2026-08-09T10:10:00+09:00",
      "observed_at": "2026-08-09T10:05:00+09:00",
      "workbook": "archive/day.xlsx",
      "race_levels": "archive/race_levels.xlsx",
      "race_levels_observed_at": "2026-08-09T09:00:00+09:00",
      "popularity": {"1": 2, "2": 1, "3": 3}
    }
  ],
  "finalized_at": {"202604020601": "2026-08-09T10:25:00+09:00"},
  "exclude_results": {}
}
```

```sh
python -m tools.prepare_ab_comparison \
  --index /path/to/index.json --settled /path/to/settled.xlsx \
  --output /path/to/new-ab-fixture --raceday 2026-08-09
```

新規ディレクトリに `snapshots.json`、`results.json`、`run.json`、`manifest.json` と元ファイルのコピーを生成します。マニフェストの `files_sha256` は生成した3ファイルと、索引、元の確定結果、参照した全 Excel・マスター・オッズの SHA-256 を記録します。標準出力に表示するマニフェストの SHA-256 を NAS ポリシーの `manifest_sha256` に登録してください。

## 1日分の設定

PR #34 の固定版は各期間の重複を禁止し、C/D の最適化に学習日中に確定したレースを必要とします。そのため、学習に8月9日、検証に8月10日、前向きに8月11日を割り当てます。試行回数は各2回です。8月9日のデータで特徴量再計算、C/D の学習、A〜D 比較表・除外理由・マニフェストの生成まで確認できます。比較表の検証・前向き母数は0です。評価指標の実測と候補選定には、別日のデータを含む時系列比較が必要です。
