# NAS A〜D比較のフィクスチャ（Issue #35）

[PR #34 の比較プログラム仕様](https://github.com/hallo-102/jibunnyou/blob/ee79e825cd1669aa3428ada70a28cbafb43d9f9f/tools/compare_ab_models.md)に沿って、2026-08-09（JST）の発走前保存ファイルと確定結果をまとめます。保存日時・発走時刻・確定時刻は保存履歴を使って記入します。ファイルの更新日時や最新のマスターからの推定は避けてください。

## 準備

1. 保存済み Excel と任意のマスター・オッズファイルを索引と同じディレクトリの `archive/` 以下に置きます。Excel は比較プログラムのシート形式に合わせます。発走前人気、保存済み DL 出力、マスター類の取得時刻などは比較プログラムと同じキーで記入できます。
2. `index.json` の `snapshots` にレースごとのメタデータを記入します。ファイル指定は索引からの相対パスです。既知の `*_sha256` を記入した場合は保存時のハッシュと照合します。
3. `capture_evidence.json` に、保存ログや保存済みデータ内の記録から確認した取得時刻をレース・入力ごとに記入します。Excel・マスター・オッズには SHA-256、発走前人気・DL には保存済みの値を記録します。DL の証跡にはモデル SHA と学習終了時刻も必要です。比較するレースの Excel は証跡必須です。任意入力を裏付けられない場合は対応するキーを省略します。
4. 確定結果には、既存の学習ツールと同じ結果・払戻シートを持つ Excel、または PR #34 の `results.json` 形式の確定結果を指定します。Excel の着順シートには馬番が必要です。対象レースと確定時刻を `finalized_at` に記入します。同着や返還で単一の上位3頭と払戻に変換できないレースは、`exclude_results` に理由を記入します。比較プログラムではこれらを着順・払戻の欠落として扱います。券種ごとの払戻は正常に確定した全組番を収録してください。

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

`capture_evidence.json` の例です。SHA-256 は原本の実値を指定します。`source` は運用担当者が照合した保存記録の参照です。既存収集処理が書き込んだ `README!B1` を使う場合は `source` を明示し、保存環境のタイムゾーンを指定します。ツールはコピーした原本のセル値を照合します。

```json
{
  "202604020601": {
    "workbook": {
      "source": "workbook:README!B1",
      "timezone": "Asia/Tokyo",
      "observed_at": "2026-08-09T10:05:00+09:00",
      "sha256": "<原本のSHA-256>"
    },
    "race_levels": {
      "source": "保存記録ID/マスターの取得イベント",
      "observed_at": "2026-08-09T09:00:00+09:00",
      "sha256": "<原本のSHA-256>"
    },
    "popularity": {
      "source": "保存記録ID/人気の取得イベント",
      "observed_at": "2026-08-09T10:05:00+09:00",
      "values": {"1": 2, "2": 1, "3": 3}
    }
  }
}
```

```sh
python -m tools.prepare_ab_comparison \
  --index /path/to/index.json --settled /path/to/settled.xlsx \
  --evidence /path/to/capture_evidence.json --output /path/to/new-ab-fixture --raceday 2026-08-09
```

新規ディレクトリに `snapshots.json`、`results.json`、`run.json`、`manifest.json` と元ファイルのコピーを生成します。マニフェスト v2 の `original_files_sha256` は索引、取得証跡、元の確定結果と参照ファイルのハッシュです。`generated_files_sha256` は整形後の3ファイルのハッシュです。標準出力に表示するマニフェストの SHA-256 を NAS ポリシーに登録してください。

## 1日分の設定

`run.json` は `mode: single_day_ab`、`methods: ["A", "B"]`、`periods.validation: ["2026-08-09", "2026-08-09"]` を指定します。A は従来の特徴量計算、B は修正後の特徴量計算で、両方が固定コミットの同じ本番基準重みを使用します。確定結果は両方式の評価に使用します。C・D の最適化と候補選定は全期間工程で扱います。
