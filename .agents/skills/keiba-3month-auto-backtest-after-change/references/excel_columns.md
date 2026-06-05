# Excel列候補

## ファイル名候補

| 用途 | 既定候補 |
| --- | --- |
| 入力Excel | `data/input/馬の競走成績_{date}.xlsx` |
| 入力Excel補助 | `data/input/*{date}*.xlsx` |
| 予想出力Excel | `data/output/馬の競走成績_with_feat_{date}.xlsx` |
| 予想出力Excel補助 | `data/output/*with_feat*{date}*.xlsx`, `data/output/*{date}*.xlsx` |
| 実績Excel | `data/master/racedata_results.xlsx` |
| レースレベルExcel | `data/master/race_levels.xlsx` |

`{date}` は `YYYYMMDD`。固定名で見つからない場合は、設定JSONで `input_patterns`、`output_patterns`、`result_master_path` を指定する。`race_levels.xlsx` はレースレベル判断用で、結果照合には `racedata_results.xlsx` を使う。

## レース識別列

優先順位はレースID、次に日付、場名、レース番号の複合キー。

| 目的 | 候補列 |
| --- | --- |
| レースID | `race_id`, `レースID`, `レースid`, `レースキー`, `race_key` |
| 日付 | `date`, `日付`, `開催日`, `年月日` |
| 場名 | `場所`, `場名`, `競馬場`, `開催場`, `track` |
| レース番号 | `R`, `レース`, `レース番号`, `race_no`, `race_number` |

## 馬識別列

馬IDがあれば馬IDを優先し、なければ馬名、馬番の順で照合する。

| 目的 | 候補列 |
| --- | --- |
| 馬ID | `horse_id`, `馬ID`, `馬id`, `競走馬ID` |
| 馬名 | `horse_name`, `馬名`, `競走馬名` |
| 馬番 | `horse_number`, `馬番`, `番` |

## 予想ランキング列

| 目的 | 候補列 |
| --- | --- |
| 順位 | `予想順位`, `ランキング`, `rank`, `Rank`, `score_rank`, `印順位` |
| スコア | `予想スコア`, `score`, `Score`, `合計スコア`, `期待値`, `評価点`, `prediction_score` |

順位列がある場合は昇順、スコア列だけの場合は降順でランキングを作る。

## 結果列

| 目的 | 候補列 |
| --- | --- |
| 着順 | `着順`, `確定着順`, `finish`, `finish_order`, `着` |
| 1着馬 | `1着馬`, `1着馬名`, `一着馬`, `一着馬名` |
| 2着馬 | `2着馬`, `2着馬名`, `二着馬`, `二着馬名` |
| 3着馬 | `3着馬`, `3着馬名`, `三着馬`, `三着馬名` |
| 人気 | `人気`, `単勝人気`, `popularity`, `ninki` |
| 三連複払戻 | `三連複払戻`, `三連複配当`, `3連複払戻`, `3連複配当`, `trio_payout` |

`racedata_results.xlsx` の日付シートでは、主に `レースID`, `着 順`, `馬 番`, `馬名`, `人 気`, `払戻種別`, `組番`, `払戻金` を使う。

結果Excelがレース単位で1から3着馬列を持つ場合は、その3頭だけでTOP5完全捕捉率と3連複想定的中率を計算する。各馬の着順行がある場合は、ランキング上位馬ごとの馬券内率と欠損馬数も精度高く計算する。
