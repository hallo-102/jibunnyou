# NAS統合候補の設計

対象はIssue #16。GitHub main `178855fd117301131c8e3ebf70ce8c5e9e327728` から専用ブランチ `codex/issue16-nas-integration` を作成した。main・本番NAS・既存PRは変更/マージしない。

共通11モジュール、設定、重みのバイトはmainから変更しない。`nas.candidate` が同じ `run_pipeline()` を直接importし、入力・マスタ・オッズ・出力を明示する。既定のWindowsパスを使う通常runner.main()、GUI/COM、収集、DL学習、正式配信は起動しない。DL混合が非0になった場合は、時点別学習が未検証なので停止する。

候補の `nas/candidate.json` は20260817モデルを明示し、既存registryの実体SHA、実効SHA、モデル名、role、31グループ/3131要素、legacy版との一致を確認する。`production` は共通コード内部のrole名であり、NAS正式モデルを採用・変更した意味ではない。NASの20260730はそのまま保持する。

| 境界 | 実装 |
|---|---|
| 入力 | PR #10のmanifest検証を再利用。必要な入力/マスタ/オッズの全SHAが必須。指定日のコピーだけをnas.snapshotで固定 |
| 出力 | 新規runディレクトリを毎回確保。成功前READYなし。失敗時FAILED、非開催NO_RACE |
| 完成検査 | NAS取得元の対象外理由補完、全レース件数、必須列、未分類拒否に加え、一意順位と全馬集合を検証 |
| 順位 | 共通mainの丸め前scoreと既存同点解消順。各レース1位1頭、連番、TOP5=min(5,対象頭数)。馬キー重複/欠損/非有限scoreを拒否 |
| 頭数/取消 | 入力に残る取消馬も部分的に除去しない。予想するレースは入力全馬と一致する必要がある。履歴不足等はレース単位で対象外に分類。取消後の順位/買い目再生成は別承認 |
| 買い目 | PR #10の市場補完と既存pipelineの買い目関数を利用。候補と旧NASの買い目は別ファイル |
| 記録 | runtime.jsonにモデルSHA、コードSHA、実行commit、アダプターSHA、順位規則、買い目コードSHA、入力manifest SHA、環境、頭数、完成検査を保存 |
| 回顧 | PR #11のreviewを必要なファイルだけ取得。新予想の順位と、原本を保持した回顧用score/馬番補正を分離。旧NAS発走前原本を上書きしない |
| Drive | sync_plan.jsonによるdry-runだけ。送信先なし・実行なし。rclone/Gmail/本番READYにアクセスしない |
| マスタ | 読み取り専用snapshotのみ。本番マスタの更新処理を候補に含めない |
| 定期処理 | 本番scheduler/daily/回顧serviceは温存。候補composeはrestart=noの手動一回実行。新たな本番定期処理を登録しない |

ARM64/Python 3.11向けcomposeは別project名、読取専用root、snapshot:ro、候補専用output、networkなし、capabilitiesなし、非root UIDを使用する。実行時に認証情報やDocker socketをマウントしない。DockerはソースをホワイトリストでCOPYし、データ/秘密情報を含めない。イメージのビルドだけは依存取得のネットワークを使う。torchはPR #10が再利用する既存runnerのimport依存であり、DL学習を有効化するためではない。

`continuous_improvement` の取得範囲はarchive/runtime_metadata/shadow/reviewのみ。service、history自動集計、optimizer、bootstrap、正式マスタ更新、Drive配信は取り込まない。元PRの2重保守を避けるためファイルを元のpackage名に置き、取得SHAを記録した。将来 #10/#11 を統合する際は [reused_sources.json](reused_sources.json) の同一ファイルを突合し、重複パッチを適用しない。

## 前向き検証の扱い

mainのactive generationは `field_size_adjusted_finish_20260905`、開始/HOLDOUTはいずれも20260905。固定 `config/production_model.json` Git blob SHAの期待値は `f20d595c630be04a6306ae71cef1465f3f127c62`、実体は `2dee82c1a45fca1b253c2fc2a1713d8b4752c3a9` で、既存の厳格ローダーは停止する。候補はこのエラーを記録し、前向き処理を呼ばず `offline` / `retrospective`、`legacy_forward_generation: null` に限定する。これは前向き検証成功ではない。

PR #5は未マージで `production_20260817_20261024`、開始/HOLDOUT=20261024を提案している。今回その世代やSHAを自動適用しない。PR #4のoffline/前向き分離方針も維持する。新世代は実際の承認・コード固定後の未来の対象開催日から別途開始し、過去再計算やNAS旧実績を算入しない。既存の8開催日・300レース等の採用ゲートを緩和しない。

## 差分の原因を分離する方法

`nas.compare` は元rankを保持し、特徴量、丸め前score/total、表示score、順位、TOP5、元買い目行、券種を含むROI買い目行、除外理由を馬/レース別に保存する。特徴量/scoreの許容差は絶対・相対とも1e-9。rank/TOP5/買い目に数値許容差は適用しない。旧Excelに未保存の丸め前scoreは推測せず、旧pipelineの同じnormalize_scoreを丸め前で採取するdiagnostic sidecarを使う。sidecarと予想Excelの対応SHAも検証する。

| 比較 | データ差 | モデル差 | 実装差 | 解釈 |
|---|---|---|---|---|
| 旧保存出力 vs 候補 | 未確認 | 20260730/20260817 | 旧NAS/main | 歴史的参考のみ。因果を断定しない |
| 同一snapshotのlegacy_replay vs 候補 | 入力/マスタ/オッズのバイト一致 | 20260730/20260817 | 特徴量/順位等が異なる | データ差を除外。特徴量の変化は実装差。score/買い目変化はモデルと実装の複合 |
| Windows/main vs Linux候補 | 同一snapshot必須 | 同一20260817、実体/実効SHA一致必須 | 同一共通コード | OS/依存による差を確認。今回Linux実行は未完了 |
| モデル寄与の追加診断 | 同一snapshot・同じ特徴量/補正を固定 | 2モデルだけ変更 | 固定 | 重みの寄与を調べる後続の統制実験。今回因果分解は未完了 |

モデル差と実装差が同時に変わる比較から単一の原因を断定しない。全特徴量監査や20260730の共通コード上の等価実効重み検証は後続案件として引き継ぐ。
