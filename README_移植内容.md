# 競馬予想プログラム移植内容

## 移植の目的

`1_keibayosou_best_import_roi_runner.py` を起点に通常の競馬予想へ関係するコード、固定マスタ、実効best重み、およびコードが動的参照する予想履歴を、混在物から分離して新フォルダへコピーした。

- 元フォルダ: `C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026`
- 新フォルダ: `C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026-2`
- メイン実行ファイル: `C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026-2\1_keibayosou_best_import_roi_runner.py`
- 移植実施日: 2026-08-06
- 元フォルダのファイルは削除、移動、上書きしていない。
- 移植先は作業開始時に空であり、既存ファイルの上書きは発生していない。
- 元フォルダで未コミットだった `1_keibayosou_config.py` と `1_keibayosou_pipeline.py` は、作業時点の内容をそのままコピーした。

## 依存関係

直接依存は `1_keibayosou_config.py`、`1_keibayosou_loaders.py`、`1_keibayosou_pipeline.py`、`1_keibayosou_pace.py`、`1_keibayosou_utils.py`。

間接依存は `1_keibayosou_course_style.py`、`1_keibayosou_features.py`、`1_keibayosou_penalties.py`。これら9ファイルは `importlib` により、先頭が数字の実ファイル名から互換モジュール名へ登録される。

動的依存として、設定が `yosou_py/best_feature_weights_YYYYMMDD.py` の日付最大ファイルを選ぶため、実効ファイル `best_feature_weights_20260730.py` をコピーした。また、メインの後処理が `data/output/馬の競走成績_with_feat_*.xlsx` をglob検索し、実結果マスタと照合して推定馬券内率を計算するため、該当153ファイルをコピーした。

## 移植した主要ファイル

- A（移植必須）: メイン実行ファイルとローカル依存モジュール8ファイル
- A（移植必須）: `yosou_py/best_feature_weights_20260730.py`
- A（移植必須）: `data/master/race_levels.xlsx`
- A（移植必須）: `data/master/racedata_results.xlsx`
- A（移植必須）: `data/master/場所_馬場_タイム.xlsx`
- B（移植推奨）: `etc_py/1_02_scrape_jra_odds_2.py`（対象日オッズCSVの事前作成用）
- B（移植推奨）: `コード役割簡易説明.txt`
- D（入出力データだが実行時参照あり）: `data/output/馬の競走成績_with_feat_*.xlsx` 153ファイル
- A（追加移植必須）: `tokutyouryou_keisann/` のPythonパッケージ11ファイル
- A（追加移植必須）: 指定された収集・変換・結果反映スクリプト4ファイル
- A（追加移植必須）: `data/input/*馬の競走成績_*.xlsx` 127ファイル（特徴量計算がglob参照）
- A（追加移植必須）: `data/master/grade_race_master.csv`
- A（追加移植必須）: `data/output/weight_adoption/decision_*.json` 5ファイル
- A（追加移植必須・機密設定）: `config/credentials.ini`（内容は管理資料へ記載しない）

全ファイル名は `移植ファイル一覧.txt` に記録した。

## フォルダ構成

```text
keiba_yosou_2026-2/
├─ 1_keibayosou_best_import_roi_runner.py
├─ 1_keibayosou_config.py
├─ 1_keibayosou_course_style.py
├─ 1_keibayosou_features.py
├─ 1_keibayosou_loaders.py
├─ 1_keibayosou_pace.py
├─ 1_keibayosou_penalties.py
├─ 1_keibayosou_pipeline.py
├─ 1_keibayosou_utils.py
├─ コード役割簡易説明.txt
├─ config/
│  └─ credentials.ini
├─ data/
│  ├─ input/  （特徴量計算が参照する過去入力127ファイル）
│  ├─ master/
│  │  ├─ race_levels.xlsx
│  │  ├─ racedata_results.xlsx
│  │  ├─ grade_race_master.csv
│  │  └─ 場所_馬場_タイム.xlsx
│  ├─ ozzu_csv/
│  └─ output/  （予想履歴153ファイル、重み採用履歴5ファイル）
├─ etc_py/
│  ├─ 00_Export_To_Excel_4.py
│  ├─ 01_jizen_syuusyuu_race_info_classfix_20251212.py
│  ├─ 1_02_scrape_jra_odds_2.py
│  ├─ 10_kekka_scraper_20260102.py
│  └─ 11_2_kekka_hanei_payout_260322 copy.py
├─ tokutyouryou_keisann/  （実行入口を含むパッケージ11ファイル）
└─ yosou_py/
   └─ best_feature_weights_20260730.py
```

## パス修正内容

`etc_py/1_02_scrape_jra_odds_2.py` のオッズCSV既定保存先だけを修正した。

- 修正前: 元フォルダの `data/ozzu_csv` を直接指定する絶対パス
- 修正後: `Path(__file__).resolve().parents[1] / "data" / "ozzu_csv"`
- 理由: 新フォルダ配下の保存先を、補助スクリプト自身の位置から解決するため
- 予想ロジック、特徴量、重み、順位、評価条件、ペナルティ、危険馬判定は変更していない。

主要9ファイルは既に `CODE_DIR = Path(__file__).resolve().parent` を基準にしているため、フォルダ名変更に伴うコード修正は不要だった。

追加移植では次の3ファイルの旧フォルダ固定パスを修正した。

- `etc_py/00_Export_To_Excel_4.py`: 入出力マスタフォルダをスクリプト位置基準の `data/master` へ変更
- `etc_py/01_jizen_syuusyuu_race_info_classfix_20251212.py`: `credentials.ini` と入力Excel出力先をスクリプト位置基準へ変更
- `etc_py/10_kekka_scraper_20260102.py`: 結果マスタ保存先と `credentials.ini` をスクリプト位置基準へ変更

`etc_py/11_2_kekka_hanei_payout_260322 copy.py` と `tokutyouryou_keisann/` は元からスクリプト位置基準または環境変数基準のため、パス修正は不要だった。

## 意図的に残した外部参照

`1_keibayosou_config.py` の `DATA_ROOT` は `C:\Users\okino\OneDrive\ドキュメント\my_python_cursor` を指す。ここから共有の `csv`、`exe`、`ini`、`json` を定義しているため、プログラム本体の旧フォルダ参照ではなく外部共有ルートとしてそのまま残した。今回調査したメイン実行経路では、これら4定義は直接importされていない。将来それらを使う別処理も新フォルダへ完全分離する場合は、別途方針決定が必要。

## コピーしなかったデータ・ファイル

- 過去の `data/input/馬の競走成績_YYYYMMDD.xlsx`: 初回移植では対象外だったが、追加指定された特徴量計算がglob参照するため、該当127ファイルを追加コピーした
- 過去の `data/ozzu_csv/OZZU_YYYYMMDD.csv`: 対象日ごとの入力データであり、補助スクレイパーで都度作成するため
- `data/input/success_report*.xlsx`: 累積・過去出力であり、予想入力ではないため
- 旧 `best_feature_weights_*.py`: 動的選択では日付最大の20260730だけが実効ファイルになるため
- race levelのbackup、tmp、testファイル: 固定マスタ `race_levels.xlsx` が存在するため
- `.env`: 追加スクリプトからも参照されないためコピーしていない。`config/credentials.ini` はスクレイパー2本の必須設定として、内容を表示せず追加コピーした
- `.git`、`.venv`、キャッシュ、ログ、バックアップ、テスト、検証結果、Webアプリ関連、学習・最適化用コード: 通常予想の依存ではないため
- `data/output` 内の比較・検証用CSV/JSON/Excel/Markdown: メインのglob条件に一致しないため

すべて元フォルダには残している。

## 判断保留

重大な判断保留ファイルはない。外部共有ルートの `csv`、`exe`、`ini`、`json` は通常予想の直接依存ではないためコピーせず、意図的な外部参照として記録した。

## 今後の動作確認時の注意事項

- 対象日の `data/input/馬の競走成績_YYYYMMDD.xlsx` を配置する。
- 対象日の `data/ozzu_csv/OZZU_YYYYMMDD.csv` を配置する。補助スクレイパーを使う場合はPlaywright本体とブラウザの導入が必要。
- Python環境には少なくともNumPy、pandas、PyTorch、openpyxl、requestsが必要。
- 実行時の `KEIBA_SCORING_MODEL_VERSION` 環境変数によりlegacy/five_blockが切り替わるため、旧環境と同じ値で確認する。
- 初回確認では出力先 `data/output` と `data/input/success_report.xlsx` の更新に注意する。
- 特徴量計算は `data/input/*馬の競走成績_*.xlsx`、`data/master/racedata_results.xlsx`、`race_levels.xlsx`、最新best重み、重み採用履歴を参照する。
- 事前収集・結果収集スクリプトは認証設定を使用するため、`config/credentials.ini` のアクセス権と管理に注意する。
- 事前収集にはChrome/Selenium/WebDriver Manager、結果収集にはEdge/Selenium、HTML解析にはBeautifulSoupなどが必要。

## 未実施事項

今回は依頼どおり、次を実施していない。

- 新フォルダでのPythonプログラム実行
- 競馬予想処理の動作確認
- Excel出力確認
- 旧版と新版の予想結果比較
- 予想精度の評価
