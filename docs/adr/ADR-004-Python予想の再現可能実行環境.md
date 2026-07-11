# ADR-004 Python予想の再現可能実行環境

- 状態: 採用
- 日付: 2026-07-10

## 背景

既存の2段階予想はExcel、NumPy、PyTorch、外部重み、過去予想出力を参照する。ホスト直接実行では実行時点のファイル集合と依存関係が固定されず、WindowsとLinuxのPyTorch CPU実装差でDL学習結果に小さな数値差も生じた。

## 検討した選択肢

1. Windowsホスト直接実行を正とする。
2. Docker/Linux CPU環境を正とし、runごとに入力と実行資産を固定する。
3. 既存予想を別モデルへ全面置換する。

## 採用した方法

- Docker/Linux、Python 3.11、PyTorch `2.11.0+cpu`を正式な予想実行環境とする。
- run単位の`legacy_workspace`へコード、入力Excel、OZZU、master、重みをSHA-256検査付きで複写する。
- 過去予想と重みは対象日より前だけを使い、未来情報を除外する。
- CPU thread数と`PYTHONHASHSEED`を固定する。
- Excelバイト一致ではなく、馬同一性、順位、score、DL、risk、推定馬券内率、期待値の行単位Golden Masterで判定する。

## 採用理由

- Docker内の独立2回実行で318頭・重要11項目の不一致0件を実測できた。
- 実行中間ファイルが既存`data/output`を上書きしない。
- Web、Worker、運用手順の実行環境を一つに固定できる。

## 利点

- 同一スナップショットの再現と監査が可能。
- 外部重み、master、コード、依存版を後から追跡できる。
- GPU/CUDA依存を持たず、Windows Docker Desktopで実行できる。

## 欠点

- Windowsホストの旧実行結果とDocker/LinuxのDL数値は完全一致しない。
- PyTorch CPU wheelによりバックエンドimageが大きくなる。
- 過去OZZUに取得時刻のない重複snapshotがある日は、安全に再予想できない。

## 影響範囲

- `backend/Dockerfile`、`docker-compose.yml`、`backend/requirements.txt`
- `prediction_workspace.py`、`prediction_runner.py`、`prediction_cli.py`
- 予想runのConfigVersion、FeatureWeightVersion、Artifact、Golden Masterテスト

## 移行方法と後方互換性

- 旧CLIとExcelシートは変更せず、Adapterがそのまま実行する。
- 旧予想Excelは履歴として保持する。新規予想は各runの環境・入力hashと共に識別する。
- Windows直接実行は調査用とし、採否判定はDocker/Linux内の同一版で比較する。
