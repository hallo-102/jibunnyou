# Keiba AI Studio 障害対応・FAQ

## 初動

1. 画面のエラー文、対象日、race ID、job ID、request IDを記録する。
2. `docker compose ps`と`Invoke-RestMethod http://127.0.0.1:18080/api/ready`を確認する。
3. `docker compose logs --since 30m api worker frontend gateway`を保存する。
4. 予想や買い目を繰り返し実行せず、原因を一つずつ切り分ける。
5. DB削除、volume削除、migration stampは行わない。

## 症状別

### 画面が開かない

- Docker Desktopが起動しているか確認する。
- `gateway`と`frontend`がUpか確認する。
- port 18080が競合している場合は`.env`の`KEIBA_APP_PORT`を変更し、`docker compose up -d gateway`を実行する。

### readinessが失敗する

- `postgres`: `docker compose logs postgres api`を確認する。
- `redis`: `docker compose logs redis worker`を確認する。
- `artifact_storage`: OneDrive同期、空き容量、`data`配下の書込権限を確認する。
- `config`: `.env`の必須値とJSON形式の配列を確認する。

### `password authentication failed`または`role does not exist`になる

- PostgreSQLの`POSTGRES_USER`、`POSTGRES_PASSWORD`、`POSTGRES_DB`は、空volumeの初回初期化時だけDB内部へ作成される。
- 既存volumeがある状態で`.env`だけを変更しても、DB内部のrole名・password・database名は自動変更されない。
- `docker compose down -v`で解決しようとすると業務データを失うため実行しない。
- volume backupを取得し、既存role/databaseを確認してから、DB内部と`.env`を同じ値へ移行する。

### Python予想が失敗する

- 開催日とrace IDが選択されているか確認する。
- 入力Excel、オッズ、master、品質状態を確認する。
- jobの標準エラーとArtifactを確認する。
- Pythonロジック変更後ならGolden Masterと3か月backtestへ戻る。

### 独立AIが失敗する

- `OPENAI_API_KEY`未設定は想定された安全停止で、Python予想は継続できる。
- キーを`.env`へ設定後、`docker compose up -d --force-recreate api worker`を実行する。
- JSON不正、timeout、存在しない馬番は最大回数内で失敗となる。入力を変えず無制限再試行しない。
- 独立入力にPython順位が混入した疑いがある場合は結果を採用しない。

### 買い目候補が停止される

- 品質RED、上限超過、取消、重大不一致、データ不足を確認する。
- AI案の`review_required`は不具合ではなく、明示確認を要求する安全状態である。
- アプリは外部購入を実行しない。`purchased`は外部購入後の手動記録である。

### 結果精算ができない

- `confirmed`結果だけが通常精算対象である。
- 暫定、取消、返還、同着、失格と構造化払戻を確認する。
- 100円基準払戻、券種、組合せが一致しないデータを手修正で通さない。

### DB migrationが失敗する

- 現在revisionとheadを確認する。
- 非空の未管理DBへ勝手にstampしない。
- 変更前backupを隔離DBへ復元し、再現してから復旧手順を決める。

## FAQ

### APIキーなしで使えますか

Python予想、データ確認、既存履歴は使えます。独立AIと、その新規結果を必要とする比較統合だけが安全に失敗します。

### 自動で馬券を購入しますか

購入しません。DB制約とservice実装で自動購入を無効にし、外部投票clientを持ちません。

### `.env`を共有してよいですか

共有しないでください。`.env.example`だけを共有し、秘密値は各PCで設定します。

### OneDrive配下で使えますか

利用できます。ただし大量Artifact同期中は遅延やlockが起きるため、空き容量と同期状態を確認してください。DB本体はDocker volumeです。

### データを初期化したいです

通常運用には初期化手順を提供しません。必要ならbackupを取り、削除対象と復元方法を明示して別作業として実施します。
