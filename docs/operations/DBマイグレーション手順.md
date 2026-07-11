# DBマイグレーション手順

## 原則

- 本番DB操作前に`pg_dump`を取得し、ファイルサイズと終了コードを確認する。
- `upgrade`は前進専用とし、schema不一致を自動修復しない。
- `downgrade`は検証環境またはバックアップ復元を伴う承認済みロールバックだけで使う。
- `.env.example`のパスワードを本番へ使用しない。

## 空DBまたはversion管理済みDB

```powershell
# 現在revisionを確認する。
docker compose run --rm api python -m app.db.migrate current

# 最新revisionまで適用する。
docker compose run --rm api python -m app.db.migrate upgrade head
```

API通常起動時にも`upgrade head`が実行される。非空の未version DBは安全のため起動を拒否する。

## 旧create_all DBの初回移行

1. API、Worker、Beatを停止する。
2. `pg_dump`を取得する。
3. 復元用テストDBでバックアップを復元する。
4. 次の明示stampをテストDBで実行する。

```powershell
# 現行metadataとの差分が0件の場合だけbaseline stampが成功する。
docker compose run --rm api python -m app.db.migrate stamp-legacy

# stamp後に最新revisionへ進める。
docker compose run --rm api python -m app.db.migrate upgrade head
```

5. APIテスト、件数照合、代表レース照合を行う。
6. 同じ手順を本番へ適用する。

`stamp-legacy`が拒否された場合は続行しない。schema差分とバックアップを確認し、個別migrationを作成する。

## 検証環境のdowngrade

```powershell
# baselineより前へ戻す例。全業務テーブルが削除されるため本番では通常実行しない。
docker compose run --rm api python -m app.db.migrate downgrade base

# 再度最新へ進める。
docker compose run --rm api python -m app.db.migrate upgrade head
```

