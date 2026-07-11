# Phase 2 Loop 1 Alembic検証結果

- 実施日: 2026-07-10
- 対象: Alembic baseline、起動時migration、legacy schema保護、主要一意制約
- Alembic: 1.18.5
- PostgreSQL: 16

## 実装結果

- 現行20テーブルをrevision `0001_baseline`として明示migration化した。
- API起動時の`Base.metadata.create_all`を廃止し、`upgrade head`へ置換した。
- 非空かつ未version、またはversion行が空の部分DBは自動upgradeを拒否する。
- 旧`create_all` schemaは現行metadataとの差分0件の場合だけ、明示`stamp-legacy`を許可する。
- `/api/version`へ`database_revision`を追加した。
- CLIで`current`、`upgrade`、`downgrade`、`stamp-legacy`を提供した。

## 自動・受入テスト

| テスト | 結果 | 補足 |
|---|---|---|
| Backend compileall | 成功 | app/alembic/tests |
| Host pytest | 成功 | 16 passed |
| Container pytest | 成功 | 16 passed |
| SQLite空DB upgrade | 成功 | 20テーブル、revision一致 |
| SQLite downgrade/re-upgrade | 成功 | 20→0→20 |
| Legacy schema自動upgrade拒否 | 成功 | 保存済み行を維持 |
| Legacy schema明示stamp | 成功 | schema差分0の場合だけ許可 |
| 部分version DB拒否 | 成功 | version table空を安全側で拒否 |
| PostgreSQL空DB起動migration | 成功 | API起動時に20テーブル作成 |
| PostgreSQL schema差分 | 成功 | 0件 |
| PostgreSQL downgrade/re-upgrade | 成功 | 20→0→20 |
| PostgreSQL revision | 成功 | 前後とも`0001_baseline` |
| PostgreSQL一意制約 | 成功 | race/horse、prediction run/race/horse、AI run/race/horse |
| readiness/version | 成功 | ready、DB revision表示 |
| 検証環境の後片付け | 成功 | container/network/volume削除 |

## 安全上の判断

- 既存DBを推測でstampしない。
- schema差分が1件でもあればstampを拒否し、手動調査へ回す。
- downgradeは明示revision必須とし、通常起動では実行しない。
- 検証は専用Compose projectと使い捨てvolumeで行い、実データへ接続していない。

## 判定

- 自己評価: 94/100
- 判定: Loop 1完了、Phase 2継続
- 次工程: 中核履歴テーブル、外部キー・check制約、時刻/確率/金額のDB契約を追加migrationで実装する。

