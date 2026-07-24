# Keiba AI Studio

Keiba AI Studioは、既存Python予想、ChatGPT用予想プロンプトの生成・コピー、手動で取り込んだ回答履歴、買い目候補、結果照合、成績分析を一つの画面で扱うローカル競馬予想アプリです。ChatGPTにはWebで最新情報を調査し、Pythonを見ない独立評価の後にPython予想へ賛否・順位修正を行うよう依頼します。ChatGPTの自動操作と外部への自動投票は実装していません。

## 最短起動手順（Windows PowerShell）

前提はWindows 11、Docker Desktop、Docker Compose v2です。プロジェクト直下で実行します。

```powershell
# 環境変数サンプルをローカル設定へコピーする。
Copy-Item .env.example .env

# 初回構築時だけ、.envのKEIBA_POSTGRES_PASSWORDとKEIBA_DATABASE_URL内の同じパスワードを変更する。
# 既存のpostgres_data volumeがある場合は、DB内部のrole変更なしに値だけ変えてはいけない。
notepad .env

# 全7サービスをbuildして起動する。
docker compose up -d --build

# healthと稼働状態を確認する。
Invoke-RestMethod http://127.0.0.1:18080/api/ready
docker compose ps
```

ブラウザで `http://127.0.0.1:18080` を開きます。ChatGPT手動予想にAPIキーは不要です。対象レースのPython予想後にプロンプトを作成・コピーし、ChatGPTへ手動で貼り付けます。Web検索が有効であることを確認して送信し、回答は画面へ手動で貼り付けて保存します。

```powershell
# 設定変更をAPIとWorkerへ反映する。
docker compose up -d --force-recreate api worker beat
```

## 停止と再開

```powershell
# データvolumeを残して停止する。
docker compose stop

# 既存containerを再開する。
docker compose start

# containerを削除して停止する。DB volumeは残る。
docker compose down
```

`docker compose down -v`はDBとRedis volumeを削除するため、バックアップとユーザーの明示判断なしに実行しないでください。

## テスト

```powershell
# Backend全回帰を実行する。
.\.venv\Scripts\python.exe -m pytest backend\tests -q

# UI契約とproduction buildを実行する。
npm --prefix frontend run test:ui
npm --prefix frontend run build

# 正式container内でBackend全回帰を実行する。
$api = docker compose ps -q api
docker exec $api python -m pytest tests -q
```

## ドキュメント

- [運用手順](docs/operations/運用手順.md)
- [障害対応・FAQ](docs/operations/障害対応・FAQ.md)
- [統合仕様書](docs/design/00_Keiba_AI_Studio_統合仕様書.md)
- [要件トレーサビリティ](docs/design/01_要件トレーサビリティ管理表.md)
- [バックアップ対象・復元契約](docs/design/03_バックアップ対象・復元契約.md)
- [既知の制限事項](docs/既知の制限事項.md)
- [変更履歴](docs/CHANGELOG.md)

API仕様は起動後に `http://127.0.0.1:18080/api/docs`、OpenAPI JSONは `http://127.0.0.1:18080/api/openapi.json` で確認できます。
