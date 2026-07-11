FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /workspace/backend

RUN groupadd --gid 10001 keiba \
    && useradd --uid 10001 --gid keiba --create-home --shell /usr/sbin/nologin keiba \
    && mkdir -p /workspace/data/exports /workspace/data/staging /workspace/data/logs /workspace/beat \
    && chown -R keiba:keiba /workspace

COPY backend/requirements.txt /tmp/requirements.txt
# 予想はCPU実行を前提とし、CUDA依存を含まない固定wheelを先に導入する。
RUN pip install --no-cache-dir \
        --index-url https://download.pytorch.org/whl/cpu \
        torch==2.11.0+cpu \
    && pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=keiba:keiba backend /workspace/backend
# リリース安全テストがコンテナ内でも公開ポート・秘密管理・UI契約を検査できるよう、
# 実行時秘密を含まない契約ファイルだけを読み取り用に同梱する。
COPY --chown=keiba:keiba docker-compose.yml .env.example .gitignore /workspace/release-contract/
COPY --chown=keiba:keiba frontend/app /workspace/release-contract/frontend/app
COPY --chown=keiba:keiba README.md /workspace/release-contract/README.md
COPY --chown=keiba:keiba docs/operations /workspace/release-contract/docs/operations

USER keiba

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
