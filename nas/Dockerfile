FROM python:3.11-slim-bookworm
ARG SOURCE_COMMIT
RUN test -n "$SOURCE_COMMIT"
ENV KEIBA_CANDIDATE_COMMIT=$SOURCE_COMMIT
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TZ=Asia/Tokyo
WORKDIR /opt/keiba
COPY nas/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY 1_keibayosou_*.py model_registry.py shadow_validation.py /opt/keiba/
COPY config/production_model.json config/shadow_model.json config/forward_validation.json /opt/keiba/config/
COPY yosou_py/best_feature_weights_*.py /opt/keiba/yosou_py/
COPY continuous_improvement /opt/keiba/continuous_improvement/
COPY nas /opt/keiba/nas/
USER 10001:10001
ENTRYPOINT ["python", "-m", "nas.candidate"]
