FROM mcr.microsoft.com/playwright/python:v1.60.0-jammy

WORKDIR /workspace

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

ENV TZ=Asia/Tokyo
ENV PYTHONUNBUFFERED=1

CMD ["python3", "--version"]