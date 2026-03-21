FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    netcat-openbsd \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/* \
    && node --version \
    && npm --version \
    && npm install -g pmxtjs \
    && rm -rf /root/.npm

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

CMD ["python", "--version"]
