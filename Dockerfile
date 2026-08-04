# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# 1. Use JRE instead of JDK, add --no-install-recommends, and cache apt downloads
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# 2. Cache pip downloads to drastically speed up rebuilds when requirements change
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

# 3. Copy the rest of the application code
COPY . .

ENTRYPOINT ["python", "src/main.py"]