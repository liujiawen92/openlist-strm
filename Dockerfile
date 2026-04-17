# openlist-strm Dockerfile
# Multi-stage build for minimal image size

# Stage 1: Builder
FROM python:3.9-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libffi-dev && rm -rf /var/lib/apt/lists/*
RUN python -m venv /opt/venv
COPY requirements.txt .
RUN /opt/venv/bin/pip install --no-cache-dir -r requirements.txt
RUN /opt/venv/bin/pip install --no-cache-dir gunicorn

# Stage 2: Production
FROM python:3.9-slim AS production
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /opt/venv /opt/venv
COPY *.py ./
COPY templates/ ./templates/
COPY static/ ./static/
ENV PATH="/opt/venv/bin:${PATH}"
ENV PYTHONPATH="/app"
RUN useradd -m -u 1000 appuser && \
    mkdir -p /app/logs /app/config && \
    chown -R appuser:appuser /app
USER appuser
EXPOSE 5246
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:${WEB_PORT:-5246}/ || exit 1
# Use gunicorn in production; set FLASK_DEBUG=true for debug mode
CMD ["sh", "-c", \
    "if [ \"${FLASK_DEBUG:-false}\" = \"true\" ]; then python app.py; else gunicorn --bind 0.0.0.0:${WEB_PORT:-5246} --workers 2 --timeout 60 app:app; fi"]
