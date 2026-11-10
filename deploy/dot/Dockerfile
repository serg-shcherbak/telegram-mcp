ARG PYTHON_IMAGE=python:3.13.12-slim-bookworm
FROM ${PYTHON_IMAGE} AS builder
WORKDIR /build
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
RUN pip install --no-cache-dir uv==0.11.3
COPY pyproject.toml uv.lock README.md LICENSE MANIFEST.in ./
COPY main.py migrate_session.py sanitize.py session_string_generator.py ./
COPY telegram_mcp ./telegram_mcp
RUN uv sync --frozen --no-dev --no-editable

FROM ${PYTHON_IMAGE} AS runtime
ARG VCS_REF
RUN test -n "$VCS_REF" && \
    groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid 1000 --no-create-home appuser && \
    mkdir -p /app/state/media /app/session /app/config /opt/telegram-dot && \
    chmod 0700 /app/state /app/state/media /app/session /app/config && \
    chown -R 1000:1000 /app
LABEL org.opencontainers.image.revision=$VCS_REF
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/opt/venv/bin:$PATH \
    HOME=/app/state \
    XDG_STATE_HOME=/app/state \
    TELEGRAM_LEGACY_EVENTS=0 \
    TELEGRAM_EVENT_FEED=0 \
    TELEGRAM_WARM_ENTITY_CACHE=0
WORKDIR /app/state
COPY --from=builder /opt/venv /opt/venv
# The installation guard validates the installer's recorded local build source.
COPY --from=builder /build/pyproject.toml /build/pyproject.toml
COPY deploy/dot/dot_server.py /opt/telegram-dot/dot_server.py
USER 1000:1000
CMD ["python", "/opt/telegram-dot/dot_server.py"]
