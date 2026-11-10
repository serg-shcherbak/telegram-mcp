ARG PYTHON_IMAGE=python:3.13.12-slim-bookworm
FROM ${PYTHON_IMAGE} AS builder
WORKDIR /build
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
RUN pip install --no-cache-dir uv==0.11.3
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY main.py migrate_session.py sanitize.py session_string_generator.py ./
COPY telegram_mcp ./telegram_mcp
RUN uv sync --frozen --extra events --no-dev --no-editable

FROM ${PYTHON_IMAGE} AS runtime
ARG VCS_REF
RUN test -n "$VCS_REF" && \
    groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid 1000 --no-create-home appuser && \
    mkdir -p /app/state/events /app/session && \
    chown -R 1000:1000 /app
LABEL org.opencontainers.image.revision=$VCS_REF
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/opt/venv/bin:$PATH \
    TELEGRAM_LEGACY_EVENTS=0 \
    TELEGRAM_WARM_ENTITY_CACHE=0 \
    DOT_EVENTS_ENABLED=0
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY deploy/dot/dot_server.py /app/dot_server.py
USER 1000:1000
CMD ["python", "/app/dot_server.py"]
