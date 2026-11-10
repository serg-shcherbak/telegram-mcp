ARG PYTHON_IMAGE=python:3.13.12-slim-bookworm
FROM ${PYTHON_IMAGE}
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid 1000 --no-create-home appuser
ARG VCS_REF
RUN test -n "$VCS_REF"
LABEL org.opencontainers.image.revision=$VCS_REF
COPY telegram_mcp/audio_converter.py /opt/audio_converter.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 1000:1000
WORKDIR /audio
CMD ["python", "/opt/audio_converter.py"]
