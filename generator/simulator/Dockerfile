FROM python:3.13.12-slim-bookworm@sha256:a58daefb915e1e03ad48f3ca4df8832065412c5c35cacb9d39f4229184de12b6

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.lock /tmp/requirements.lock
RUN python -m pip install --requirement /tmp/requirements.lock \
    && useradd --create-home --uid 10001 simulator

WORKDIR /app
COPY config.json /app/config.json
COPY simulator /app/simulator

USER simulator
ENTRYPOINT ["python", "-m", "simulator"]
