FROM python:3.15-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements/producer.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/ ./producer/

# Never run as root.
RUN useradd --create-home --uid 10001 appuser && chown -R appuser /app
USER appuser

CMD ["python", "-m", "producer.main"]
