# syntax=docker/dockerfile:1.7
FROM python:3.12.8-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/worker/gen:/app
WORKDIR /app
RUN useradd --create-home --uid 10001 apollo
COPY worker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY worker/ /app/worker/
USER apollo
EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=2s --retries=5 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/metrics', timeout=1)"]
ENTRYPOINT ["python", "-m", "worker"]
